import argparse
import hashlib
from datetime import datetime, timezone
from google.cloud import bigquery

from banking_pipeline.config import load_entities
from banking_pipeline.gcp_io import read_source_text
from banking_pipeline.parser import parse_interleaved_text

META_SCHEMA = [
    bigquery.SchemaField("batch_id", "STRING"),
    bigquery.SchemaField("business_date", "DATE"),
    bigquery.SchemaField("load_type", "STRING"),
    bigquery.SchemaField("source_file", "STRING"),
    bigquery.SchemaField("source_path", "STRING"),
    bigquery.SchemaField("source_row_number", "INTEGER"),
    bigquery.SchemaField("file_hash", "STRING"),
    bigquery.SchemaField("ingestion_ts", "TIMESTAMP"),
]

def stable_batch_id(business_date: str, file_hash: str) -> str:
    return f"{business_date.replace('-', '')}_{file_hash[:12]}"

def query_scalar(client, sql, params):
    job_config = bigquery.QueryJobConfig(query_parameters=params)
    rows = list(client.query(sql, job_config=job_config).result())
    return rows[0][0] if rows else None

def insert_control_row(client, table_id, row):
    errors=client.insert_rows_json(table_id,[row])
    if errors:
        raise RuntimeError(f"Control-table insert failed: {errors}")

def main():
    parser=argparse.ArgumentParser()
    parser.add_argument("--project-id",required=True)
    parser.add_argument("--dataset",default="revolut_bronze")
    parser.add_argument("--source",required=True,help="Local path or gs:// URI")
    parser.add_argument("--business-date",required=True)
    parser.add_argument("--load-type",choices=["FULL","INCREMENTAL"],required=True)
    parser.add_argument("--entities-config",default="config/entities.yml")
    args=parser.parse_args()

    entities=load_entities(args.entities_config)
    content=read_source_text(args.source,args.project_id)
    file_hash=hashlib.sha256(content.encode("utf-8")).hexdigest()
    batch_id=stable_batch_id(args.business_date,file_hash)
    source_file=args.source.rsplit("/",1)[-1]
    start_time=datetime.now(timezone.utc).isoformat()
    client=bigquery.Client(project=args.project_id)
    control_id=f"{args.project_id}.{args.dataset}.CONTROL_TABLE_bronze"
    control=f"`{control_id}`"

    already_loaded=query_scalar(
        client,
        f"""SELECT COUNT(*) FROM {control}
            WHERE file_hash=@file_hash
              AND entity_name='__FILE__'
              AND processing_status='BRONZE_SUCCESS'""",
        [bigquery.ScalarQueryParameter("file_hash","STRING",file_hash)],
    )
    if already_loaded:
        print(f"Replay-safe skip: {source_file} already loaded as {batch_id}")
        return

    total_rows=0
    try:
        blocks=parse_interleaved_text(content)
        unknown=sorted(set(blocks)-set(entities))
        if unknown:
            raise ValueError(f"Unknown entity blocks: {unknown}. Add them to config/entities.yml")

        batch_param=bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("batch_id","STRING",batch_id)]
        )
        for cfg in entities.values():
            table_id=f"`{args.project_id}.{args.dataset}.{cfg['target_table']}`"
            client.query(f"DELETE FROM {table_id} WHERE batch_id=@batch_id",job_config=batch_param).result()
        client.query(f"DELETE FROM {control} WHERE batch_id=@batch_id",job_config=batch_param).result()

        for entity_name,block in blocks.items():
            cfg=entities[entity_name]
            if block.header != cfg["columns"]:
                raise ValueError(
                    f"{entity_name} header mismatch. Expected {cfg['columns']}, got {block.header}"
                )

            table_id=f"{args.project_id}.{args.dataset}.{cfg['target_table']}"
            rows=[]
            for row_number,values in enumerate(block.rows,start=1):
                record=dict(zip(block.header,values))
                record.update({
                    "batch_id":batch_id,
                    "business_date":args.business_date,
                    "load_type":args.load_type,
                    "source_file":source_file,
                    "source_path":args.source,
                    "source_row_number":row_number,
                    "file_hash":file_hash,
                    "ingestion_ts":start_time,
                })
                rows.append(record)

            job_config=bigquery.LoadJobConfig(
                schema=[bigquery.SchemaField(c,"STRING") for c in cfg["columns"]]+META_SCHEMA,
                write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            )
            client.load_table_from_json(rows,table_id,job_config=job_config).result()
            total_rows += len(rows)

            insert_control_row(client,control_id,{
                "batch_id":batch_id,"business_date":args.business_date,
                "load_type":args.load_type,"source_file":source_file,
                "source_path":args.source,"file_hash":file_hash,
                "entity_name":entity_name,"target_table":cfg["target_table"],
                "source_row_count":len(rows),"bronze_row_count":len(rows),
                "rejected_row_count":0,"processing_status":"SUCCESS",
                "pipeline_status":"BRONZE_SUCCESS","start_time":start_time,
                "end_time":datetime.now(timezone.utc).isoformat(),
                "error_message":None,"created_at":start_time,
            })
            print(f"Loaded {len(rows)} {entity_name} rows -> {table_id}")

        insert_control_row(client,control_id,{
            "batch_id":batch_id,"business_date":args.business_date,
            "load_type":args.load_type,"source_file":source_file,
            "source_path":args.source,"file_hash":file_hash,
            "entity_name":"__FILE__","target_table":None,
            "source_row_count":total_rows,"bronze_row_count":total_rows,
            "rejected_row_count":0,"processing_status":"BRONZE_SUCCESS",
            "pipeline_status":"PENDING_DBT","start_time":start_time,
            "end_time":datetime.now(timezone.utc).isoformat(),
            "error_message":None,"created_at":start_time,
        })
        print(f"Bronze complete. batch_id={batch_id}, total_rows={total_rows}")

    except Exception as exc:
        try:
            insert_control_row(client,control_id,{
                "batch_id":batch_id,"business_date":args.business_date,
                "load_type":args.load_type,"source_file":source_file,
                "source_path":args.source,"file_hash":file_hash,
                "entity_name":"__FILE__","target_table":None,
                "source_row_count":total_rows,"bronze_row_count":total_rows,
                "rejected_row_count":0,"processing_status":"FAILED",
                "pipeline_status":"FAILED","start_time":start_time,
                "end_time":datetime.now(timezone.utc).isoformat(),
                "error_message":str(exc)[:1024],"created_at":start_time,
            })
        except Exception as control_exc:
            print(f"Unable to write failure control row: {control_exc}")
        raise

if __name__ == "__main__":
    main()
