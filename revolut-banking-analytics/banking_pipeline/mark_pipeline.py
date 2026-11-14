import argparse
import hashlib
from google.cloud import bigquery
from banking_pipeline.gcp_io import read_source_text
from banking_pipeline.bronze_loader import stable_batch_id

def main():
    p=argparse.ArgumentParser()
    p.add_argument('--project-id', required=True)
    p.add_argument('--dataset', default='revolut_bronze')
    p.add_argument('--source', required=True)
    p.add_argument('--business-date', required=True)
    p.add_argument('--status', default='SUCCESS')
    args=p.parse_args()
    content=read_source_text(args.source,args.project_id)
    file_hash=hashlib.sha256(content.encode('utf-8')).hexdigest()
    batch_id=stable_batch_id(args.business_date,file_hash)
    client=bigquery.Client(project=args.project_id)
    sql=f"""
    UPDATE `{args.project_id}.{args.dataset}.CONTROL_TABLE_bronze`
    SET pipeline_status=@status, end_time=CURRENT_TIMESTAMP()
    WHERE batch_id=@batch_id AND entity_name='__FILE__'
    """
    cfg=bigquery.QueryJobConfig(query_parameters=[bigquery.ScalarQueryParameter('status','STRING',args.status),bigquery.ScalarQueryParameter('batch_id','STRING',batch_id)])
    client.query(sql,job_config=cfg).result()
    print(f'Updated {batch_id} pipeline_status={args.status}')

if __name__=='__main__':
    main()
