import argparse
from datetime import datetime, timezone
from google.cloud import bigquery
from banking_pipeline.config import load_entities, load_rules

def replace_rows(client, table_id, rows):
    client.query(f"TRUNCATE TABLE `{table_id}`").result()
    errors = client.insert_rows_json(table_id, rows)
    if errors:
        raise RuntimeError(f"Metadata load failed for {table_id}: {errors}")
    print(f"Loaded {len(rows)} rows into {table_id}")

def main():
    parser=argparse.ArgumentParser()
    parser.add_argument("--project-id",required=True)
    parser.add_argument("--dataset",default="revolut_bronze")
    parser.add_argument("--entities-config",default="config/entities.yml")
    parser.add_argument("--rules-config",default="config/validation_rules.yml")
    args=parser.parse_args()

    client=bigquery.Client(project=args.project_id)
    now=datetime.now(timezone.utc).isoformat()
    entities=load_entities(args.entities_config)

    entity_rows=[]
    column_rows=[]
    for entity_name,cfg in entities.items():
        entity_rows.append({
            "entity_name":entity_name,
            "target_table":cfg["target_table"],
            "primary_key":cfg["primary_key"],
            "expected_columns":cfg["columns"],
            "active_flag":True,
            "updated_at":now,
        })
        required=set(cfg.get("required_fields",[]))
        types=cfg.get("data_types",{})
        for ordinal,column in enumerate(cfg["columns"],start=1):
            column_rows.append({
                "entity_name":entity_name,
                "column_name":column,
                "ordinal_position":ordinal,
                "silver_data_type":types.get(column,"STRING"),
                "required_flag":column in required,
                "updated_at":now,
            })

    rules=load_rules(args.rules_config)
    rule_rows=[dict(rule,active_flag=True,updated_at=now) for rule in rules]

    replace_rows(client,f"{args.project_id}.{args.dataset}.ENTITY_CONFIG_bronze",entity_rows)
    replace_rows(client,f"{args.project_id}.{args.dataset}.COLUMN_CONFIG_bronze",column_rows)
    replace_rows(client,f"{args.project_id}.{args.dataset}.VALIDATION_RULES_bronze",rule_rows)

if __name__ == "__main__":
    main()
