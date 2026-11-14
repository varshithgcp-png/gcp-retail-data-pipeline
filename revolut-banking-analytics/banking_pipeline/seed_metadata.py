import argparse
from datetime import datetime, timezone
from google.cloud import bigquery
from banking_pipeline.config import load_entities, load_rules

def main():
    parser=argparse.ArgumentParser(); parser.add_argument("--project-id",required=True); parser.add_argument("--dataset",default="revolut_bronze"); parser.add_argument("--entities-config",default="config/entities.yml"); parser.add_argument("--rules-config",default="config/validation_rules.yml"); args=parser.parse_args()
    client=bigquery.Client(project=args.project_id); now=datetime.now(timezone.utc).isoformat(); entities=load_entities(args.entities_config)
    entity_rows=[{"entity_name":name,"target_table":cfg["target_table"],"primary_key":cfg["primary_key"],"expected_columns":cfg["columns"],"active_flag":True,"updated_at":now} for name,cfg in entities.items()]
    rules=load_rules(args.rules_config); rule_rows=[dict(rule,active_flag=True,updated_at=now) for rule in rules]
    for table_name,rows in [("ENTITY_CONFIG_bronze",entity_rows),("VALIDATION_RULES_bronze",rule_rows)]:
        table_id=f"{args.project_id}.{args.dataset}.{table_name}"; client.query(f"TRUNCATE TABLE `{table_id}`").result(); errors=client.insert_rows_json(table_id,rows)
        if errors: raise RuntimeError(f"Metadata load failed for {table_name}: {errors}")
        print(f"Loaded {len(rows)} rows into {table_id}")

if __name__ == "__main__": main()
