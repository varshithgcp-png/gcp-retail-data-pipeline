"""Metadata for Fit Analytics batch ingestion."""

ENTITY_CONFIG = {
    "customer": {
        "columns": [
            "customer_id", "region", "height_cm", "weight_kg",
            "preferred_fit", "updated_at"
        ],
        "required": ["customer_id", "region", "preferred_fit", "updated_at"],
        "string_fields": ["customer_id", "region", "preferred_fit"],
        "integer_fields": ["height_cm"],
        "numeric_fields": ["weight_kg"],
        "date_fields": [],
        "timestamp_fields": ["updated_at"],
        "allowed_values": {
            "preferred_fit": {"SLIM", "REGULAR", "RELAXED"}
        },
        "bq_schema": (
            "customer_id:STRING,region:STRING,height_cm:INTEGER,weight_kg:NUMERIC,"
            "preferred_fit:STRING,updated_at:TIMESTAMP,ingestion_ts:TIMESTAMP,"
            "batch_id:STRING,source_file:STRING"
        ),
    },
    "product": {
        "columns": [
            "product_id", "sku", "brand", "category", "size_system",
            "size_label", "fit_profile", "unit_price", "updated_at"
        ],
        "required": [
            "product_id", "sku", "brand", "category", "size_system",
            "size_label", "fit_profile", "unit_price", "updated_at"
        ],
        "string_fields": [
            "product_id", "sku", "brand", "category", "size_system",
            "size_label", "fit_profile"
        ],
        "integer_fields": [],
        "numeric_fields": ["unit_price"],
        "date_fields": [],
        "timestamp_fields": ["updated_at"],
        "allowed_values": {
            "fit_profile": {"TRUE_TO_SIZE", "RUNS_SMALL", "RUNS_LARGE"}
        },
        "bq_schema": (
            "product_id:STRING,sku:STRING,brand:STRING,category:STRING,"
            "size_system:STRING,size_label:STRING,fit_profile:STRING,"
            "unit_price:NUMERIC,updated_at:TIMESTAMP,ingestion_ts:TIMESTAMP,"
            "batch_id:STRING,source_file:STRING"
        ),
    },
    "purchase": {
        "columns": [
            "purchase_id", "customer_id", "product_id", "recommended_size",
            "purchased_size", "quantity", "amount", "purchase_date", "updated_at"
        ],
        "required": [
            "purchase_id", "customer_id", "product_id", "purchased_size",
            "quantity", "amount", "purchase_date", "updated_at"
        ],
        "string_fields": [
            "purchase_id", "customer_id", "product_id",
            "recommended_size", "purchased_size"
        ],
        "integer_fields": ["quantity"],
        "numeric_fields": ["amount"],
        "date_fields": ["purchase_date"],
        "timestamp_fields": ["updated_at"],
        "allowed_values": {},
        "bq_schema": (
            "purchase_id:STRING,customer_id:STRING,product_id:STRING,"
            "recommended_size:STRING,purchased_size:STRING,quantity:INTEGER,"
            "amount:NUMERIC,purchase_date:DATE,updated_at:TIMESTAMP,"
            "ingestion_ts:TIMESTAMP,batch_id:STRING,source_file:STRING"
        ),
    },
    "return": {
        "columns": [
            "return_id", "purchase_id", "customer_id", "product_id",
            "return_reason", "fit_feedback", "return_amount", "return_date"
        ],
        "required": [
            "return_id", "purchase_id", "customer_id", "product_id",
            "return_reason", "return_amount", "return_date"
        ],
        "string_fields": [
            "return_id", "purchase_id", "customer_id", "product_id",
            "return_reason", "fit_feedback"
        ],
        "integer_fields": [],
        "numeric_fields": ["return_amount"],
        "date_fields": ["return_date"],
        "timestamp_fields": [],
        "allowed_values": {
            "fit_feedback": {
                "TRUE_TO_SIZE", "RUNS_SMALL", "RUNS_LARGE", ""
            }
        },
        "bq_schema": (
            "return_id:STRING,purchase_id:STRING,customer_id:STRING,"
            "product_id:STRING,return_reason:STRING,fit_feedback:STRING,"
            "return_amount:NUMERIC,return_date:DATE,ingestion_ts:TIMESTAMP,"
            "batch_id:STRING,source_file:STRING"
        ),
    },
    "recommendation": {
        "columns": [
            "recommendation_id", "customer_id", "product_id",
            "recommended_size", "confidence_score",
            "recommendation_ts", "event_id"
        ],
        "required": [
            "recommendation_id", "customer_id", "product_id",
            "recommended_size", "confidence_score",
            "recommendation_ts", "event_id"
        ],
        "string_fields": [
            "recommendation_id", "customer_id", "product_id",
            "recommended_size", "event_id"
        ],
        "integer_fields": [],
        "numeric_fields": ["confidence_score"],
        "date_fields": [],
        "timestamp_fields": ["recommendation_ts"],
        "allowed_values": {},
        "bq_schema": (
            "recommendation_id:STRING,customer_id:STRING,product_id:STRING,"
            "recommended_size:STRING,confidence_score:FLOAT,"
            "recommendation_ts:TIMESTAMP,event_id:STRING,"
            "ingestion_ts:TIMESTAMP,batch_id:STRING,source_file:STRING"
        ),
    },
}

QUARANTINE_SCHEMA = (
    "entity_type:STRING,raw_record:STRING,error_message:STRING,"
    "source_file:STRING,batch_id:STRING,ingestion_ts:TIMESTAMP"
)


AUDIT_SCHEMA = (
    "pipeline_name:STRING,batch_id:STRING,source_count:INTEGER,"
    "stage_count:INTEGER,target_count:INTEGER,rejected_count:INTEGER,"
    "start_time:TIMESTAMP,end_time:TIMESTAMP,status:STRING,error_message:STRING"
)
