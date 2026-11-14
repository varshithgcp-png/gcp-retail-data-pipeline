"""Metadata-driven Fit Analytics batch ingestion pipeline.

Flow:
GCS CSV -> parse -> validate -> BigQuery stage
                         -> BigQuery quarantine
"""

import argparse
import logging
from datetime import datetime, timezone

import apache_beam as beam
from apache_beam.io.gcp.bigquery import BigQueryDisposition
from apache_beam.metrics import Metrics
from apache_beam.transforms.combiners import Count
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions

try:
    from dataflow.schemas import AUDIT_SCHEMA, ENTITY_CONFIG, QUARANTINE_SCHEMA
    from dataflow.validators import parse_csv_line, validate_and_cast
except ModuleNotFoundError:
    # Allows direct execution of this file from Composer/Dataflow launcher.
    from schemas import AUDIT_SCHEMA, ENTITY_CONFIG, QUARANTINE_SCHEMA
    from validators import parse_csv_line, validate_and_cast


class ParseValidateRouteFn(beam.DoFn):
    INVALID = "invalid"

    def __init__(self, entity_type: str, source_file: str, batch_id: str):
        self.entity_type = entity_type
        self.source_file = source_file
        self.batch_id = batch_id
        self.valid_records = Metrics.counter(self.__class__, "valid_records")
        self.invalid_records = Metrics.counter(self.__class__, "invalid_records")

    def _invalid_row(self, line: str, message: str, ingestion_ts: str):
        return {
            "entity_type": self.entity_type,
            "raw_record": line,
            "error_message": message,
            "source_file": self.source_file,
            "batch_id": self.batch_id,
            "ingestion_ts": ingestion_ts,
        }

    def process(self, line: str):
        config = ENTITY_CONFIG[self.entity_type]
        ingestion_ts = datetime.now(timezone.utc).isoformat()

        try:
            row = parse_csv_line(line, config["columns"])
            is_valid, typed_row, error = validate_and_cast(row, config)

            if not is_valid:
                self.invalid_records.inc()
                yield beam.pvalue.TaggedOutput(
                    self.INVALID,
                    self._invalid_row(line, error, ingestion_ts),
                )
                return

            typed_row["ingestion_ts"] = ingestion_ts
            typed_row["batch_id"] = self.batch_id
            typed_row["source_file"] = self.source_file
            self.valid_records.inc()
            yield typed_row

        except Exception as exc:
            self.invalid_records.inc()
            yield beam.pvalue.TaggedOutput(
                self.INVALID,
                self._invalid_row(line, str(exc), ingestion_ts),
            )


def create_audit_record(
    _,
    valid_count,
    invalid_count,
    entity_type,
    batch_id,
    start_time,
):
    end_time = datetime.now(timezone.utc).isoformat()
    return {
        "pipeline_name": f"fitanalytics_{entity_type}_ingestion",
        "batch_id": batch_id,
        "source_count": valid_count + invalid_count,
        "stage_count": valid_count,
        "target_count": 0,
        "rejected_count": invalid_count,
        "start_time": start_time,
        "end_time": end_time,
        "status": "PROCESSED",
        "error_message": None,
    }


def run(argv=None):
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--entity_type",
        required=True,
        choices=sorted(ENTITY_CONFIG),
    )
    parser.add_argument(
        "--input",
        required=True,
        help="GCS file path or pattern.",
    )
    parser.add_argument(
        "--output_table",
        required=True,
        help="PROJECT:DATASET.TABLE",
    )
    parser.add_argument(
        "--quarantine_table",
        required=True,
        help="PROJECT:DATASET.TABLE",
    )
    parser.add_argument(
        "--batch_id",
        required=True,
        help="Replay-safe batch identifier supplied by orchestration.",
    )
    parser.add_argument(
        "--audit_table",
        required=True,
        help="PROJECT:DATASET.TABLE for pipeline-level audit records.",
    )

    known_args, beam_args = parser.parse_known_args(argv)

    options = PipelineOptions(beam_args)
    options.view_as(SetupOptions).save_main_session = True

    config = ENTITY_CONFIG[known_args.entity_type]
    pipeline_start_time = datetime.now(timezone.utc).isoformat()

    with beam.Pipeline(options=options) as pipeline:
        outputs = (
            pipeline
            | "Read CSV" >> beam.io.ReadFromText(
                known_args.input,
                skip_header_lines=1,
            )
            | "Parse Validate Route" >> beam.ParDo(
                ParseValidateRouteFn(
                    known_args.entity_type,
                    known_args.input,
                    known_args.batch_id,
                )
            ).with_outputs(ParseValidateRouteFn.INVALID, main="valid")
        )

        _ = (
            outputs.valid
            | "Write Valid Stage Rows" >> beam.io.WriteToBigQuery(
                known_args.output_table,
                schema=config["bq_schema"],
                create_disposition=BigQueryDisposition.CREATE_NEVER,
                write_disposition=BigQueryDisposition.WRITE_APPEND,
            )
        )

        _ = (
            outputs.invalid
            | "Write Quarantine Rows" >> beam.io.WriteToBigQuery(
                known_args.quarantine_table,
                schema=QUARANTINE_SCHEMA,
                create_disposition=BigQueryDisposition.CREATE_NEVER,
                write_disposition=BigQueryDisposition.WRITE_APPEND,
            )
        )

        valid_count = outputs.valid | "Count Valid Rows" >> Count.Globally()
        invalid_count = outputs.invalid | "Count Invalid Rows" >> Count.Globally()

        audit_record = (
            pipeline
            | "Create Audit Seed" >> beam.Create([None])
            | "Build Audit Record" >> beam.Map(
                create_audit_record,
                valid_count=beam.pvalue.AsSingleton(valid_count),
                invalid_count=beam.pvalue.AsSingleton(invalid_count),
                entity_type=known_args.entity_type,
                batch_id=known_args.batch_id,
                start_time=pipeline_start_time,
            )
        )

        _ = (
            audit_record
            | "Write Pipeline Audit" >> beam.io.WriteToBigQuery(
                known_args.audit_table,
                schema=AUDIT_SCHEMA,
                create_disposition=BigQueryDisposition.CREATE_NEVER,
                write_disposition=BigQueryDisposition.WRITE_APPEND,
            )
        )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
