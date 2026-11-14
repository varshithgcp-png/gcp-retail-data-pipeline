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
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions

from dataflow.schemas import ENTITY_CONFIG, QUARANTINE_SCHEMA
from dataflow.validators import parse_csv_line, validate_and_cast


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

    known_args, beam_args = parser.parse_known_args(argv)

    options = PipelineOptions(beam_args)
    options.view_as(SetupOptions).save_main_session = True

    config = ENTITY_CONFIG[known_args.entity_type]

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


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
