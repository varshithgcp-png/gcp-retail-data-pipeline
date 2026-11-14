from pathlib import Path
from google.cloud import storage

def read_source_text(source: str, project_id: str) -> str:
    if source.startswith("gs://"):
        without_scheme = source[5:]
        bucket_name, blob_name = without_scheme.split("/", 1)
        client = storage.Client(project=project_id)
        return client.bucket(bucket_name).blob(blob_name).download_as_text()
    return Path(source).read_text(encoding="utf-8")
