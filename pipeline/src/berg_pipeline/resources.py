import os
from pathlib import Path

import dagster as dg
from dagster_duckdb import DuckDBResource

from berg_pipeline import paths


class R2Resource(dg.ConfigurableResource):
    """R2 over its S3-compatible API. Credentials come from the environment, never from code."""

    account_id: str
    bucket: str
    access_key_id: str
    secret_access_key: str

    @property
    def endpoint_url(self) -> str:
        return f"https://{self.account_id}.r2.cloudflarestorage.com"

    def client(self):
        """A fresh client. Reuse one across a bulk sync; per-file clients are pure overhead."""
        import boto3

        return boto3.client(
            "s3",
            endpoint_url=self.endpoint_url,
            aws_access_key_id=self.access_key_id,
            aws_secret_access_key=self.secret_access_key,
        )

    def upload(self, local_path: Path, key: str, client=None) -> None:
        (client or self.client()).upload_file(str(local_path), self.bucket, key)

    def get_json(self, key: str, client=None) -> dict | None:
        """Parsed JSON at key, or None if the object does not exist."""
        import json

        client = client or self.client()
        try:
            body = client.get_object(Bucket=self.bucket, Key=key)["Body"].read()
        except Exception as e:  # noqa: BLE001 — only "absent" is ours; anything else is real
            code = getattr(e, "response", {}).get("Error", {}).get("Code")
            if code in ("NoSuchKey", "404", "NoSuchBucket"):
                return None
            raise
        return json.loads(body)

    def existing_sizes(self, client=None) -> dict[str, int]:
        """key → size for everything in the bucket, so a resumed sync can skip what's done."""
        client = client or self.client()
        sizes: dict[str, int] = {}
        for page in client.get_paginator("list_objects_v2").paginate(Bucket=self.bucket):
            for obj in page.get("Contents", []):
                sizes[obj["Key"]] = obj["Size"]
        return sizes


def default_duckdb() -> DuckDBResource:
    """A month of raw CSV does not fit in RAM — memory_limit + a spill directory are mandatory."""
    paths.DUCKDB_TMP.mkdir(parents=True, exist_ok=True)
    return DuckDBResource(
        database=os.getenv("BERG_DUCKDB_PATH", str(paths.DUCKDB_PATH)),
        connection_config={
            "memory_limit": os.getenv("BERG_DUCKDB_MEMORY_LIMIT", "8GB"),
            "temp_directory": os.getenv("BERG_DUCKDB_TEMP_DIR", str(paths.DUCKDB_TMP)),
        },
    )


def default_r2() -> R2Resource:
    return R2Resource(
        account_id=dg.EnvVar("R2_ACCOUNT_ID"),
        bucket=dg.EnvVar("R2_BUCKET"),
        access_key_id=dg.EnvVar("R2_ACCESS_KEY_ID"),
        secret_access_key=dg.EnvVar("R2_SECRET_ACCESS_KEY"),
    )
