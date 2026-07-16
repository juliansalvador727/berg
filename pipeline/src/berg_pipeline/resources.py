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

    def upload(self, local_path: Path, key: str) -> None:
        import boto3

        client = boto3.client(
            "s3",
            endpoint_url=self.endpoint_url,
            aws_access_key_id=self.access_key_id,
            aws_secret_access_key=self.secret_access_key,
        )
        client.upload_file(str(local_path), self.bucket, key)


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
