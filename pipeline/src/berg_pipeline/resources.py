import hashlib
import hmac
import os
import sys
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

import dagster as dg
from dagster_duckdb import DuckDBResource

from berg_pipeline import paths

R2_OUTER_ATTEMPTS = 6
R2_RETRY_MAX_DELAY_S = 30


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
        from botocore.config import Config

        return boto3.client(
            "s3",
            endpoint_url=self.endpoint_url,
            aws_access_key_id=self.access_key_id,
            aws_secret_access_key=self.secret_access_key,
            config=Config(
                connect_timeout=30,
                read_timeout=120,
                tcp_keepalive=True,
                retries={"max_attempts": 10, "mode": "adaptive"},
            ),
        )

    @staticmethod
    def _retryable(error: Exception) -> bool:
        from botocore.exceptions import (
            ClientError,
            ConnectionClosedError,
            ConnectTimeoutError,
            EndpointConnectionError,
            ReadTimeoutError,
        )

        if isinstance(
            error,
            (ConnectionClosedError, ConnectTimeoutError, EndpointConnectionError, ReadTimeoutError),
        ):
            return True
        if isinstance(error, ClientError):
            response = error.response
            status = response.get("ResponseMetadata", {}).get("HTTPStatusCode", 0)
            code = response.get("Error", {}).get("Code", "")
            return status >= 500 or code in {"RequestTimeout", "SlowDown", "Throttling"}
        return False

    @classmethod
    def _with_transient_retries(cls, operation: Callable[[], Any], description: str) -> Any:
        """Retry beyond botocore's request retries so one dead connection cannot kill a sync."""
        for attempt in range(1, R2_OUTER_ATTEMPTS + 1):
            try:
                return operation()
            except Exception as error:
                if attempt == R2_OUTER_ATTEMPTS or not cls._retryable(error):
                    raise
                delay = min(2 ** (attempt - 1), R2_RETRY_MAX_DELAY_S)
                print(
                    f"R2 transient failure during {description}; retrying in {delay}s "
                    f"({attempt}/{R2_OUTER_ATTEMPTS}): {error}",
                    file=sys.stderr,
                    flush=True,
                )
                time.sleep(delay)
        raise AssertionError("retry loop exhausted without returning or raising")

    def upload(
        self,
        local_path: Path,
        key: str,
        client=None,
        *,
        content_type: str | None = None,
        cache_control: str | None = None,
    ) -> None:
        checksum = self._digest(local_path, "sha256")
        client = client or self.client()
        extra_args = {"Metadata": {"berg-sha256": checksum}}
        if content_type:
            extra_args["ContentType"] = content_type
        if cache_control:
            extra_args["CacheControl"] = cache_control
        self._with_transient_retries(
            lambda: client.upload_file(
                str(local_path),
                self.bucket,
                key,
                ExtraArgs=extra_args,
            ),
            f"upload {key}",
        )

    def delete(self, key: str, client=None) -> None:
        """Delete an authoritative output that rebuilt to zero rows."""
        client = client or self.client()
        self._with_transient_retries(
            lambda: client.delete_object(Bucket=self.bucket, Key=key),
            f"delete {key}",
        )

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

    def existing_objects(self, client=None) -> dict[str, dict]:
        """key → size/ETag for everything in the bucket."""
        client = client or self.client()
        objects: dict[str, dict] = {}
        for page in client.get_paginator("list_objects_v2").paginate(Bucket=self.bucket):
            for obj in page.get("Contents", []):
                objects[obj["Key"]] = {"size": obj["Size"], "etag": obj.get("ETag", "")}
        return objects

    @staticmethod
    def _digest(path: Path, algorithm: str) -> str:
        with path.open("rb") as fh:
            return hashlib.file_digest(fh, algorithm).hexdigest()

    def object_matches(self, local_path: Path, key: str, remote: dict, client=None) -> bool:
        """True only when the remote object is proven to contain the local bytes.

        S3-compatible single-part ETags are MD5 digests. Multipart ETags are not, so uploads
        carry an explicit SHA-256 metadata value and resumed syncs verify that instead.
        """
        if remote.get("size") != local_path.stat().st_size:
            return False

        etag = str(remote.get("etag", "")).strip('"')
        if len(etag) == 32 and "-" not in etag:
            return hmac.compare_digest(etag.lower(), self._digest(local_path, "md5"))

        client = client or self.client()
        head = client.head_object(Bucket=self.bucket, Key=key)
        remote_sha256 = head.get("Metadata", {}).get("berg-sha256", "")
        return bool(remote_sha256) and hmac.compare_digest(
            remote_sha256.lower(), self._digest(local_path, "sha256")
        )


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
