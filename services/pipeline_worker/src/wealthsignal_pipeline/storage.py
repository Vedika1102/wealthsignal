from __future__ import annotations

import os
from dataclasses import dataclass

try:
    import boto3
except ImportError:  # pragma: no cover - exercised only when boto3 is installed
    boto3 = None


DEFAULT_BUCKET_NAME = "wealthsignal-filings"


@dataclass(slots=True)
class ObjectStorageConfig:
    """Configuration for MinIO or another S3-compatible object store."""

    endpoint_url: str
    access_key_id: str
    secret_access_key: str
    bucket_name: str = DEFAULT_BUCKET_NAME
    secure: bool = False
    region_name: str = "us-east-1"


class ObjectStorage:
    """Thin wrapper around an S3-compatible client for raw filing artifacts."""

    def __init__(self, config: ObjectStorageConfig, client: object) -> None:
        self.config = config
        self._client = client

    def ensure_bucket(self) -> None:
        try:
            self._client.head_bucket(Bucket=self.config.bucket_name)
        except Exception:
            self._client.create_bucket(Bucket=self.config.bucket_name)

    def upload_text(self, key: str, body: str, *, content_type: str = "application/xml") -> None:
        self._client.put_object(
            Bucket=self.config.bucket_name,
            Key=key,
            Body=body.encode("utf-8"),
            ContentType=content_type,
        )

    def upload_raw_filing_artifacts(
        self,
        *,
        cik: str,
        accession_number: str,
        information_table_text: str,
        primary_document_text: str | None = None,
    ) -> dict[str, str]:
        """Upload raw SEC artifacts using the project bucket/key conventions."""

        self.ensure_bucket()
        base_key = f"{cik}/{accession_number}"
        uploaded = {}
        self.upload_text(f"{base_key}/information_table.xml", information_table_text)
        uploaded["information_table"] = f"{self.config.bucket_name}/{base_key}/information_table.xml"
        if primary_document_text is not None:
            self.upload_text(f"{base_key}/primary_document.xml", primary_document_text)
            uploaded["primary_document"] = f"{self.config.bucket_name}/{base_key}/primary_document.xml"
        return uploaded


def load_storage_from_env() -> ObjectStorage | None:
    """Build an object storage client from env vars, or return None when unset."""

    endpoint_url = os.getenv("WEALTHSIGNAL_OBJECT_STORAGE_ENDPOINT")
    access_key_id = os.getenv("WEALTHSIGNAL_OBJECT_STORAGE_ACCESS_KEY")
    secret_access_key = os.getenv("WEALTHSIGNAL_OBJECT_STORAGE_SECRET_KEY")
    if not endpoint_url or not access_key_id or not secret_access_key:
        return None

    if boto3 is None:
        raise RuntimeError("boto3 is required when object storage is configured")

    secure = os.getenv("WEALTHSIGNAL_OBJECT_STORAGE_SECURE", "false").lower() == "true"
    config = ObjectStorageConfig(
        endpoint_url=endpoint_url,
        access_key_id=access_key_id,
        secret_access_key=secret_access_key,
        bucket_name=os.getenv("WEALTHSIGNAL_OBJECT_STORAGE_BUCKET", DEFAULT_BUCKET_NAME),
        secure=secure,
        region_name=os.getenv("WEALTHSIGNAL_OBJECT_STORAGE_REGION", "us-east-1"),
    )
    client = boto3.client(
        "s3",
        endpoint_url=config.endpoint_url,
        aws_access_key_id=config.access_key_id,
        aws_secret_access_key=config.secret_access_key,
        region_name=config.region_name,
        use_ssl=config.secure,
    )
    return ObjectStorage(config, client)
