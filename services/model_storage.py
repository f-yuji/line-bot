"""Supabase Storage helpers for rebound model artifacts."""
from __future__ import annotations

import logging
import os
from pathlib import Path

logger = logging.getLogger(__name__)

DEFAULT_MODEL_BUCKET = "rebound-models"


def model_bucket_name() -> str:
    return os.getenv("MODEL_STORAGE_BUCKET", DEFAULT_MODEL_BUCKET).strip() or DEFAULT_MODEL_BUCKET


def _storage_path(path: str | Path) -> str:
    return str(path).replace("\\", "/").lstrip("/")


def ensure_model_bucket(sb, bucket: str | None = None) -> str:
    bucket_name = bucket or model_bucket_name()
    try:
        buckets = sb.storage.list_buckets()
        names = {getattr(b, "name", None) or (b.get("name") if isinstance(b, dict) else None) for b in buckets}
        if bucket_name in names:
            return bucket_name
    except Exception as e:
        logger.warning("[model_storage] bucket list failed: %s", e)
    try:
        sb.storage.create_bucket(bucket_name, options={"public": False})
        logger.info("[model_storage] created bucket=%s", bucket_name)
    except Exception as e:
        logger.warning("[model_storage] bucket create skipped/failed bucket=%s error=%s", bucket_name, e)
    return bucket_name


def upload_model_artifact(sb, local_path: str | Path, storage_path: str | Path | None = None, *, bucket: str | None = None) -> bool:
    path = Path(local_path)
    if not path.exists():
        logger.warning("[model_storage] upload skipped missing file=%s", path)
        return False
    bucket_name = ensure_model_bucket(sb, bucket)
    object_path = _storage_path(storage_path or path)
    data = path.read_bytes()
    storage = sb.storage.from_(bucket_name)
    try:
        storage.upload(object_path, data, file_options={"upsert": "true"})
    except Exception as e:
        # Older Supabase Storage APIs may reject upsert. Remove and retry once.
        logger.warning("[model_storage] upload upsert failed path=%s error=%s", object_path, e)
        try:
            storage.remove([object_path])
        except Exception:
            pass
        storage.upload(object_path, data)
    logger.info("[model_storage] uploaded bucket=%s path=%s bytes=%d", bucket_name, object_path, len(data))
    return True


def upload_model_artifacts(sb, paths: list[str | Path], *, root: str | Path, bucket: str | None = None) -> int:
    root_path = Path(root)
    count = 0
    for path in paths:
        local_path = Path(path)
        try:
            object_path = local_path.relative_to(root_path)
        except ValueError:
            object_path = local_path
        if upload_model_artifact(sb, local_path, object_path, bucket=bucket):
            count += 1
    return count


def download_model_artifact(sb, storage_path: str | Path, local_path: str | Path, *, bucket: str | None = None) -> bool:
    bucket_name = bucket or model_bucket_name()
    object_path = _storage_path(storage_path)
    target = Path(local_path)
    storage = sb.storage.from_(bucket_name)
    try:
        data = storage.download(object_path)
    except Exception as e:
        logger.warning("[model_storage] download failed bucket=%s path=%s error=%s", bucket_name, object_path, e)
        return False
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(data)
    logger.info("[model_storage] downloaded bucket=%s path=%s -> %s bytes=%d", bucket_name, object_path, target, len(data))
    return True

