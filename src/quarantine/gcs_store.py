from typing import TYPE_CHECKING, Any
import hashlib
import json
import shutil
import tempfile
import threading
from collections.abc import Iterable
from pathlib import Path
from typing import TYPE_CHECKING, Any


from .errors import StorageError
from .store import StorageBackend, build_record

if TYPE_CHECKING:  # pragma: no cover - typing only
    from types import ModuleType
    

def _import_gcs()-> tuple[ModuleType, type[Exception], type[Exception]]:
    try:
        from google.api_core.exceptions import (  # noqa: PLC0415
            GoogleAPICallError,
            PreconditionFailed,
        )
        from google.cloud import storage
    except ImportError as exc:
        raise StorageError(
            "the gcs:// backend needs google-cloud-storage, which is an optional extra: "
            'pip install "quarantine-py[gcs]"'
        ) from exc
    
    return storage,PreconditionFailed, GoogleAPICallError


class GCSStore(StorageBackend):
    "A Quarantine stored as per-record objects in a GCS bucket."
    
    def __init__(self,url:str) -> None:
        if not url.startswith("gs://"):
            raise StorageError(f"not a gs:// URL: {url!r}")
        rest = url[len("gs://") :]
        bucket, _, prefix = rest.partition("/")
        if not bucket:
            raise StorageError(f"{url!r} is missing a bucket name (gs://bucket/prefix)")
        storage, precondition_failed, api_error = _import_gcs()
        self.dir: str = url.rstrip("/")
        self.bucket_name = bucket
        self.prefix = prefix.strip("/")
        self.problems: list[str] = []
        self._client = storage.Client()
        self._bucket = self._client.bucket(bucket)
        self._precondition_failed = precondition_failed
        self._api_error = api_error
        self._mutex = threading.Lock()
        self._id_hint = 0
        digest = hashlib.sha256(self.dir.encode("utf-8")).hexdigest()[:12]
        self._cache = Path(tempfile.gettempdir()) / f"quarantine-gcs-{digest}"
        
    def __repr__(self) -> str:
        return f"GCSStore({self.dir!r})"
    
    # keys
    
    def _key(self, record_id: int, name: str) -> str:
        base=f"{record_id:04d}/{name}"
        return f"{self.prefix}/{base}" if self.prefix else base
    def _list_prefix(self) -> str:
        return f"{self.prefix}/" if self.prefix else ""

    def _wrap(self, action: str, exc: Exception) -> StorageError:
        return StorageError(f"cannot {action} in {self.dir}: {exc}")
