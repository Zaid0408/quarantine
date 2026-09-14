"""The GCS backend: same promises as the local folder, different primitives."""

from __future__ import annotations

import sys
import uuid
import os
import pytest

from quarantine import Quarantine, StorageBackend, open_store, quarantine, register_backend, retry
from quarantine.core import Config
from quarantine.errors import StorageError
from quarantine.gcs_store import GCSStore
from quarantine.store import coerce_dir

from google.auth.credentials import AnonymousCredentials
from google.cloud import storage


BUCKET = "quarantine-test-bucket-gcs"

SOURCE = """
some_source_data = 100 
"""

def _client():
    """Helper method to instantiate the GCS client pointed at the emulator.
    
    Using AnonymousCredentials prevents the library from attempting to load 
    real Google application default credentials from your local machine.
    """
    return storage.Client(
        credentials=AnonymousCredentials(),
        project="test-project"
    )
    
@pytest.fixture(autouse=True)
def setup_gcs_emulator_env(monkeypatch):
    """Automatically ensures the client routing environment variables are set.
    
    This replaces the AWS credential monkeypatching from your S3 setup.
    """
    # Force the google-cloud-storage library to route requests to the local Docker container
    monkeypatch.setenv("STORAGE_EMULATOR_HOST", "http://localhost:4443")
    
@pytest.fixture
def module(target_module):
    return target_module(SOURCE, name="qtarget_gcs")

@pytest.fixture
def gcs_url():
    """A mocked bucket plus a unique prefix, so tests cannot see each other.
    
    Mirrors your s3_url fixture by setting up the mock state on fake-gcs-server.
    """
    client = _client()
    
    # Create the bucket in the emulator if it doesn't exist yet
    if not client.lookup_bucket(BUCKET):
        client.create_bucket(BUCKET)
        
    # Return the target path using the official cloud URI scheme
    yield f"gs://{BUCKET}/{uuid.uuid4().hex}"

@pytest.fixture
def gcs(gcs_url):
    """Returns an instance of your custom GCS storage module."""
    # Assuming GCSStore initializes using a gs:// path like your S3Store did
    return GCSStore(gcs_url)