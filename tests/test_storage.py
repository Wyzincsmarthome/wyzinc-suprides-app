import sys
import types

import pytest

import storage


def _make_dummy_boto():
    dummy = types.SimpleNamespace()

    def client(service_name, region_name=None):
        return {"service_name": service_name, "region_name": region_name}

    dummy.client = client
    return dummy


def test_s3storage_accepts_bucket_arn(monkeypatch):
    dummy_boto = _make_dummy_boto()
    monkeypatch.setitem(sys.modules, "boto3", dummy_boto)
    monkeypatch.setenv("S3_BUCKET", "arn:aws:s3:::wyzinc-suprides-data/jobs")
    monkeypatch.setenv("S3_REGION", "eu-west-1")
    monkeypatch.delenv("S3_PREFIX", raising=False)

    storage_obj = storage.S3Storage()

    assert storage_obj.bucket == "wyzinc-suprides-data"
    assert storage_obj.prefix == "jobs"
    assert storage_obj.s3["region_name"] == "eu-west-1"


def test_s3storage_explicit_prefix_overrides_arn(monkeypatch):
    dummy_boto = _make_dummy_boto()
    monkeypatch.setitem(sys.modules, "boto3", dummy_boto)
    monkeypatch.setenv("S3_BUCKET", "arn:aws:s3:::wyzinc-suprides-data/jobs")
    monkeypatch.setenv("S3_REGION", "eu-west-1")
    monkeypatch.setenv("S3_PREFIX", "custom/prefix/")

    storage_obj = storage.S3Storage()

    assert storage_obj.bucket == "wyzinc-suprides-data"
    assert storage_obj.prefix == "custom/prefix"
