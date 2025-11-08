import sys
from pathlib import Path

import pandas as pd
import pytest
from flask import Flask


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


@pytest.fixture
def suprides_client(monkeypatch, tmp_path):
    import app_suprides as sup
    from storage import LocalStorage

    data_dir = tmp_path / "data"
    storage_dir = tmp_path / "storage"
    data_dir.mkdir()
    storage_dir.mkdir()

    monkeypatch.setattr(sup, "CLASSIFIED_CSV", str(data_dir / "suprides_classified.csv"))
    monkeypatch.setattr(sup, "SUPRIDES_CSV", sup.CLASSIFIED_CSV)
    monkeypatch.setattr(sup, "JOB_STATUS_FILE", str(data_dir / "job_status.json"))
    monkeypatch.setattr(sup, "get_storage", lambda: LocalStorage(base_dir=str(storage_dir)))

    sup._set_job_state(**sup._default_job_state())

    app = Flask(__name__)
    app.register_blueprint(sup.bp)
    client = app.test_client()

    yield client, sup

    sup._set_job_state(**sup._default_job_state())


def test_classify_defaults_to_async(monkeypatch, suprides_client):
    client, sup = suprides_client
    triggered = {}

    def fake_start(job_id, flask_app):
        triggered["job_id"] = job_id
        sup._set_job_state(running=True, job_id=job_id)

    monkeypatch.setattr(sup, "_start_classify_job", fake_start)

    resp = client.get("/suprides/classify")
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["success"] is True
    assert data["mode"] == "async"
    assert data["job"]["running"] is True
    assert data["poll_url"].endswith("/suprides/jobs/suprides/status")
    assert triggered["job_id"]
    assert data["message"].startswith("Job de classificação Suprides")
    assert data["csv"].endswith("suprides_classified.csv")
    assert data["review_url"].endswith("/suprides/review_classified")


def test_classify_blocking_flow(monkeypatch, suprides_client):
    client, sup = suprides_client

    def fail_start(*_args, **_kwargs):  # não deve ser chamado em modo blocking
        raise AssertionError("não deve arrancar job assíncrono em modo blocking")

    monkeypatch.setattr(sup, "_start_classify_job", fail_start)

    df = pd.DataFrame(
        [
            {"sku": "SKU1", "name": "Produto 1", "qty_available": 5, "price_cost": 10},
            {"sku": "SKU2", "name": "Produto 2", "qty_available": 0, "price_cost": 7},
        ]
    )

    monkeypatch.setattr(sup, "classify_suprides_products", lambda simulate=False: df)

    captured = {}

    def fake_save(result_df):
        captured["rows"] = list(result_df["sku"].tolist())
        return result_df

    monkeypatch.setattr(sup, "save_suprides_df", fake_save)

    resp = client.get("/suprides/classify?blocking=1")
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["success"] is True
    assert data["mode"] == "blocking"
    assert data["rows"] == 2
    assert data["csv"].endswith("suprides_classified.csv")
    assert data["message"] == "Classificação concluída em modo síncrono."
    assert data["review_url"].endswith("/suprides/review_classified")
    assert captured["rows"] == ["SKU1", "SKU2"]


def test_classify_async_endpoint_respects_running_state(monkeypatch, suprides_client):
    client, sup = suprides_client

    sup._set_job_state(running=True, job_id="existing", success=None)

    called = {"count": 0}

    def fake_start(*_args, **_kwargs):
        called["count"] += 1

    monkeypatch.setattr(sup, "_start_classify_job", fake_start)

    resp = client.post("/suprides/classify_async")
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["already_running"] is True
    assert data["job"]["job_id"] == "existing"
    assert called["count"] == 0
    assert data["message"].startswith("Job de classificação Suprides já estava")


def test_async_job_finalizes_and_updates_state(monkeypatch, suprides_client):
    client, sup = suprides_client

    df = pd.DataFrame(
        [
            {"sku": "SKU1", "title": "Produto 1", "status": "catalog_match"},
            {"sku": "SKU2", "title": "Produto 2", "status": "not_found"},
        ]
    )

    monkeypatch.setattr(sup, "classify_suprides_products", lambda simulate=False: df)

    captured = {}

    def fake_save(result_df):
        captured["rows"] = list(result_df["sku"].tolist())
        return result_df

    monkeypatch.setattr(sup, "save_suprides_df", fake_save)

    class ImmediateThread:
        def __init__(self, target, name=None, daemon=None):
            self._target = target

        def start(self):
            self._target()

    monkeypatch.setattr(sup.threading, "Thread", ImmediateThread)

    resp = client.post("/suprides/classify_async")
    assert resp.status_code == 200
    data = resp.get_json()
    job = data["job"]

    assert job["running"] is False
    assert job["success"] is True
    assert job["rows"] == 2
    assert job["csv"].endswith("suprides_classified.csv")
    assert job["summary"]["catalog_match"] == 1
    assert job["summary"]["not_found"] == 1
    assert captured["rows"] == ["SKU1", "SKU2"]
