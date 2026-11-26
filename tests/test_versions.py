"""Two model versions sharing one node: persistence, vocabulary, scheduler,
same-version parallelism, cross-version interleaving and restart stability.
"""

from __future__ import annotations

import asyncio
import sqlite3
import time

import pytest
from fastapi.testclient import TestClient

from app.config import Settings
from app.db import Database
from app.main import create_app
from app.node import SimulatedClassificationNode, classify_text
from app.runner import RunManager
from app.scheduler import VersionScheduler
from app.versions import normalize_model_version

RESIDENT = 128


# ------------------------------------------------------------- version tokens


def test_normalize_version_aliases():
    assert normalize_model_version(None) == "v1"
    assert normalize_model_version("") == "v1"
    assert normalize_model_version("v1") == "v1"
    assert normalize_model_version("V2") == "v2"
    assert normalize_model_version("new") == "v2"
    assert normalize_model_version("original") == "v1"
    assert normalize_model_version("新版") == "v2"
    assert normalize_model_version("原版") == "v1"
    with pytest.raises(ValueError):
        normalize_model_version("v9")


def test_two_vocabularies_are_different():
    # A new-lexicon text: v2 has explicit keyword hits, and across a fixed
    # corpus the two versions must not produce identical classifications.
    corpus = [
        "量子计算与可控核聚变取得突破，算力需求上升",
        "基因编辑与靶向药物临床试验进展",
        "季后赛转会消息与马拉松比赛",
        "峰会制裁关税与北约条约",
        "zzz qqq unrelated words alpha beta",
        "another no keyword sentence gamma delta",
    ]
    pairs = [
        (classify_text(t, f"D{i}", "v1"), classify_text(t, f"D{i}", "v2"))
        for i, t in enumerate(corpus)
    ]
    assert any(a != b for a, b in pairs), pairs
    # v2 keyword classification for its own lexicon is deterministic.
    c1 = classify_text("量子 核聚变 算力 大模型", "X", "v2")
    c2 = classify_text("量子 核聚变 算力 大模型", "X", "v2")
    assert c1 == c2 and c1[0] == "technology"


# ----------------------------------------------------------------- DB migration


_OLD_SCHEMA = """
CREATE TABLE jobs (
    job_id TEXT PRIMARY KEY, status TEXT NOT NULL, total INTEGER NOT NULL,
    initial_batch_size INTEGER NOT NULL, current_batch_size INTEGER NOT NULL,
    vram_limit_mb INTEGER, last_error TEXT, created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL, started_at TEXT, finished_at TEXT
);
CREATE TABLE documents (
    job_id TEXT NOT NULL, seq INTEGER NOT NULL, doc_id TEXT NOT NULL,
    content TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'pending',
    category TEXT, confidence REAL, vram_estimate_mb INTEGER NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0, claimed_by TEXT, claimed_at TEXT,
    error_code TEXT, error_message TEXT, created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL, finished_at TEXT, PRIMARY KEY (job_id, seq),
    UNIQUE (job_id, doc_id)
);
CREATE TABLE dispatch_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT, job_id TEXT NOT NULL,
    seq INTEGER NOT NULL, batch_id TEXT NOT NULL, vram_limit_mb INTEGER NOT NULL,
    estimate_mb INTEGER NOT NULL, dispatched_at TEXT NOT NULL
);
CREATE TABLE node_settings (
    id INTEGER PRIMARY KEY CHECK (id = 1), vram_limit_mb INTEGER NOT NULL,
    per_doc_delay_ms INTEGER NOT NULL, updated_at TEXT NOT NULL
);
"""


def test_legacy_database_rows_default_to_original_v1(tmp_path):
    db_path = tmp_path / "legacy.db"
    raw = sqlite3.connect(db_path)
    raw.executescript(_OLD_SCHEMA)
    raw.execute(
        "INSERT INTO jobs (job_id,status,total,initial_batch_size,"
        "current_batch_size,created_at,updated_at) "
        "VALUES ('OLD','completed',1,1,1,'t','t')"
    )
    raw.execute(
        "INSERT INTO dispatch_log (job_id,seq,batch_id,vram_limit_mb,"
        "estimate_mb,dispatched_at) VALUES ('OLD',0,'b_x',512,50,'t')"
    )
    raw.commit()
    raw.close()

    # Opening with the new code runs idempotent ALTER TABLE migration.
    db = Database(str(db_path))
    job = db.get_job("OLD")
    assert job["model_version"] == "v1"
    row = db.list_dispatches("OLD")[0]
    assert row["model_version"] == "v1"
    # Migration is idempotent: a second init does not raise.
    db.init_schema()
    db.close()

    # Reopen once more for good measure.
    db2 = Database(str(db_path))
    assert db2.get_job("OLD")["model_version"] == "v1"
    db2.close()


def test_create_job_persists_canonical_version(tmp_path):
    db = Database(str(tmp_path / "v.db"))
    docs = [{"doc_id": "d", "content": "x", "vram_estimate_mb": 50}]
    db.create_job("J1", docs, 1, 4096, model_version="new")
    assert db.get_job("J1")["model_version"] == "v2"
    db.create_job("J2", docs, 1, 4096)
    assert db.get_job("J2")["model_version"] == "v1"
    db.close()


# ------------------------------------------------------------ scheduler unit


async def _unit_batch(gate, version, hold, name, fail=False):
    ticket = await gate.admit(version)
    try:
        gate.note_dispatched(ticket)
        await asyncio.sleep(hold)
        if fail:
            raise RuntimeError(f"boom-{name}")
    finally:
        gate.release(ticket)


def test_scheduler_same_version_parallel_and_fifo_switch():
    gate = VersionScheduler()

    async def scenario():
        await asyncio.gather(
            _unit_batch(gate, "v1", 0.10, "A"),
            _unit_batch(gate, "v1", 0.12, "B"),
            _delayed(gate, "v2", 0.02, 0.04, "C"),
            _delayed(gate, "v1", 0.03, 0.04, "D"),
        )

    asyncio.run(scenario())
    assert gate.peak_in_flight_by_version["v1"] >= 2
    assert [(s["from"], s["to"]) for s in gate.switches] == [
        (None, "v1"),
        ("v1", "v2"),
        ("v2", "v1"),
    ]


async def _delayed(gate, version, delay, hold, name):
    await asyncio.sleep(delay)
    await _unit_batch(gate, version, hold, name)


def test_scheduler_failed_batch_still_allows_switch():
    gate = VersionScheduler()

    async def scenario():
        outcomes = await asyncio.gather(
            _unit_batch(gate, "v1", 0.03, "boom", fail=True),
            _delayed(gate, "v2", 0.01, 0.02, "C"),
            return_exceptions=True,
        )
        assert isinstance(outcomes[0], RuntimeError)

    asyncio.run(scenario())
    assert gate.resident_version == "v2"
    assert gate.switch_count >= 1
    assert sum(gate.in_flight_counts().values()) == 0


def test_scheduler_minimum_one_batch_before_next_switch():
    """A v1 waiter queued immediately behind a fresh v2 residency must not
    cause the gate to switch back before v2's admitted batch dispatches.
    """
    gate = VersionScheduler()
    progress: list[str] = []

    async def tracked(version, name, hold, delay=0.0):
        if delay:
            await asyncio.sleep(delay)
        ticket = await gate.admit(version)
        progress.append(f"admit-{name}")
        gate.note_dispatched(ticket)
        progress.append(f"dispatch-{name}")
        await asyncio.sleep(hold)
        gate.release(ticket)

    async def scenario():
        await asyncio.gather(
            tracked("v1", "A", 0.05),
            tracked("v2", "B", 0.02, delay=0.01),
            tracked("v1", "C", 0.02, delay=0.015),
        )

    asyncio.run(scenario())
    # B is dispatched exactly once before C is admitted.
    assert progress.index("dispatch-B") < progress.index("admit-C")
    assert [(s["to"]) for s in gate.switches] == ["v1", "v2", "v1"]


# ------------------------------------------------------- runner integration


def _build_manager(tmp_path, name="iv.db", delay_ms=60):
    db = Database(str(tmp_path / name))
    db.seed_node_settings(4096, delay_ms)
    node = SimulatedClassificationNode(db, model_resident_mb=RESIDENT)
    scheduler = VersionScheduler()
    manager = RunManager(db, node, scheduler=scheduler)
    return db, node, scheduler, manager


def _docs(prefix, n):
    return [
        {
            "doc_id": f"{prefix}-{i}",
            "content": "普通短文本，银行芯片医院球队政府",
            "vram_estimate_mb": 50,
        }
        for i in range(n)
    ]


def test_runner_same_version_batches_overlap(tmp_path):
    db, node, gate, manager = _build_manager(tmp_path, "parallel.db", 80)
    db.create_job("A", _docs("A", 2), 2, 4096, model_version="v1")
    db.create_job("B", _docs("B", 2), 2, 4096, model_version="v1")

    async def scenario():
        await asyncio.gather(manager.run_job("A"), manager.run_job("B"))

    asyncio.run(scenario())
    assert db.status_counts("A")["succeeded"] == 2
    assert db.status_counts("B")["succeeded"] == 2
    # Two v1 batches were genuinely in the node at the same time.
    assert node.peak_active_batches >= 2
    assert gate.peak_in_flight_by_version.get("v1", 0) >= 2
    versions = {r["model_version"] for r in db.list_dispatches("A")}
    assert versions == {"v1"}
    db.close()


def test_runner_waiting_version_batch_has_no_dispatch_row(tmp_path):
    db, _node, gate, manager = _build_manager(tmp_path, "interleave.db", 120)
    # A: one slow batch of 2 v1 docs.
    db.create_job("A", _docs("A", 2), 2, 4096, model_version="v1")
    # B: a single v2 doc, will queue while A is in flight.
    db.create_job("B", _docs("B", 1), 1, 4096, model_version="v2")

    async def scenario():
        task_a = asyncio.create_task(manager.run_job("A"))
        deadline = time.monotonic() + 2
        while gate.snapshot()["in_flight_batches"] < 1:
            await asyncio.sleep(0.005)
            if time.monotonic() > deadline:
                raise AssertionError("v1 batch never became in flight")

        task_b = asyncio.create_task(manager.run_job("B"))
        deadline = time.monotonic() + 2
        while gate.queued_counts().get("v2", 0) < 1:
            await asyncio.sleep(0.005)
            if time.monotonic() > deadline:
                raise AssertionError("v2 batch never queued")

        # The queued v2 batch must not touch the dispatch ledger yet.
        assert db.list_dispatches("B") == []
        snap = gate.snapshot()
        assert snap["resident_model_version"] == "v1"

        await asyncio.gather(task_a, task_b)

    asyncio.run(scenario())

    rows_b = db.list_dispatches("B")
    assert len(rows_b) == 1
    assert rows_b[0]["model_version"] == "v2"
    assert db.status_counts("A")["succeeded"] == 2
    assert db.status_counts("B")["succeeded"] == 1
    # Exactly one cross switch, v1 -> v2.
    assert [(s["from"], s["to"]) for s in gate.switches] == [
        (None, "v1"),
        ("v1", "v2"),
    ]
    db.close()


def test_runner_two_version_interleaving_and_pause(tmp_path):
    db, _node, gate, manager = _build_manager(tmp_path, "mix.db", 40)
    # A has 6 docs / batch 2 -> several sequential v1 batches.
    db.create_job("A", _docs("A", 6), 2, 4096, model_version="v1")
    db.create_job("B", _docs("B", 2), 2, 4096, model_version="v2")
    db.create_job("C", _docs("C", 2), 2, 4096, model_version="v1")

    async def scenario():
        task_a = asyncio.create_task(manager.run_job("A"))
        # Let A's first v1 batch enter the node.
        deadline = time.monotonic() + 2
        while gate.snapshot()["in_flight_batches"] < 1:
            await asyncio.sleep(0.005)
            if time.monotonic() > deadline:
                raise AssertionError("A never started")
        # Queue B (v2) then C (v1) while A is in flight.
        task_b = asyncio.create_task(manager.run_job("B"))
        await asyncio.sleep(0.01)
        task_c = asyncio.create_task(manager.run_job("C"))
        await asyncio.gather(task_a, task_b, task_c)

    asyncio.run(scenario())

    for jid, n in (("A", 6), ("B", 2), ("C", 2)):
        assert db.status_counts(jid)["succeeded"] == n

    # Every dispatch row carries its own job's fixed version.
    for jid, ver in (("A", "v1"), ("B", "v2"), ("C", "v1")):
        rows = db.list_dispatches(jid)
        assert rows and all(r["model_version"] == ver for r in rows)

    # Global dispatch id order must show the v2 epoch between v1 epochs,
    # never a v2 row before the first v1 dispatch.
    all_rows = []
    for jid in ("A", "B", "C"):
        all_rows.extend(db.list_dispatches(jid))
    all_rows.sort(key=lambda r: r["id"])
    versions_in_order = [r["model_version"] for r in all_rows]
    first_v2 = versions_in_order.index("v2")
    assert first_v2 >= 2  # A's first batch (2 docs) was dispatched first
    assert "v1" in versions_in_order[first_v2 + 1 :]
    db.close()


def test_restart_resume_keeps_model_version(tmp_path):
    db_path = str(tmp_path / "restart_version.db")
    db = Database(db_path)
    db.seed_node_settings(4096, 0)
    db.create_job("V2J", _docs("V2J", 4), 2, 4096, model_version="v2")
    # Simulate one terminal doc and one claimed batch from a dead process.
    db.mark_succeeded("V2J", 0, "technology", 0.9)
    db.claim_next_batch(
        "V2J", 2, "dead-owner", 4096, model_version="v2"
    )
    reset_docs, _ = db.reset_stale_work()
    assert reset_docs == 2
    db.close()

    # ---- new process: fresh Database / Node / Manager, same file ----
    db2 = Database(db_path)
    assert db2.get_job("V2J")["model_version"] == "v2"
    node2 = SimulatedClassificationNode(db2, model_resident_mb=RESIDENT)
    manager2 = RunManager(db2, node2)
    asyncio.run(manager2.run_job("V2J"))

    assert db2.status_counts("V2J")["succeeded"] == 4
    rows = db2.list_dispatches("V2J")
    # Historical + post-restart rows all carry v2.
    assert rows and all(r["model_version"] == "v2" for r in rows)
    db2.close()


# ------------------------------------------------------------------- HTTP API


@pytest.fixture
def client(tmp_path):
    settings = Settings(
        db_path=str(tmp_path / "api_versions.db"),
        default_vram_limit_mb=4096,
        default_batch_size=4,
        default_delay_ms=0,
        model_resident_mb=128,
        doc_base_mb=48,
        mb_per_100_chars=4,
    )
    app = create_app(settings)
    with TestClient(app) as c:
        yield c


def test_http_default_version_and_aliases(client: TestClient):
    r = client.post("/jobs", json={"documents": [{"id": "d", "text": "文本"}]})
    assert r.status_code == 201, r.text
    body = r.json()
    assert body["model_version"] == "v1"
    assert body["version"] == "v1"
    assert body["model_version_label"] == "original"

    r2 = client.post(
        "/jobs",
        json={
            "job_id": "NEW1",
            "version": "new",
            "documents": [{"id": "d", "text": "量子算力"}],
        },
    )
    assert r2.status_code == 201, r2.text
    b2 = r2.json()
    assert b2["model_version"] == "v2"
    assert b2["model_version_label"] == "new"
    assert client.get("/jobs/NEW1").json()["model_version"] == "v2"


def test_http_invalid_version_is_422(client: TestClient):
    r = client.post(
        "/jobs",
        json={"model_version": "v3", "documents": [{"id": "d", "text": "x"}]},
    )
    assert r.status_code == 422


def test_http_boolean_and_generation_selectors(client: TestClient):
    for payload, expected in [
        ({"use_new_model": True}, "v2"),
        ({"use_new_model": False}, "v1"),
        ({"new_version": "true"}, "v2"),
        ({"generation": 2}, "v2"),
        ({"model": "original"}, "v1"),
    ]:
        r = client.post(
            "/jobs",
            json={**payload, "documents": [{"id": "d", "text": "文本"}]},
        )
        assert r.status_code == 201, (payload, r.text)
        body = r.json()
        assert body["model_version"] == expected, (payload, body)
        # Response aliases carry the same canonical token.
        assert body["version"] == expected
        assert body["model"] == expected
        assert body["model_name"] == expected


def test_http_dispatch_rows_carry_version(client: TestClient):
    r = client.post(
        "/jobs",
        json={
            "job_id": "V2HTTP",
            "model_version": "v2",
            "vram_limit_mb": 4096,
            "documents": [
                {"id": str(i), "text": "量子核聚变算力大模型"}
                for i in range(3)
            ],
        },
    )
    assert r.status_code == 201

    deadline = time.time() + 10
    while time.time() < deadline:
        body = client.get("/jobs/V2HTTP").json()
        if body["status"] in ("completed", "completed_with_failures", "error"):
            break
        time.sleep(0.02)
    assert body["status"] == "completed", body

    d = client.get("/jobs/V2HTTP/dispatches").json()
    assert d["model_version"] == "v2"
    assert d["dispatch_count"] == 3
    assert all(row["model_version"] == "v2" for row in d["dispatches"])
    assert all(row["version"] == "v2" for row in d["dispatches"])


def test_http_scheduler_endpoint_and_models(client: TestClient):
    cfg = client.get("/admin/node").json()
    assert cfg["model_versions"] == ["v1", "v2"]
    assert "scheduler" in cfg and "resident_model_version" in cfg

    sched = client.get("/admin/scheduler")
    assert sched.status_code == 200
    assert "switch_count" in sched.json()

    models = client.get("/models")
    assert models.status_code == 200
    versions = [v["model_version"] for v in models.json()["versions"]]
    assert versions == ["v1", "v2"]
