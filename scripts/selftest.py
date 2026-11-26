#!/usr/bin/env python3
"""End-to-end self-test for the classifier service.

Two ways to run:

1. Fully local (spawns uvicorn on a random port with a temp SQLite file,
   SIGKILLs it mid-job and starts it again)::

       python scripts/selftest.py all

2. Against an already running server (e.g. ``docker compose up``)::

       python scripts/selftest.py --base-url http://localhost:8000
       python scripts/selftest.py --base-url http://localhost:8000 \\
           --restart-cmd "docker compose restart classifier-api"

Only the Python standard library is required on the host.
"""

from __future__ import annotations

import argparse
import json
import os
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.request
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parent.parent

PASS = "✅ PASS"
FAIL = "❌ FAIL"
INFO = "ℹ️ "


class AssertFailed(Exception):
    pass


def check(cond: bool, msg: str) -> None:
    if not cond:
        raise AssertFailed(msg)
    print(f"{PASS} {msg}")


class Client:
    def __init__(self, base_url: str):
        self.base = base_url.rstrip("/")

    def _request(
        self, method: str, path: str, body: dict | None = None,
        timeout: float = 30.0,
    ) -> tuple[int, Any]:
        data = None
        headers = {"Accept": "application/json"}
        if body is not None:
            data = json.dumps(body).encode("utf-8")
            headers["Content-Type"] = "application/json"
        req = urllib.request.Request(
            self.base + path, data=data, headers=headers, method=method
        )
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                raw = resp.read().decode("utf-8")
                return resp.status, json.loads(raw) if raw else None
        except urllib.error.HTTPError as e:
            raw = e.read().decode("utf-8")
            try:
                return e.code, json.loads(raw)
            except json.JSONDecodeError:
                return e.code, raw

    def get(self, path: str) -> Any:
        code, body = self._request("GET", path)
        if code != 200:
            raise AssertFailed(f"GET {path} -> {code}: {body}")
        return body

    def post(self, path: str, body: dict | None = None) -> tuple[int, Any]:
        return self._request("POST", path, body or {})

    def put(self, path: str, body: dict) -> Any:
        code, resp = self._request("PUT", path, body)
        if code != 200:
            raise AssertFailed(f"PUT {path} -> {code}: {resp}")
        return resp

    def wait_health(self, timeout: float = 30.0) -> dict:
        deadline = time.time() + timeout
        last = None
        while time.time() < deadline:
            try:
                return self.get("/health")
            except Exception as exc:  # noqa: BLE001
                last = exc
                time.sleep(0.25)
        raise AssertFailed(f"server never became healthy: {last}")

    def wait_terminal(
        self, job_id: str, timeout: float = 60.0
    ) -> dict[str, Any]:
        deadline = time.time() + timeout
        last_status = None
        while time.time() < deadline:
            job = self.get(f"/jobs/{job_id}")
            last_status = job["status"]
            if last_status in (
                "completed",
                "completed_with_failures",
                "error",
            ):
                return job
            time.sleep(0.2)
        raise AssertFailed(
            f"job {job_id} did not finish within {timeout}s "
            f"(last status {last_status})"
        )


def short_doc(i: int) -> dict:
    topics = [
        "央行银行金融财报投资股票",
        "球队比赛进球体育联赛",
        "芯片算法人工智能软件数据",
        "医院药物疫苗健康医学",
    ]
    return {"id": f"D-{i:03d}", "text": f"文档{i:03d}：{topics[i % 4]}。" * 3}


def wait_scheduler_condition(
    cli: Client,
    predicate: Callable[[dict], bool],
    timeout: float = 5.0,
) -> dict:
    deadline = time.time() + timeout
    last: dict = {}
    while time.time() < deadline:
        last = cli.get("/admin/scheduler")
        if predicate(last):
            return last
        time.sleep(0.03)
    raise AssertFailed(f"scheduler condition not met; last snapshot={last}")


# ---------------------------------------------------------------- scenarios


def scenario_shrink(cli: Client) -> None:
    print("\n=== 场景 1：部分成功 → OOM 缩批 → 单篇失败 → 其余完成 ===")
    cli.put("/admin/node", {"vram_limit_mb": 512, "per_doc_delay_ms": 0})

    code, demo = cli.post("/demo/run")
    check(code in (200, 201), f"POST /demo/run 返回 {code}")
    job_id = demo["job_id"]
    check(job_id == "JOB-DEMO-SHRINK", "演示作业号为 JOB-DEMO-SHRINK")

    job = cli.wait_terminal(job_id)
    check(job["status"] == "completed_with_failures",
          f"作业终态为 completed_with_failures（实际 {job['status']}）")
    c = job["counts"]
    check(c["succeeded"] == 7 and c["failed"] == 1,
          f"7 成功 / 1 失败（实际 成功{c['succeeded']} 失败{c['failed']}）")
    check(c["pending"] == 0 and c["processing"] == 0,
          "没有遗留 pending/processing 文档")
    check(job["current_batch_size"] == 1,
          f"批大小已从 4 缩到 1（实际 {job['current_batch_size']}）")

    docs = job["documents"]
    check([d["seq"] for d in docs] == list(range(8)),
          "查询结果严格按提交顺序(seq)排列")
    failed = [d for d in docs if d["status"] == "failed"]
    check(len(failed) == 1 and failed[0]["doc_id"] == "DEMO-05",
          "唯一失败是超长单篇 DEMO-05")
    check(failed[0]["error_code"] == "document_exceeds_vram_limit",
          "失败码 document_exceeds_vram_limit")
    check("512MB" in (failed[0]["error_message"] or ""),
          "失败原因包含显存需求与上限说明")
    check(all(d["category"] for d in docs if d["status"] == "succeeded"),
          "成功文档都带分类结果")

    dispatches = cli.get(f"/jobs/{job_id}/dispatches")["dispatches"]
    first_batch_id = dispatches[0]["batch_id"]
    first_batch_seqs = sorted(
        d["seq"] for d in dispatches if d["batch_id"] == first_batch_id
    )
    check(first_batch_seqs == [0, 1, 2, 3],
          f"首次派发为整批 4 篇（实际 seq={first_batch_seqs}）")
    # DEMO-03 (seq2) was in the OOM batch and then re-dispatched smaller.
    seq2 = [d for d in dispatches if d["seq"] == 2]
    check(len(seq2) == 2 and seq2[0]["batch_id"] != seq2[1]["batch_id"],
          "触发 OOM 的未完成文档在更小批次中被重试")


def scenario_concurrent_resume(cli: Client, tag: str = "CONC") -> None:
    print("\n=== 场景 2：并发续跑去重（无重启部分） ===")
    cli.put("/admin/node", {"vram_limit_mb": 4096, "per_doc_delay_ms": 120})
    job_id = f"JOB-{tag}-{int(time.time())}"
    code, _body = cli.post("/jobs", {
        "job_id": job_id,
        "batch_size": 4,
        "vram_limit_mb": 4096,
        "documents": [short_doc(i) for i in range(12)],
    })
    check(code == 201, f"创建并发测试作业 {job_id}")
    # It auto-starts; hammer resume concurrently.
    with ThreadPoolExecutor(max_workers=10) as pool:
        responses = list(pool.map(
            lambda _i: cli.post(f"/jobs/{job_id}/resume"), range(10)
        ))
    codes = [c for c, _ in responses]
    check(all(c == 200 for c in codes), "10 个并发 resume 请求全部 200")
    started_true = sum(1 for _, b in responses if b.get("started") is True)
    check(started_true == 0,
          f"作业已自动运行，没有任何 resume 重复启动（started=true 数={started_true}）")

    job = cli.wait_terminal(job_id)
    check(job["counts"]["succeeded"] == 12, "12 篇全部成功")
    rows = cli.get(f"/jobs/{job_id}/dispatches")["dispatches"]
    seqs = [r["seq"] for r in rows]
    check(sorted(seqs) == list(range(12)) and len(set(seqs)) == 12,
          "每篇文档恰好派发一次，无重复派发")
    cli.put("/admin/node", {"vram_limit_mb": 512, "per_doc_delay_ms": 0})


def scenario_restart_resume(
    cli: Client,
    restart_fn: Callable[[], None] | None,
) -> None:
    print("\n=== 场景 3：kill -9 / 重启后按原作业号续跑 + 8 并发 resume ===")
    if restart_fn is None:
        print(f"{INFO}未提供重启手段，跳过本场景（本地 all 模式会完整执行）")
        return

    cli.put("/admin/node", {"vram_limit_mb": 4096, "per_doc_delay_ms": 150})
    job_id = f"JOB-RESTART-{int(time.time())}"
    code, _ = cli.post("/jobs", {
        "job_id": job_id,
        "batch_size": 4,
        "vram_limit_mb": 4096,
        "documents": [short_doc(i) for i in range(16)],
    })
    check(code == 201, f"创建重启测试作业 {job_id}")

    # Let some documents become terminal and at least one batch in flight.
    terminal_before: set[int] = set()
    max_dispatch_id = 0
    deadline = time.time() + 10
    while time.time() < deadline:
        job = cli.get(f"/jobs/{job_id}")
        terminal_before = {
            d["seq"] for d in job["documents"]
            if d["status"] in ("succeeded", "failed")
        }
        rows = cli.get(f"/jobs/{job_id}/dispatches")["dispatches"]
        max_dispatch_id = max((r["id"] for r in rows), default=0)
        if terminal_before and job["counts"]["processing"] > 0:
            break
        time.sleep(0.1)
    check(len(terminal_before) >= 1,
          f"kill 前已有终态文档 {sorted(terminal_before)}")
    print(f"{INFO}kill 前终态 seq={sorted(terminal_before)}，"
          f"dispatch id 上界={max_dispatch_id}")

    restart_fn()
    health = cli.wait_health(timeout=40)
    reset_info = health.get("stale_reset_on_startup") or {}
    check(reset_info.get("reset_documents", 0) >= 1,
          f"启动时把孤儿 processing 重置为 pending：{reset_info}")

    job_after_boot = cli.get(f"/jobs/{job_id}")
    check(job_after_boot["status"] == "interrupted",
          f"崩溃作业被标记 interrupted（实际 {job_after_boot['status']}）")

    # Concurrent resumes against the SAME original job id.
    barrier = threading.Barrier(8)

    def do_resume(_i):
        barrier.wait()
        return cli.post(f"/jobs/{job_id}/resume")

    with ThreadPoolExecutor(max_workers=8) as pool:
        responses = list(pool.map(do_resume, range(8)))
    check(all(c == 200 for c, _ in responses), "8 个并发续跑请求全部 200")
    started = [b.get("started") for _, b in responses]
    check(sum(1 for s in started if s is True) == 1,
          f"恰好 1 个请求真正启动续跑（started 分布={started}）")

    job = cli.wait_terminal(job_id, timeout=60)
    check(job["status"] == "completed",
          f"续跑后作业 completed（实际 {job['status']}）")
    check(job["counts"]["succeeded"] == 16, "16 篇全部成功")

    rows = cli.get(f"/jobs/{job_id}/dispatches")["dispatches"]
    new_rows = [r for r in rows if r["id"] > max_dispatch_id]
    leaked = sorted({r["seq"] for r in new_rows} & terminal_before)
    check(not leaked,
          f"重启前已终态的文档在重启后零派发（泄漏 seq={leaked}）")
    new_seqs = [r["seq"] for r in new_rows]
    check(len(new_seqs) == len(set(new_seqs)),
          f"并发续跑没有重复派发同一文档（重启后派发 seq={sorted(new_seqs)}）")
    print(f"{INFO}重启后新派发 {len(new_rows)} 篇：{sorted(set(new_seqs))}")
    cli.put("/admin/node", {"vram_limit_mb": 512, "per_doc_delay_ms": 0})


def scenario_same_version_parallel(cli: Client) -> None:
    print("\n=== 场景 4：同版作业的批次可并行（两批在节点内重叠） ===")
    # One batch = 2 docs * 300ms ~= 600ms, a wide overlap window.
    cli.put("/admin/node", {"vram_limit_mb": 4096, "per_doc_delay_ms": 300})
    ts = int(time.time() * 1000)
    job_a = f"JOB-PAR-A-{ts}"
    job_b = f"JOB-PAR-B-{ts}"
    for jid in (job_a, job_b):
        code, _ = cli.post("/jobs", {
            "job_id": jid,
            "model_version": "v2",
            "batch_size": 2,
            "vram_limit_mb": 4096,
            "documents": [short_doc(1), short_doc(2)],
        })
        check(code == 201, f"创建同版(v2)并行作业 {jid}")

    # Observe the overlap directly in the scheduler snapshot.
    snap = wait_scheduler_condition(
        cli, lambda s: s.get("in_flight_batches", 0) >= 2, timeout=5
    )
    check(snap["in_flight_batches"] >= 2,
          f"同一时刻至少 2 个同版批次在途（实际 {snap['in_flight_batches']}）")
    check(snap.get("in_flight_batches_by_version", {}).get("v2", 0) >= 2,
          f"两个在途批次都属于 v2：{snap.get('in_flight_batches_by_version')}")

    cli.wait_terminal(job_a, timeout=30)
    cli.wait_terminal(job_b, timeout=30)
    node_cfg = cli.get("/admin/node")
    check(node_cfg["node_concurrency"]["peak_active_batches"] >= 2,
          "节点侧实测峰值并发批次 >= 2："
          f"{node_cfg['node_concurrency'].get('peak_active_batches')}")
    for jid in (job_a, job_b):
        rows = cli.get(f"/jobs/{jid}/dispatches")["dispatches"]
        check(len(rows) == 2 and all(r["model_version"] == "v2" for r in rows),
              f"{jid} 的 2 条流水都记录 model_version=v2")
    cli.put("/admin/node", {"vram_limit_mb": 512, "per_doc_delay_ms": 0})


def scenario_versions_interleave(cli: Client) -> None:
    print("\n=== 场景 5：两版交错 —— 排队不写流水、排空在途再换版、至少派一批 ===")
    # First v1 batch = 2 docs * 600ms = ~1.2s: plenty of time to observe
    # the v2 waiter queued and dispatch-less.
    cli.put("/admin/node", {"vram_limit_mb": 4096, "per_doc_delay_ms": 600})
    ts = int(time.time() * 1000)
    job_a = f"JOB-IV-A-{ts}"
    job_b = f"JOB-IV-B-{ts}"
    job_c = f"JOB-IV-C-{ts}"

    code, _ = cli.post("/jobs", {
        "job_id": job_a,
        "model_version": "v1",
        "batch_size": 2,
        "vram_limit_mb": 4096,
        "documents": [short_doc(i) for i in range(4)],
    })
    check(code == 201, f"创建先跑的 v1 作业 {job_a}")

    # Wait until A's first v1 batch is actually dispatched.
    deadline = time.time() + 5
    first_rows: list = []
    while time.time() < deadline:
        first_rows = cli.get(f"/jobs/{job_a}/dispatches")["dispatches"]
        if len(first_rows) >= 2:
            break
        time.sleep(0.03)
    check(len(first_rows) >= 2, "v1 首批 2 篇已实际派发并写入流水")

    code, _ = cli.post("/jobs", {
        "job_id": job_b,
        "model_version": "v2",
        "batch_size": 1,
        "vram_limit_mb": 4096,
        "documents": [short_doc(9)],
    })
    check(code == 201, f"创建请求换版的 v2 作业 {job_b}")

    snap = wait_scheduler_condition(
        cli,
        lambda s: s.get("queued_batches_by_version", {}).get("v2", 0) >= 1,
        timeout=5,
    )
    check(snap["resident_model_version"] == "v1",
          f"v2 排队时节点仍驻留 v1（实际 {snap['resident_model_version']}）")

    # Core requirement: a batch waiting for a switch has NO dispatch rows.
    waiting_b = cli.get(f"/jobs/{job_b}/dispatches")
    check(waiting_b["dispatch_count"] == 0,
          f"等候换版的 v2 批次不写派发流水（实际 {waiting_b['dispatch_count']} 条）")

    # Queue another v1 job behind v2: it must also stay dispatch-less.
    code, _ = cli.post("/jobs", {
        "job_id": job_c,
        "model_version": "v1",
        "batch_size": 1,
        "vram_limit_mb": 4096,
        "documents": [short_doc(3)],
    })
    check(code == 201, f"创建排在 v2 之后的 v1 作业 {job_c}")
    time.sleep(0.1)
    waiting_c = cli.get(f"/jobs/{job_c}/dispatches")
    check(waiting_c["dispatch_count"] == 0,
          "v2 已排队后到达的 v1 新批被暂停接纳，同样不写流水")

    job_a_body = cli.wait_terminal(job_a, timeout=40)
    job_b_body = cli.wait_terminal(job_b, timeout=40)
    job_c_body = cli.wait_terminal(job_c, timeout=40)
    check(job_a_body["counts"]["succeeded"] == 4, "v1 作业 A 4 篇全部成功")
    check(job_b_body["counts"]["succeeded"] == 1, "v2 作业 B 1 篇成功")
    check(job_c_body["counts"]["succeeded"] == 1, "v1 作业 C 1 篇成功")

    rows_a = cli.get(f"/jobs/{job_a}/dispatches")["dispatches"]
    rows_b = cli.get(f"/jobs/{job_b}/dispatches")["dispatches"]
    rows_c = cli.get(f"/jobs/{job_c}/dispatches")["dispatches"]
    check(all(r["model_version"] == "v1" for r in rows_a + rows_c),
          "A/C 流水全部标记 v1")
    check(all(r["model_version"] == "v2" for r in rows_b),
          "B 流水全部标记 v2，且实际派发时才写入")

    # Global monotonic id order proves the version epochs:
    # A first batch (v1,v1) -> B (v2) -> remaining v1.
    wanted = {job_a, job_b, job_c}
    global_rows = [
        r for r in cli.get("/dispatches?limit=5000")["dispatches"]
        if r["job_id"] in wanted
    ]
    global_rows.sort(key=lambda r: r["id"])
    seq_versions = [r["model_version"] for r in global_rows]
    check(seq_versions[:2] == ["v1", "v1"],
          f"全局流水以 v1 首批开头（实际 {seq_versions}）")
    first_v2 = seq_versions.index("v2")
    check(first_v2 == 2, f"v2 在 v1 首批排空后才派发（首个 v2 位置={first_v2}）")
    check(any(v == "v1" for v in seq_versions[first_v2 + 1:]),
          "v2 至少派发一批后又切回 v1 完成排队的 v1 作业")

    snap = cli.get("/admin/scheduler")
    pairs = [(s["from"], s["to"]) for s in snap["recent_switches"]]
    check(("v1", "v2") in pairs and ("v2", "v1") in pairs,
          f"调度器记录了 v1->v2 与 v2->v1 换版：{pairs}")

    # Different simulated vocabulary: same corpus under v1 vs v2 must yield
    # at least one different category.
    code, v1job = cli.post("/jobs", {
        "model_version": "v1", "vram_limit_mb": 4096,
        "documents": [
            {"id": "T1", "text": "量子计算与可控核聚变，算力和大模型智能体"},
            {"id": "T2", "text": "基因编辑靶向药物与免疫细胞疗法"},
            {"id": "T3", "text": "zzz qqq unrelated alpha beta gamma"},
        ],
    })
    check(code == 201, "创建词表对照 v1 作业")
    code, v2job = cli.post("/jobs", {
        "model_version": "v2", "vram_limit_mb": 4096,
        "documents": [
            {"id": "T1", "text": "量子计算与可控核聚变，算力和大模型智能体"},
            {"id": "T2", "text": "基因编辑靶向药物与免疫细胞疗法"},
            {"id": "T3", "text": "zzz qqq unrelated alpha beta gamma"},
        ],
    })
    check(code == 201, "创建词表对照 v2 作业")
    b1 = cli.wait_terminal(v1job["job_id"], timeout=20)
    b2 = cli.wait_terminal(v2job["job_id"], timeout=20)
    m1 = {d["doc_id"]: d["category"] for d in b1["documents"]}
    m2 = {d["doc_id"]: d["category"] for d in b2["documents"]}
    check(any(m1[k] != m2[k] for k in m1),
          f"两版模拟词表使至少一篇分类不同：v1={m1} v2={m2}")
    cli.put("/admin/node", {"vram_limit_mb": 512, "per_doc_delay_ms": 0})


def scenario_version_restart(
    cli: Client,
    restart_fn: Callable[[], None] | None,
) -> None:
    print("\n=== 场景 6：v2 作业 kill -9 / 重启续跑，版本从首批到重启不变 ===")
    if restart_fn is None:
        print(f"{INFO}未提供重启手段，跳过本场景（本地 all 模式会完整执行）")
        return

    cli.put("/admin/node", {"vram_limit_mb": 4096, "per_doc_delay_ms": 150})
    job_id = f"JOB-V2-RESTART-{int(time.time())}"
    code, _ = cli.post("/jobs", {
        "job_id": job_id,
        "model_version": "v2",
        "batch_size": 2,
        "vram_limit_mb": 4096,
        "documents": [short_doc(i) for i in range(8)],
    })
    check(code == 201, f"创建 v2 重启测试作业 {job_id}")

    terminal_before: set[int] = set()
    max_dispatch_id = 0
    deadline = time.time() + 10
    while time.time() < deadline:
        job = cli.get(f"/jobs/{job_id}")
        terminal_before = {
            d["seq"] for d in job["documents"]
            if d["status"] in ("succeeded", "failed")
        }
        rows = cli.get(f"/jobs/{job_id}/dispatches")["dispatches"]
        max_dispatch_id = max((r["id"] for r in rows), default=0)
        if terminal_before and job["counts"]["processing"] > 0:
            break
        time.sleep(0.1)
    check(len(terminal_before) >= 1,
          f"kill 前已有 v2 终态文档 {sorted(terminal_before)}")
    pre_rows = cli.get(f"/jobs/{job_id}/dispatches")["dispatches"]
    check(all(r["model_version"] == "v2" for r in pre_rows),
          "kill 前流水已记录 model_version=v2")

    restart_fn()
    cli.wait_health(timeout=40)

    job_after_boot = cli.get(f"/jobs/{job_id}")
    check(job_after_boot["model_version"] == "v2",
          f"重启后作业仍是 v2（实际 {job_after_boot['model_version']}）")

    barrier = threading.Barrier(4)

    def do_resume(_i):
        barrier.wait()
        return cli.post(f"/jobs/{job_id}/resume")

    with ThreadPoolExecutor(max_workers=4) as pool:
        responses = list(pool.map(do_resume, range(4)))
    started = [b.get("started") for _, b in responses]
    check(sum(1 for s in started if s is True) == 1,
          f"4 个并发 resume 恰好 1 个启动（started 分布={started}）")

    job = cli.wait_terminal(job_id, timeout=60)
    check(job["status"] == "completed",
          f"v2 作业续跑后 completed（实际 {job['status']}）")
    check(job["counts"]["succeeded"] == 8, "8 篇全部成功")

    rows = cli.get(f"/jobs/{job_id}/dispatches")["dispatches"]
    check(all(r["model_version"] == "v2" for r in rows),
          "重启前后所有派发流水均为 model_version=v2")
    new_rows = [r for r in rows if r["id"] > max_dispatch_id]
    leaked = sorted({r["seq"] for r in new_rows} & terminal_before)
    check(not leaked, f"重启前终态文档零重派（泄漏 seq={leaked}）")
    cli.put("/admin/node", {"vram_limit_mb": 512, "per_doc_delay_ms": 0})


# ------------------------------------------------- local uvicorn orchestration


def _free_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


def run_all_local() -> int:
    port = _free_port()
    base_url = f"http://127.0.0.1:{port}"
    tmpdir = tempfile.mkdtemp(prefix="cls-selftest-")
    db_path = str(Path(tmpdir) / "selftest.db")
    log_path = str(Path(tmpdir) / "uvicorn.log")

    env = dict(os.environ)
    env.update({
        "DB_PATH": db_path,
        "VRAM_LIMIT_MB": "512",
        "BATCH_SIZE": "4",
        "NODE_DELAY_MS": "0",
        "PYTHONPATH": str(REPO_ROOT),
    })
    python = sys.executable
    log_files: list = []

    def spawn() -> subprocess.Popen:
        # Intentionally long-lived for the whole server lifetime; the
        # handles are closed in the final cleanup below.
        log = open(log_path, "ab")  # noqa: SIM115
        log_files.append(log)
        return subprocess.Popen(
            [python, "-m", "uvicorn", "app.main:app",
             "--host", "127.0.0.1", "--port", str(port),
             "--log-level", "warning"],
            cwd=str(REPO_ROOT), env=env,
            stdout=log, stderr=subprocess.STDOUT,
            start_new_session=True,
        )

    proc = spawn()
    cli = Client(base_url)
    holder = [proc]

    def hard_restart() -> None:
        current = holder[0]
        print(f"{INFO}SIGKILL uvicorn (pid={current.pid}) 模拟进程崩溃 ...")
        try:
            os.killpg(os.getpgid(current.pid), signal.SIGKILL)
        except (ProcessLookupError, PermissionError):
            current.kill()
        current.wait(timeout=10)
        time.sleep(0.5)
        holder[0] = spawn()

    failures: list[str] = []
    try:
        health = cli.wait_health()
        print(f"{PASS}本地 uvicorn 已启动：{base_url} db={db_path}")
        print(f"{INFO}/health -> {health}")

        for fn in (
            lambda: scenario_shrink(cli),
            lambda: scenario_concurrent_resume(cli),
            lambda: scenario_same_version_parallel(cli),
            lambda: scenario_versions_interleave(cli),
            lambda: scenario_restart_resume(cli, hard_restart),
            lambda: scenario_version_restart(cli, hard_restart),
        ):
            try:
                fn()
            except Exception as exc:  # noqa: BLE001
                failures.append(str(exc))
                print(f"{FAIL} {exc}")
    finally:
        current = holder[0]
        try:
            os.killpg(os.getpgid(current.pid), signal.SIGTERM)
        except (ProcessLookupError, PermissionError):
            current.kill()
        try:
            current.wait(timeout=10)
        except Exception:  # noqa: BLE001
            current.kill()
        for fh in log_files:
            fh.close()

    print("\n================ 自测总结 ================")
    if failures:
        print(f"{FAIL} {len(failures)} 个场景失败：")
        for f in failures:
            print(f" - {f}")
        print(f"{INFO}uvicorn 日志：{log_path}")
        return 1
    print(
        "全部场景通过：缩批重试、单篇失败隔离、并发去重、kill -9 重启续跑、"
        "同版并行、两版交错（排队不入流水/排空换版/至少一批）、版本重启不变。"
    )
    print(f"{INFO}临时数据库保留在 {db_path}（可手动删除 {tmpdir}）")
    return 0


def run_against(base_url: str, restart_cmd: str | None) -> int:
    cli = Client(base_url)
    health = cli.wait_health()
    print(f"{PASS}目标服务健康：{base_url} -> {health}")

    restart_fn: Callable[[], None] | None = None
    if restart_cmd:
        def restart_fn() -> None:  # type: ignore[no-redef]
            print(f"{INFO}执行重启命令：{restart_cmd}")
            rc = subprocess.run(
                restart_cmd, check=False, shell=True, cwd=str(REPO_ROOT)
            ).returncode
            if rc != 0:
                raise AssertFailed(f"重启命令退出码 {rc}")
            time.sleep(1.0)

    failures: list[str] = []
    for fn in (
        lambda: scenario_shrink(cli),
        lambda: scenario_concurrent_resume(cli),
        lambda: scenario_same_version_parallel(cli),
        lambda: scenario_versions_interleave(cli),
        lambda: scenario_restart_resume(cli, restart_fn),
        lambda: scenario_version_restart(cli, restart_fn),
    ):
        try:
            fn()
        except Exception as exc:  # noqa: BLE001
            failures.append(str(exc))
            print(f"{FAIL} {exc}")

    print("\n================ 自测总结 ================")
    if failures:
        print(f"{FAIL} {len(failures)} 个场景失败")
        return 1
    print("针对运行中服务的全部可用场景通过。")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "mode", nargs="?", default="all",
        choices=["all", "remote"],
        help="all=自起 uvicorn 完整自测；remote=配合 --base-url",
    )
    parser.add_argument("--base-url", default=os.environ.get(
        "BASE_URL", "http://localhost:8000"))
    parser.add_argument("--restart-cmd", default=None,
                        help="remote 模式下用于重启服务的 shell 命令")
    args = parser.parse_args()

    if args.mode == "all":
        return run_all_local()
    return run_against(args.base_url, args.restart_cmd)


if __name__ == "__main__":
    sys.exit(main())
