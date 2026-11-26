"""Local simulated classification node.

VRAM cost model (deliberately simple and fully observable)::

    batch_vram_mb = model_resident_mb
                    + sum(estimate_vram_mb(text_i))
    estimate_vram_mb(text) = doc_base_mb
                             + ceil(len(text) * mb_per_100_chars / 100)

The node processes a batch strictly in submission order.  Before processing
each document it checks whether the document fits on top of the model
resident memory and the already-processed documents.  If it does not fit,
the node returns the partial classifications produced so far together with
an ``oom`` descriptor -- exactly the "partial result, then out of memory"
behaviour the retry logic has to handle.

Two model generations share this node.  They use different deterministic
keyword vocabularies (``VOCABULARIES``); callers pass ``model_version`` to
:meth:`SimulatedClassificationNode.classify_batch`.  Residency / switching
policy lives in :class:`app.scheduler.VersionScheduler`; this class only
classifies and records how many batch calls are actually overlapping.
"""

from __future__ import annotations

import asyncio
import hashlib
import math
from dataclasses import dataclass, field
from typing import Any

from .versions import (
    DEFAULT_MODEL_VERSION,
    MODEL_VERSION_V1,
    MODEL_VERSION_V2,
    normalize_model_version,
)

# Tiny deterministic keyword classifier vocabularies.  The point is the
# batching / recovery / version-scheduling machinery, not model quality.
#
# v1 = the original vocabulary (unchanged from the pre-multi-version service).
KEYWORD_CATEGORIES_V1: list[tuple[str, tuple[str, ...]]] = [
    (
        "finance",
        ("股票", "基金", "财报", "银行", "投资", "金融", "经济",
         "stock", "finance", "bank", "earnings"),
    ),
    (
        "technology",
        ("芯片", "算法", "软件", "人工智能", "模型", "代码", "数据",
         "tech", "software", "algorithm", "chip", "ai"),
    ),
    (
        "health",
        ("医学", "健康", "医院", "药物", "疫苗", "疾病",
         "health", "medical", "hospital", "drug"),
    ),
    (
        "sports",
        ("比赛", "球队", "进球", "奥运", "联赛", "体育",
         "sport", "match", "goal", "olympic"),
    ),
    (
        "politics",
        ("政府", "选举", "政策", "总统", "议会", "外交",
         "election", "government", "president", "policy"),
    ),
]

# v2 = the new model's *different* simulated vocabulary.  The five category
# labels stay the same so downstream consumers keep working, but the keyword
# lists are a new-generation lexicon (new domains + some moved/added terms),
# which makes v1 and v2 classify at least some texts differently.
KEYWORD_CATEGORIES_V2: list[tuple[str, tuple[str, ...]]] = [
    (
        "finance",
        ("央行", "货币政策", "流动性", "债券", "汇率", "通胀",
         "私募", "营收", "估值", "供应链金融",
         "portfolio", "bond", "forex", "inflation", "fed"),
    ),
    (
        "technology",
        (
            # New-model lexicon: infra / frontier tech rather than the
            # v1 generic words.
            "量子", "核聚变", "算力", "大模型", "生成式", "智能体",
            "agent", "gpu", "cuda", "向量", "蒸馏", "推理芯片",
            "商业航天", "火箭", "卫星",
            "quantum", "fusion", "llm", "agent", "gpu", "inference",
        ),
    ),
    (
        "health",
        ("基因", "细胞", "靶向药", "免疫", "蛋白", "脑机",
         "临床试验", "公共卫生", "医保",
         "gene", "crispr", "antibody", "biotech", "vaccine"),
    ),
    (
        "sports",
        ("季后赛", "转会", "球员", "教练", "马拉松", "冬奥",
         "世界杯", "青训",
         "playoff", "transfer", "marathon", "world cup", "coach"),
    ),
    (
        "politics",
        ("峰会", "制裁", "地缘", "关税", "北约", "联合国",
         "大使", "条约", "大选",
         "summit", "sanction", "geopolit", "tariff", "nato", "treaty"),
    ),
]

VOCABULARIES: dict[str, list[tuple[str, tuple[str, ...]]]] = {
    MODEL_VERSION_V1: KEYWORD_CATEGORIES_V1,
    MODEL_VERSION_V2: KEYWORD_CATEGORIES_V2,
}

# Backwards-compatible alias: old code/tests import KEYWORD_CATEGORIES.
KEYWORD_CATEGORIES = KEYWORD_CATEGORIES_V1
FALLBACK_CATEGORIES = [c for c, _ in KEYWORD_CATEGORIES_V1]


def estimate_vram_mb(
    text: str, *, doc_base_mb: int, mb_per_100_chars: int
) -> int:
    return doc_base_mb + math.ceil(
        len(text) * mb_per_100_chars / 100.0
    )


def classify_text(
    text: str,
    doc_id: str,
    model_version: str = DEFAULT_MODEL_VERSION,
    version: str | None = None,
) -> tuple[str, float]:
    """Deterministic keyword/hash classification.

    ``model_version`` (or the ``version=`` alias) selects the vocabulary and
    the fallback hash salt, so the new model can return different
    categories/confidences.
    """

    chosen = version if version is not None else model_version
    ver = normalize_model_version(chosen)
    categories = VOCABULARIES[ver]
    fallback = [c for c, _ in categories]
    lowered = text.lower()
    best_category: str | None = None
    best_hits = 0
    for category, keywords in categories:
        hits = sum(1 for kw in keywords if kw.lower() in lowered)
        if hits > best_hits:
            best_hits = hits
            best_category = category
    if best_category is None:
        # Version-specific salt keeps the two models' no-keyword fallback
        # choices different for most ids while remaining deterministic.
        digest = hashlib.sha256(
            f"{ver}:{doc_id}".encode()
        ).digest()
        idx = digest[0] % len(fallback)
        best_category = fallback[idx]
        confidence = 0.70 + (digest[1] % 25) / 100.0
    else:
        digest = hashlib.sha256(
            (ver + doc_id + best_category).encode("utf-8")
        ).digest()
        confidence = 0.82 + (digest[0] % 17) / 100.0  # 0.82 .. 0.98
    return best_category, round(min(confidence, 0.99), 4)


@dataclass
class OOMInfo:
    error_code: str = "node_out_of_memory"
    limit_mb: int = 0
    used_before_mb: int = 0
    required_total_mb: int = 0
    next_seq: int = -1
    next_doc_id: str = ""
    remaining_seqs: list[int] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "error_code": self.error_code,
            "limit_mb": self.limit_mb,
            "used_before_mb": self.used_before_mb,
            "required_total_mb": self.required_total_mb,
            "next_seq": self.next_seq,
            "next_doc_id": self.next_doc_id,
            "remaining_seqs": self.remaining_seqs,
        }


@dataclass
class NodeBatchResult:
    """Return value of one node dispatch.

    ``succeeded`` contains one dict per classified document; ``oom`` is set
    when the node ran out of memory at ``oom.next_seq``.  Documents from
    that seq onwards are absent from ``succeeded``.
    """

    succeeded: list[dict[str, Any]]
    oom: OOMInfo | None = None
    model_version: str = DEFAULT_MODEL_VERSION

    @property
    def oom_occurred(self) -> bool:
        return self.oom is not None


class SimulatedClassificationNode:
    """The fake GPU classification node.

    Memory limit and per-document delay are read from the database-backed
    settings on every dispatch, so ``PUT /admin/node`` adjusts behaviour
    immediately without a restart.

    Concurrency bookkeeping (``active_batches`` / ``peak_active_batches`` and
    per-version counters) is in-process observability used to prove that
    same-version batches really overlap.  It does not gate anything; the
    gating lives in :class:`app.scheduler.VersionScheduler`.
    """

    def __init__(self, db: Any, *, model_resident_mb: int):
        self.db = db
        self.model_resident_mb = model_resident_mb
        self.active_batches: int = 0
        self.peak_active_batches: int = 0
        self.started_batches: int = 0
        self.finished_batches: int = 0
        self.active_by_version: dict[str, int] = {}
        self.peak_by_version: dict[str, int] = {}

    def concurrency_snapshot(self) -> dict[str, Any]:
        return {
            "active_batches": self.active_batches,
            "peak_active_batches": self.peak_active_batches,
            "started_batches": self.started_batches,
            "finished_batches": self.finished_batches,
            "active_by_version": dict(self.active_by_version),
            "peak_by_version": dict(self.peak_by_version),
        }

    async def classify_batch(
        self,
        docs: list[dict[str, Any]],
        vram_limit_mb: int,
        model_version: str = DEFAULT_MODEL_VERSION,
        version: str | None = None,
    ) -> NodeBatchResult:
        chosen = version if version is not None else model_version
        ver = normalize_model_version(chosen)
        settings = self.db.get_node_settings()
        delay_s = max(0, settings["per_doc_delay_ms"]) / 1000.0

        # Observability: count genuinely overlapping node calls.
        self.active_batches += 1
        self.started_batches += 1
        self.peak_active_batches = max(
            self.peak_active_batches, self.active_batches
        )
        self.active_by_version[ver] = (
            self.active_by_version.get(ver, 0) + 1
        )
        self.peak_by_version[ver] = max(
            self.peak_by_version.get(ver, 0),
            self.active_by_version[ver],
        )

        try:
            return await self._classify(
                docs, vram_limit_mb, ver, delay_s
            )
        finally:
            self.active_batches -= 1
            self.finished_batches += 1
            self.active_by_version[ver] -= 1
            if self.active_by_version[ver] <= 0:
                self.active_by_version.pop(ver, None)

    async def _classify(
        self,
        docs: list[dict[str, Any]],
        vram_limit_mb: int,
        version: str,
        delay_s: float,
    ) -> NodeBatchResult:
        used = self.model_resident_mb
        done: list[dict[str, Any]] = []

        for index, doc in enumerate(docs):
            estimate = int(doc["vram_estimate_mb"])
            required_total = used + estimate
            if required_total > vram_limit_mb:
                oom = OOMInfo(
                    limit_mb=vram_limit_mb,
                    used_before_mb=used,
                    required_total_mb=required_total,
                    next_seq=int(doc["seq"]),
                    next_doc_id=str(doc["doc_id"]),
                    remaining_seqs=[int(d["seq"]) for d in docs[index:]],
                )
                return NodeBatchResult(
                    succeeded=done, oom=oom, model_version=version
                )

            # Simulate inference time, but only for documents that actually
            # fit and therefore get processed.
            if delay_s:
                await asyncio.sleep(delay_s)

            category, confidence = classify_text(
                doc["content"], str(doc["doc_id"]), version
            )
            done.append(
                {
                    "seq": int(doc["seq"]),
                    "doc_id": str(doc["doc_id"]),
                    "category": category,
                    "confidence": confidence,
                    "model_version": version,
                }
            )
            used += estimate

        return NodeBatchResult(
            succeeded=done, oom=None, model_version=version
        )
