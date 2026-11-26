# 归档文档分类服务（双版本共节点 · OOM 缩批 · SQLite 持久化 · 重启续跑）

归档前批量给文档分类的单实例 FastAPI 服务。**原版(v1)与新版(v2)两版分类模型共用同一个模拟节点**：提交作业时可选择版本，新版使用不同的模拟词表；节点一次只驻留一版模型，同版作业的批次可并行，另一版有批次排队时排空在途批次后换版。同时保留原有能力：

1. 节点会**先返回已完成的部分结果，再报 OOM**；
2. 服务把**尚未处理完的文档原子地退回 pending，并把批次减半重试**；
3. 缩到单篇仍超显存上限时，**只把该篇记为失败**（含原因），其余文档继续推理；
4. 作业与逐篇结果落 **SQLite**；进程/容器重启后用**原作业号** `POST /jobs/{job_id}/resume` 续跑；
5. 已有 `succeeded` / `failed` 终态的文档**永不重新派发**；SQLite 原子认领保证**并发续跑不会重复派发**；
6. `GET /jobs/{job_id}` 按**提交顺序**列出每篇进度、分类结果或失败原因；
7. 作业的 `model_version` **从首批到 OOM 重试再到重启续跑始终不变**，每条派发流水都记下实际派发时的模型版本。

启动后浏览器打开 **<http://localhost:8000/docs>** 即可完成全部操作（Swagger UI，
`POST /jobs` 请求体里的 `model_version` 下拉可选 `v1`/`v2`）。

---

## 1. 快速开始（Docker Compose）

```bash
cd <本项目目录>
docker compose up --build
```

- API / Swagger：<http://localhost:8000/docs>
- 健康检查：<http://localhost:8000/health>
- SQLite 数据库持久化在宿主机 `./data/classifier.db`（bind mount），
  因此 `docker compose restart` / 关机重开 / `docker compose down && up` 后数据仍在。

> Compose 固定单实例、镜像内固定 `uvicorn --workers 1`（进程内 RunManager 去重）。
> 即使误开多个 resume 请求，甚至多个 worker/进程共用同一个 SQLite 文件，
> 原子认领（`BEGIN IMMEDIATE` + 只选 `pending`）也不会重复派发同一篇文档。

### 在 /docs 上的最短操作路径

1. `POST /demo/run` → **Try it out → Execute**，一键生成 8 篇固定演示文档
   （作业号 `JOB-DEMO-SHRINK`，初始批大小 4，上限 512MB），创建后自动开跑。
2. `GET /jobs/{job_id}`：填入 `JOB-DEMO-SHRINK`，反复 Execute 直到
   `status = completed_with_failures`。可观察：
   - `DEMO-01/02` 在首批成功，`DEMO-03` 触发 OOM；
   - `current_batch_size` 从 `4 → 2 → 1`；
   - `DEMO-05` 单篇仍超限 → `status=failed`，
     `error_code=document_exceeds_vram_limit`，`error_message` 给出显存测算；
   - 其余 7 篇 `succeeded` 并带 `category` / `confidence`。
3. `GET /jobs/{job_id}/dispatches`：逐篇派发流水（同一 `batch_id` 为同批，
   可看到 `DEMO-03`、`DEMO-04`、`DEMO-05` 被缩批后重新派发）。
4. `PUT /admin/node`：运行中调整显存上限（如 `{"vram_limit_mb": 256,
   "per_doc_delay_ms": 80}`），立即生效且持久化到 SQLite；调大到 4096 后
   再 `POST /jobs/JOB-DEMO-SHRINK/resume` 也不会重跑终态文档
   （该 demo 已有 1 篇 failed，调大上限后……注意：failed 是终态，
   **默认续跑不会重试失败篇**，这是需求明确的“有失败记录不再派发”。
   想验证调大上限的效果，请用 `POST /jobs` 提交新作业。）

### 提交自己的编号批次

`POST /jobs`：

```json
{
  "job_id": "JOB-2026-ARCHIVE-0001",
  "model_version": "v2",
  "batch_size": 4,
  "vram_limit_mb": 512,
  "documents": [
    {"id": "DOC-001", "text": "央行货币政策与银行财报……"},
    {"id": "DOC-002", "text": "超长文献正文……"}
  ]
}
```

- `model_version` 可选：`v1`=原版模型/词表（**省略时的默认值**），
  `v2`=新版模型（不同模拟词表）。也接受别名 `original`/`new`、`old`/`new`、
  `原版`/`新版`，请求字段名用 `version` / `model` / `model_name` 也可以。
  返回体同时给出 `model_version`、`version`（同为 `v1`/`v2`）和
  `model_version_label`（`original`/`new`）。
- **升级前已有的 SQLite 作业/流水行**通过幂等 `ALTER TABLE ... DEFAULT 'v1'`
  迁移，全部自动归为原版 `v1`。
- `job_id` 可省略（服务端生成 `JOB-<uuid>`）；自带时必须符合
  `^[A-Za-z0-9_.-]+$`，重复返回 `409` 并提示查询/续跑地址。
- `documents[].id` 是“编号”，同一作业内唯一（重复 → 422）。
- 数组顺序即提交顺序，查询严格按该顺序返回（内部 `seq`）。
- `vram_limit_mb` 可按作业覆盖；不传则用节点全局可调上限。

---

## 2. 重启续跑演练（30 秒）

```bash
# 1) 启动
docker compose up -d --build

# 2) 造一个推理较慢的作业（在 /docs 或 curl 均可）
curl -s -X PUT http://localhost:8000/admin/node \
  -H 'Content-Type: application/json' \
  -d '{"vram_limit_mb":4096,"per_doc_delay_ms":150}'
curl -s -X POST http://localhost:8000/jobs \
  -H 'Content-Type: application/json' \
  -d '{
    "job_id":"JOB-R-1",
    "batch_size":4,
    "vram_limit_mb":4096,
    "documents":[
      {"id":"D1","text":"央行与银行发布财报，金融投资板块上涨。"},
      {"id":"D2","text":"足球队决赛进球大战，体育频道直播收视新高。"},
      {"id":"D3","text":"芯片算法与人工智能模型推动软件行业增长。"},
      {"id":"D4","text":"医院新药物与疫苗临床试验取得进展。"},
      {"id":"D5","text":"政府选举政策与总统外交日程引发议会讨论。"},
      {"id":"D6","text":"新能源工厂投资扩产，财报显示研发投入增加。"}
    ]
  }'

# 3) 跑一半时强杀 / 重启容器
docker compose kill classifier-api        # 模拟崩溃（SIGKILL）
docker compose up -d                      # 或 docker compose restart

# 4) 用原作业号续跑（可连点 / 并发调用，安全）
curl -s -X POST http://localhost:8000/jobs/JOB-R-1/resume

# 5) 按提交顺序查看结果
curl -s http://localhost:8000/jobs/JOB-R-1 | python -m json.tool
```

重启时服务会自动：

- 把上次残留在 `processing` 的文档重置为 `pending`（这些没有结果，允许重派）；
- 把卡在 `running` 的作业标记为 `interrupted`，等待显式 resume；
- **不触碰任何 `succeeded` / `failed` 行**。

并发保证有两层：

1. 进程内 `RunManager`：同一 `job_id` 只允许一个活动的 asyncio 续跑任务，
   并发 resume 中恰好一个返回 `started=true`，其余返回
   `started=false` + “已有续跑任务在执行”；
2. 数据层 `Database.claim_next_batch()`：`BEGIN IMMEDIATE` 事务内
   `SELECT ... WHERE status='pending' ... LIMIT n` 后立刻把这些行改成
   `processing` 并写 `dispatch_log`。即使绕过第一层（多进程/多 worker），
   两个认领事务也会被 SQLite 写锁串行化，**同一篇文档不可能被同时认领两次**。

---

## 3. 状态机与缩批规则

文档状态：`pending → processing → succeeded | failed`
（崩溃后孤儿 `processing → pending`，再走一遍。）

作业状态：`pending → running → completed | completed_with_failures`；
崩溃恢复后可能看到 `interrupted`，续跑后回到 `running`；
未预期的节点异常会落为 `error` 并把本 runner 认领中的文档退回 `pending`，
修正后仍可用原作业号 resume。

缩批循环（`app/runner.py`）：

```
size = job.current_batch_size（初始 batch_size，重启后续用已缩到的值）
loop:
    batch = 原子认领至多 size 篇 pending
    无 -> 收尾
    result = node.classify_batch(batch, limit)
    持久化 result.succeeded（部分结果不丢）
    if result.oom:
        if size == 1:
            该篇 mark_failed(document_exceeds_vram_limit, 测算原因)
        else:
            OOM 未处理完的文档 release 回 pending
            size = max(1, size // 2)，持久化到 jobs.current_batch_size
```

终止性：每次 OOM 必然带来「已有部分成功」「批大小减半（严格变小）」或
「单篇标记失败（少一篇 pending）」三者之一，因此不会死循环；
除被判定为单篇超限的文档外，其余文档都会被推理。

### 模拟显存模型

完全可观测，`GET /admin/node` 也会返回公式：

```
单篇估算 estimate_mb = doc_base_mb(默认48)
                      + ceil(len(text) * mb_per_100_chars(默认4) / 100)
一批占用   batch_mb   = model_resident_mb(默认128) + Σ estimate_mb
```

节点按提交顺序逐篇处理；处理下一篇前若
`used + estimate > vram_limit_mb`，立即返回已完成结果和 OOM 描述
（上限、已用、该篇需要的总量、从哪个 seq 开始未处理、剩余 seq 列表），
未处理篇不耗推理延迟。分类结果是确定性的关键词分类器
（金融/科技/健康/体育/政治，无命中时按编号哈希稳定选类）。

---

## 4. 两版模型共用一个节点（版本选择 · 驻留换版 · 并行规则）

实现：`app/versions.py`（版本常量/别名/词表选择）、
`app/scheduler.py`（`VersionScheduler` 换版状态机）、
`app/node.py`（两套模拟词表 + 节点并发计数）、
`app/runner.py`（每批先过版本闸门再认领/写流水）、
`app/db.py`（`jobs.model_version`、`dispatch_log.model_version` 与迁移）。

### 4.1 规则如何落地

| 需求 | 实现 |
| --- | --- |
| 提交可选原版/新版，新版不同词表 | `POST /jobs` 的 `model_version`（`v1`/`v2`，默认 `v1`）；`node.py` 中 `VOCABULARIES` 给两版各自的关键词表与 fallback 哈希盐 |
| 省略版本用原版 | Pydantic 默认 `v1`，经 `normalize_model_version(None)=='v1'` |
| 升级前已有作业用原版 | 幂等迁移：`jobs` / `dispatch_log` 增加 `model_version TEXT NOT NULL DEFAULT 'v1'`，旧行自动回填 `v1`（有专门的旧 schema 迁移测试） |
| 作业从首批到重启续跑同一版本 | 版本只在创建时写入 `jobs.model_version`；runner 每轮从作业行读取但不改写；重启后同一行仍是原版本 |
| 节点一次只驻留一版 | `VersionScheduler.resident_version` 唯一；状态机只在安全点把驻留值从 `v1` 改成 `v2` 或反向 |
| 同版作业的批次可并行 | 驻留版本且队首连续的同版等待者会被**一次性全部接纳**，各自是独立 asyncio 批次；节点用 `active_batches`/`peak_active_batches` 实测重叠 |
| 另一版有批次排队 → 当前版暂停接新批 | 异版等待者占据队首后，`_pump()` 不再接纳任何当前驻留版的新等待者 |
| 等在途批次全部结束再换版 | 换版条件含 `in_flight[resident] == 0`；每个批次在 `finally` 里 `release()`，所以**报错/取消的批次也会释放**，不会卡住换版 |
| 换入版本至少先派发一批，再响应下一次换版 | 每次换版把 `dispatched_in_current_residency` 清零，只有 `note_dispatched()`（真实非空认领）后才允许再换走；极端空认领场景也有不会死锁的兜底 |
| 等候换版的批次不写派发流水 | runner 先 `await scheduler.admit(version)`，**通过后**才调用 `claim_next_batch()`（该事务才插 `dispatch_log`）；排队中的作业 `GET .../dispatches` 为 0 条 |
| 实际派发时记下模型版本 | `claim_next_batch(..., model_version=...)` 给每条 `dispatch_log` 写 `model_version`；`GET /jobs/{id}/dispatches` 与全局 `GET /dispatches` 都返回它（另附同义 `version` 字段） |

调度器还通过 `GET /admin/scheduler`（以及 `GET /admin/node` 里的
`scheduler` / `node_concurrency` 字段）暴露：驻留版本、在途批次数（按版本）、
排队批次数（按版本）、当前驻留期已派发批数、历史换版记录、峰值并行批次等，
方便在 `/docs` 或 curl 里直接核对。`GET /models` 列出两版词表关键词。

### 4.2 用 curl 观察“排队不写流水 → 排空换版 → 两版交错”

```bash
# 慢一点，制造在途批次（每篇 600ms）
curl -s -X PUT http://localhost:8000/admin/node \
  -H 'Content-Type: application/json' \
  -d '{"vram_limit_mb":4096,"per_doc_delay_ms":600}'

# A：先跑的 v1 作业（batch_size=2，首批约 1.2s）
curl -s -X POST http://localhost:8000/jobs -H 'Content-Type: application/json' -d '{
  "job_id":"IV-A","model_version":"v1","batch_size":2,"vram_limit_mb":4096,
  "documents":[{"id":"a1","text":"银行财报"},{"id":"a2","text":"球队比赛"},
               {"id":"a3","text":"芯片算法"},{"id":"a4","text":"医院药物"}]}'

sleep 0.2
# B：请求换版的 v2 作业 —— 此时 A 的首批仍在途，B 进入排队
curl -s -X POST http://localhost:8000/jobs -H 'Content-Type: application/json' -d '{
  "job_id":"IV-B","model_version":"v2","batch_size":1,"vram_limit_mb":4096,
  "documents":[{"id":"b1","text":"量子计算与可控核聚变"}]}'

# 立刻观察：resident_model_version 仍为 v1，queued_batches_by_version 含 v2
curl -s http://localhost:8000/admin/scheduler | python -m json.tool
# 关键：B 还在等换版，所以流水是 0 条
curl -s http://localhost:8000/jobs/IV-B/dispatches     # dispatch_count = 0

# 全部结束后看全局流水（id 全局递增，即真实派发先后）：
# 版本序列形如 ["v1","v1", "v2", "v1", ...]
curl -s "http://localhost:8000/dispatches?limit=5000" | python -m json.tool
```

`scripts/selftest.py` 的“场景 4/5/6”把上述观察全部自动化（含断言）。

---

## 5. 配置（环境变量）

| 变量 | 默认 | 说明 |
| --- | --- | --- |
| `DB_PATH` | `data/classifier.db` | SQLite 文件路径；Compose 中为 `/app/data/classifier.db` |
| `VRAM_LIMIT_MB` | `512` | 模拟节点初始显存上限；首次启动写入 DB，之后以 `PUT /admin/node` 的持久化值为准 |
| `BATCH_SIZE` | `4` | 新作业默认/示例初始批大小（作业可自带） |
| `NODE_DELAY_MS` | `80` | 每篇实际推理的模拟延迟，方便观察进度与重启演练；运行时可调 |
| `MODEL_RESIDENT_MB` | `128` | 模型常驻显存 |
| `DOC_BASE_MB` | `48` | 每篇基础激活显存 |
| `MB_PER_100_CHARS` | `4` | 每 100 字符增加的显存估算 |

---

## 6. HTTP API 一览

| 方法/路径 | 作用 |
| --- | --- |
| `GET /` | 重定向到 `/docs` |
| `GET /health` | 健康检查；返回本次启动重置的孤儿作业/文档数 |
| `POST /jobs` | 提交带编号文本批次，201 后自动开跑；可带原作业号与 `model_version` |
| `GET /jobs` | 作业列表与计数（含 `model_version`/`version`） |
| `GET /jobs/{job_id}` | **按提交顺序**返回每篇状态/分类/错误，以及作业计数、当前批大小、模型版本、生效显存上限、进程内是否在跑 |
| `POST /jobs/{job_id}/resume` | 按原作业号续跑（沿用作业固定版本）；运行中调用安全去重；全终态时返回提示 |
| `GET /jobs/{job_id}/dispatches` | 该作业派发流水（批次号、当时上限、估算显存、**模型版本**、时间），核对缩批/重复派发/版本 |
| `GET /dispatches` | **全局**派发流水（跨作业、按全局 id 排序，可 `?limit=`），核对两版交错 |
| `GET /admin/scheduler` | 版本调度器快照：驻留版本、在途/排队批次（按版本）、换版记录、峰值并行 |
| `GET /models` | 两版模型与其模拟词表关键词、当前驻留版本 |
| `POST /demo/run` | 一键演示“部分成功→缩批→单篇失败→其余完成”（默认 v1） |
| `GET/PUT /admin/node` | 查看/调整显存上限与推理延迟；GET 还返回驻留版本与调度/并发快照 |

---

## 7. 本地开发与测试（无需 Docker）

需要 Python 3.11：

```bash
python3 -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt
pip install pytest httpx ruff      # 仅自测/lint 需要

uvicorn app.main:app --reload     # http://127.0.0.1:8000/docs
python -m pytest -q               # 37 个单元/接口/并发/重启/多版本测试
ruff check app scripts tests      # 已验证 All checks passed

# 端到端：脚本会自起 uvicorn、跑六大场景、两次 SIGKILL 后重启并并发 resume
# （场景 1 缩批/单篇失败，2 并发去重，3 v1 重启续跑，
#   4 同版并行，5 两版交错/排队不入流水/词表差异，6 v2 重启版本不变）
python scripts/selftest.py all

# 或对已 compose 启动的服务跑 HTTP 场景：
python scripts/selftest.py remote --base-url http://localhost:8000
# 连重启场景也自动化（示例）：
python scripts/selftest.py remote --base-url http://localhost:8000 \
    --restart-cmd "docker compose restart classifier-api"
```

`scripts/selftest.py` 只用标准库，宿主机不装依赖也能对 compose 服务运行。

### 本次改动的实测结果（开发环境）

- `python -m pytest -q`：**37 passed**（原仓库 21 个测试全部保持通过，
  新增 `tests/test_versions.py` 16 个：版本别名/布尔选择器/词表差异/旧库迁移/
  持久化、调度器同版并行、报错批次放行换版、至少一批 dwell、runner 同版重叠、
  排队零流水、两版交错、重启版本不变、HTTP 默认值/别名/422/流水/调度端点）。
- `ruff check app scripts tests`：**All checks passed!**
- `python scripts/selftest.py all`：**六大场景、64 项断言全部 ✅（退出码 0）**，
  包含两次真实 `SIGKILL` uvicorn 后的重启续跑；关键实测片段：
  - 同版(v2)两批在途计数 `2`、节点峰值并发批次 `2`；
  - v2 排队时 `resident_model_version='v1'` 且该作业流水 `dispatch_count=0`；
  - 全局版本序列 `['v1','v1','v2','v1','v1','v1']`，换版记录含
    `v1→v2`、`v2→v1`；
  - v2 作业 kill -9 重启后仍为 `v2`，8 篇全成功，重启前后流水全是 `v2`，
    终态文档零重派。
- 另用真实 uvicorn（非 TestClient）+ curl 验证了 `/docs`(200)、
  `/openapi.json`（`model_version` 枚举 `['v1','v2']`、新端点齐全）、
  `POST /jobs` 与 `/jobs/{id}/dispatches` 的版本字段。
- `docker-compose.yml` 已用 PyYAML 解析校验通过；Dockerfile 的
  `COPY app ./app` 会包含新增的 `versions.py`、`scheduler.py`。

---

## 8. 目录结构

```
app/
  config.py    # 环境变量配置与显存模型常数
  versions.py  # 【新】v1/v2 版本常量、宽松别名解析、版本标签
  scheduler.py # 【新】VersionScheduler：单版本驻留、同版并行、排空换版、至少一批
  db.py        # SQLite schema/迁移、原子认领、终态排除、崩溃重置、派发流水(含 model_version)
  node.py      # 模拟分类节点：两版词表、显存估算、部分结果 + OOM、确定性分类、并发计数
  runner.py    # 版本准入 + 缩批重试循环、单篇失败隔离、RunManager 并发去重
  models.py    # Pydantic schema（/docs 文档，含 model_version 及 version 别名）
  demo.py      # 一键演示数据
  main.py      # FastAPI 路由 / lifespan / 序列化 / 调度与词表观测端点
tests/         # pytest：节点、原子认领与多线程竞争、runner 重启/并发、HTTP API、多版本
scripts/
  selftest.py  # 零三方依赖的端到端自测（6 场景，含两次 SIGKILL 重启 + 并发 resume）
data/          # SQLite bind mount 目录（*.db 已 gitignore）
Dockerfile  docker-compose.yml  requirements.txt
```

## 9. 设计边界 / 余项

- “分类节点”是**本地模拟**（按文本长度的玩具显存模型 + 两版关键词/哈希分类），
  不是真实模型；替换 `app/node.py` 为真实推理客户端即可，编排与持久化不用改。
- **版本驻留/换版协调是进程内状态**（`VersionScheduler`）。Compose 与
  Dockerfile 固定单实例、`uvicorn --workers 1`，因此符合“节点一次只驻留一版”。
  原子 SQLite 认领仍保证多 worker/多进程/共用 db 文件时同一篇不会被重复派发，
  但跨进程的“同一驻留版本/排空换版”不做协调；如需多副本，应把版本闸门也
  下沉到 SQLite/外部锁或让所有推理经过同一个节点进程。
- 失败篇是终态，resume **不会自动重试**它们（符合“已有失败记录不再派发”）；
  如后续需要“调大显存后重试失败篇”，可加显式 `POST /jobs/{id}/retry-failed`
  （把 failed 显式改回 pending 的人工/API 动作），当前未实现。
- 缩批只减不增，逻辑最保守可预期；连续成功后谨慎回升批大小是可选优化。
- 换版本身不模拟额外加载耗时（换版动作在排空后的调度泵里即时完成）；
  “等待/排空”的时间由在途批次的推理延迟真实产生。需要的话可加
  `MODEL_SWITCH_DELAY_MS` 在换版时 sleep。
- 未实现鉴权 / 多租户 / 分页（作业与单作业文档量在归档批处理场景内）；
  `GET /jobs` 默认最近 100 条，`GET /dispatches` 默认最近 500 条（上限 5000）。
- **Docker 实测限制（如实说明）**：当前开发环境没有 `docker`/`podman`
  可执行文件，也没有 `/var/run/docker.sock`，因此 **`docker compose up --build`
  未能在此环境实际执行**。已完成的等价验证：PyYAML 解析校验
  `docker-compose.yml`；用镜像内相同的启动命令
  （`uvicorn app.main:app --host 0.0.0.0 --port 8000 --workers 1`，本地用
  8137 端口）真实起服务并 curl 验证；`scripts/selftest.py all` 自起 uvicorn
  完整跑通含 SIGKILL 重启的六大场景（64 项断言）；Python 3.11 + 锁定版本下
  37 个 pytest 全部通过。首次在有 Docker daemon 的机器上仍建议按第 1、7 节跑一次
  `docker compose up --build` 与 `python scripts/selftest.py remote
  --restart-cmd "docker compose restart classifier-api"`。
