"""Runpod GPU Pod lifecycle (not Serverless).

Three frozen stacks — do not share engine flags:

  llm        vLLM 0.10.2 V0  Qwen/Qwen3-8B-AWQ
  kev        Kev System One  jaredpalmer/kev-8b  (POST /v1/systemone)
  embedding  HuggingFace TEI Qwen/Qwen3-Embedding-0.6B  (/v1/embeddings)

Job ``runpod_gpu_pod`` (tag ``needs_runpod=true``):
  check_running_pods → pick_cheap_gpu → create_pod → wait_vllm_ready

Sensor ``runpod_idle_cleanup``:
  first daemon-process tick kills all tagged pods; later ticks kill only when
  no STARTED/STARTING/QUEUED run with tag needs_runpod=true (dlt/dbt ignored).

Env: RUNPOD_API_KEY, VLLM_API_KEY, VLLM_PORT.
Optional: RUNPOD_MAX_USD_PER_HOUR (0.25), RUNPOD_GPU_WAIT_SECONDS (600),
RUNPOD_GPU_TYPE (pin; skip picker), RUNPOD_IMAGE_NAME (llm only),
RUNPOD_KEV_IMAGE / KEV_MODEL (kev only), RUNPOD_EMBED_IMAGE (embedding only),
RUNPOD_CLOUD_TYPE, RUNPOD_NAME_PREFIX, RUNPOD_CONTAINER_DISK_GB.
"""

import os
import shlex
import time
from typing import Any

import httpx
from dagster import (
    Config,
    DagsterRunStatus,
    DefaultSensorStatus,
    EnvVar,
    Failure,
    OpExecutionContext,
    RetryPolicy,
    RunsFilter,
    SensorEvaluationContext,
    SkipReason,
    definitions,
    job,
    op,
    sensor,
)

_API = "https://rest.runpod.io/v1"
_CATALOG = "https://api.runpod.io/v2/catalog/gpus"
_NO_RETRY = RetryPolicy(max_retries=0)
_BUSY = (
    DagsterRunStatus.STARTED,
    DagsterRunStatus.STARTING,
    DagsterRunStatus.QUEUED,
)
_startup_reap_done = False


_POD_KINDS = ("llm", "embedding", "kev")
# CUDA image; Kev is installed with uv + Python 3.12 (numpy 2.5 needs >=3.12).
_KEV_IMAGE = "runpod/pytorch:2.4.0-py3.11-cuda12.4.1-devel-ubuntu22.04"
_KEV_MODEL = "jaredpalmer/kev-8b"


class GpuPodConfig(Config):
    """Launch with ops.check_running_pods.config.model_kind = llm | embedding | kev."""

    model_kind: str = "llm"


def _required(name: str) -> str:
    value = EnvVar(name).get_value()
    if not value:
        raise Failure(f"{name} is empty")
    return value


def _runpod_configured() -> bool:
    return bool(EnvVar("RUNPOD_API_KEY").get_value())


def _prefix() -> str:
    return os.environ.get("RUNPOD_NAME_PREFIX", "investment-data-products")


def _pod_name(kind: str) -> str:
    return f"{_prefix()}-{kind}"


def _headers() -> dict[str, str]:
    return {"Authorization": f"Bearer {_required('RUNPOD_API_KEY')}"}


_LLM_IMAGE = "vllm/vllm-openai:v0.10.2"
_EMBED_MODEL_ID = "Qwen/Qwen3-Embedding-0.6B"
_TEI_86 = "ghcr.io/huggingface/text-embeddings-inference:86-1.9"
_TEI_80 = "ghcr.io/huggingface/text-embeddings-inference:1.9"
_TEI_89 = "ghcr.io/huggingface/text-embeddings-inference:89-1.9"
_TEI_TURING = "ghcr.io/huggingface/text-embeddings-inference:turing-1.9"
_TEI_HOPPER = "ghcr.io/huggingface/text-embeddings-inference:hopper-1.9"

# AWQ needs SM 7.5+. V100 is SM 7.0 and vLLM rejects it.
_LLM_SKIP_SUBSTR = ("v100", "volta", "p40", "p100", "p4", "k80", "m40")
# TEI has no Volta/Pascal/Blackwell image.
_EMBED_SKIP_SUBSTR = (
    "v100",
    "volta",
    "p40",
    "p100",
    "p4",
    "k80",
    "m40",
    "1080",
    "1070",
    "1060",
    "pascal",
    "5090",
    "blackwell",
    "b200",
)


def _llm_start_cmd() -> list[str]:
    port = _required("VLLM_PORT")
    key = _required("VLLM_API_KEY")
    raw = (
        f"Qwen/Qwen3-8B-AWQ --quantization awq --host 0.0.0.0 --port {port} "
        "--gpu-memory-utilization 0.85 --max-model-len 16384 --max-num-seqs 64 "
        "--enable-chunked-prefill --trust-remote-code "
        "--enable-auto-tool-choice --tool-call-parser hermes "
        "--reasoning-parser qwen3 "
        f"--api-key {key}"
    )
    return shlex.split(raw)


def _kev_start_cmd() -> list[str]:
    port = _required("VLLM_PORT")
    model = os.environ.get("KEV_MODEL") or _KEV_MODEL
    raw = (
        "set -eux; export DEBIAN_FRONTEND=noninteractive; "
        "export PATH=\"/root/.local/bin:$HOME/.local/bin:$PATH\"; "
        "command -v curl >/dev/null || (apt-get update && apt-get install -y --no-install-recommends curl ca-certificates git); "
        "command -v git >/dev/null || (apt-get update && apt-get install -y --no-install-recommends git); "
        "curl -LsSf https://astral.sh/uv/install.sh | sh; "
        "export PATH=\"/root/.local/bin:$HOME/.local/bin:$PATH\"; "
        "git clone --depth 1 https://github.com/jaredpalmer/kev.git /opt/kev; "
        "cd /opt/kev; "
        "uv python install 3.12; "
        "uv sync --extra serve --python 3.12; "
        f"KEV_DTYPE=bf16 uv run --extra serve python -m kev.serve --run {shlex.quote(model)} "
        f"--host 0.0.0.0 --port {shlex.quote(port)} --device cuda"
    )
    return [raw]


def _embed_start_cmd() -> list[str]:
    """Wait for the GPU, then exec TEI. Candle treats a UUID CUDA_VISIBLE_DEVICES
    as CUDA_ERROR_UNKNOWN and falls back to CPU; 16k-token CPU warmup then
    OOM-kills the process in a restart loop."""
    port = _required("VLLM_PORT")
    key = _required("VLLM_API_KEY")
    token = os.environ.get("HF_TOKEN") or os.environ.get("HUGGING_FACE_HUB_TOKEN") or ""
    hf = f" --hf-token {shlex.quote(token)}" if token else ""
    raw = (
        "set -eu; "
        "i=0; "
        "until nvidia-smi >/dev/null 2>&1; do "
        "i=$((i+1)); "
        'if [ "$i" -ge 60 ]; then echo GPU never appeared >&2; nvidia-smi >&2 || true; exit 1; fi; '
        "echo waiting for nvidia-smi; sleep 2; "
        "done; "
        "nvidia-smi; "
        "export CUDA_VISIBLE_DEVICES=0; "
        "exec text-embeddings-router "
        f"--model-id {shlex.quote(_EMBED_MODEL_ID)} "
        "--hostname 0.0.0.0 "
        f"--port {shlex.quote(port)} "
        f"--api-key {shlex.quote(key)} "
        "--max-client-batch-size 32 "
        "--max-batch-tokens 2048 "
        "--dtype float16"
        f"{hf}"
    )
    return [raw]


def _docker_entrypoint(kind: str) -> list[str]:
    if kind in ("embedding", "kev"):
        return ["/bin/bash", "-lc"]
    return ["vllm", "serve"]


def _docker_start_cmd(kind: str) -> list[str]:
    if kind == "embedding":
        return _embed_start_cmd()
    if kind == "kev":
        return _kev_start_cmd()
    return _llm_start_cmd()


def _embed_image(gpu_id: str) -> str:
    pinned = os.environ.get("RUNPOD_EMBED_IMAGE")
    if pinned:
        return pinned
    name = gpu_id.lower()
    if any(part in name for part in ("hopper", "h100", "h200")):
        return _TEI_HOPPER
    if any(part in name for part in ("ada", "4090", "4080", "4070", "4060", "4050", "l40")):
        return _TEI_89
    if name.endswith("l4") or " l4" in name or "-l4" in name or "l4 " in name:
        return _TEI_89
    if any(part in name for part in ("a100", "a30")):
        return _TEI_80
    if any(
        part in name
        for part in ("t4", "turing", "1660", "2060", "2070", "2080", "t1000")
    ):
        return _TEI_TURING
    if "ada" not in name and any(
        part in name
        for part in ("rtx 4000", "rtx 5000", "rtx 6000", "rtx 8000", "quadro rtx")
    ):
        return _TEI_TURING
    return _TEI_86


def _request(method: str, path: str, **kwargs: Any) -> Any:
    with httpx.Client(timeout=60.0) as client:
        response = client.request(method, f"{_API}{path}", headers=_headers(), **kwargs)
        if response.is_error:
            raise httpx.HTTPStatusError(
                f"{response.status_code} {response.text[:500]}",
                request=response.request,
                response=response,
            )
        if response.status_code == 204 or not response.content:
            return {}
        return response.json()


def list_pods() -> list[dict[str, Any]]:
    data = _request("GET", "/pods", params={"includeMachine": True})
    if isinstance(data, list):
        return data
    return data.get("pods") or data.get("data") or []


def pods_for_kind(kind: str) -> list[dict[str, Any]]:
    name = _pod_name(kind)
    return [
        p
        for p in list_pods()
        if p.get("name") == name and p.get("desiredStatus") not in {None, "TERMINATED", "EXITED"}
    ]


def tagged_pods() -> list[dict[str, Any]]:
    prefix = _prefix()
    return [
        p
        for p in list_pods()
        if str(p.get("name") or "").startswith(prefix)
        and p.get("desiredStatus") not in {"TERMINATED"}
    ]


def kill_tagged_pods() -> list[str]:
    stopped: list[str] = []
    for pod in tagged_pods():
        pod_id = pod.get("id")
        if not pod_id:
            continue
        _request("DELETE", f"/pods/{pod_id}")
        stopped.append(str(pod_id))
    return stopped


def _max_usd() -> float:
    return float(os.environ.get("RUNPOD_MAX_USD_PER_HOUR", "0.25"))


def _gpu_wait_seconds() -> float:
    return float(os.environ.get("RUNPOD_GPU_WAIT_SECONDS", "600"))


def _vram_floor_gb(kind: str) -> int:
    return 8 if kind == "embedding" else 16


def _ok_llm_gpu(gpu_id: str) -> bool:
    name = gpu_id.lower()
    return not any(part in name for part in _LLM_SKIP_SUBSTR)


def _ok_embed_gpu(gpu_id: str) -> bool:
    name = gpu_id.lower()
    return not any(part in name for part in _EMBED_SKIP_SUBSTR)


def _catalog(cloud: str) -> list[dict[str, Any]]:
    with httpx.Client(timeout=60.0) as client:
        response = client.get(
            _CATALOG,
            headers=_headers(),
            params={
                "include": "AVAILABILITY",
                "product": "POD",
                "cloud": cloud,
                "gpuCount": 1,
            },
        )
        response.raise_for_status()
        data = response.json()
    return data.get("gpus") or data.get("data") or []


def _cheap_in_stock(kind: str) -> list[tuple[float, str, str, int]]:
    """(usd/hr, gpuTypeId, cloudType, vram_gb) at or under the cap, in stock."""
    cap = _max_usd()
    floor = _vram_floor_gb(kind)
    found: list[tuple[float, str, str, int]] = []
    errors: list[str] = []
    for cloud in ("COMMUNITY", "SECURE"):
        try:
            gpus = _catalog(cloud)
        except Exception as exc:
            errors.append(f"{cloud}: {exc}")
            continue
        for gpu in gpus:
            gpu_id = gpu.get("id")
            mem = int(gpu.get("memory") or 0)
            if not gpu_id or mem < floor:
                continue
            if kind in ("llm", "kev") and not _ok_llm_gpu(str(gpu_id)):
                continue
            if kind == "embedding" and not _ok_embed_gpu(str(gpu_id)):
                continue
            if cloud == "COMMUNITY" and not gpu.get("community"):
                continue
            if cloud == "SECURE" and not gpu.get("secure"):
                continue
            avail = str(gpu.get("availability") or "NONE").upper()
            if avail in {"NONE", ""}:
                continue
            price = (gpu.get("price") or {}).get(cloud.lower())
            if price is None or float(price) > cap:
                continue
            found.append((float(price), str(gpu_id), cloud, mem))
    if not found and len(errors) == 2:
        raise RuntimeError("; ".join(errors))
    found.sort(key=lambda row: (row[0], 0 if row[2] == "COMMUNITY" else 1, -row[3]))
    return found


def choose_gpu(kind: str) -> dict[str, Any]:
    pinned = os.environ.get("RUNPOD_GPU_TYPE")
    if pinned:
        return {
            "gpuTypeId": pinned,
            "cloudType": os.environ.get("RUNPOD_CLOUD_TYPE", "COMMUNITY"),
            "usd_per_hour": None,
            "pinned": True,
            "candidates": [
                {
                    "gpuTypeId": pinned,
                    "cloudType": os.environ.get("RUNPOD_CLOUD_TYPE", "COMMUNITY"),
                    "usd_per_hour": None,
                }
            ],
        }
    deadline = time.time() + _gpu_wait_seconds()
    last_error = "not queried"
    while True:
        try:
            options = _cheap_in_stock(kind)
        except Exception as exc:
            last_error = str(exc)
            options = []
        if options:
            usd, gpu_id, cloud, mem = options[0]
            return {
                "gpuTypeId": gpu_id,
                "cloudType": cloud,
                "usd_per_hour": usd,
                "memory_gb": mem,
                "pinned": False,
                "candidates": [
                    {
                        "gpuTypeId": g,
                        "cloudType": c,
                        "usd_per_hour": p,
                        "memory_gb": m,
                    }
                    for p, g, c, m in options
                ],
            }
        if time.time() >= deadline:
            raise Failure(
                f"no GPU in stock at or under ${_max_usd():.2f}/hr "
                f"(VRAM>={_vram_floor_gb(kind)}GB"
                f"{', SM>=7.5' if kind in ('llm', 'kev') else ', Ampere/Turing TEI (no Volta/Pascal)'}"
                f") after {_gpu_wait_seconds():.0f}s: {last_error}"
            )
        time.sleep(20)


def _pod_record(payload: Any) -> dict[str, Any]:
    if not isinstance(payload, dict):
        return {}
    if payload.get("id"):
        return payload
    for key in ("pod", "data"):
        inner = payload.get(key)
        if isinstance(inner, dict) and inner.get("id"):
            return inner
    return payload


def _mapping_blobs(pod: dict[str, Any]) -> list[Any]:
    runtime = pod.get("runtime") if isinstance(pod.get("runtime"), dict) else {}
    machine = pod.get("machine") if isinstance(pod.get("machine"), dict) else {}
    return [
        pod.get("portMappings"),
        runtime.get("portMappings"),
        machine.get("portMappings"),
        runtime.get("ports"),
        pod.get("ports"),
    ]


def _mapped_public_port(pod: dict[str, Any], port: str) -> str | None:
    want = str(port).split("/")[0]
    for blob in _mapping_blobs(pod):
        if isinstance(blob, dict):
            for key, value in blob.items():
                private = str(key).split("/")[0]
                if private != want or value in (None, ""):
                    continue
                if isinstance(value, dict):
                    public = value.get("publicPort") or value.get("public") or value.get("ip")
                    if public:
                        return str(public).split(":")[-1]
                    continue
                return str(value)
        if isinstance(blob, list):
            for entry in blob:
                if not isinstance(entry, dict):
                    continue
                private = str(entry.get("privatePort") or entry.get("private") or "").split("/")[0]
                if private != want:
                    continue
                public = entry.get("publicPort") or entry.get("public")
                if public:
                    return str(public)
    return None


def _public_ip(pod: dict[str, Any]) -> str:
    nests = [pod]
    for key in ("machine", "runtime"):
        inner = pod.get(key)
        if isinstance(inner, dict):
            nests.append(inner)
    for nest in nests:
        for key in ("publicIp", "publicIP", "ip"):
            value = nest.get(key)
            if isinstance(value, str) and value and value not in {"127.0.0.1", "localhost"}:
                return value
    for blob in _mapping_blobs(pod):
        if not isinstance(blob, list):
            continue
        for entry in blob:
            if not isinstance(entry, dict):
                continue
            ip = entry.get("ip")
            if (
                isinstance(ip, str)
                and ip
                and ip not in {"127.0.0.1", "localhost"}
                and entry.get("isIpPublic") is not False
            ):
                return ip
    return ""


def _health_urls(pod: dict[str, Any], port: str) -> list[str]:
    """Prefer mapped TCP. Proxy is the only path when the host has no public IP."""
    public_ip = _public_ip(pod)
    mapped = _mapped_public_port(pod, port)
    if public_ip and mapped:
        return [f"http://{public_ip}:{mapped}"]
    pod_id = str(pod.get("id") or "")
    if pod_id:
        return [f"https://{pod_id}-{port}.proxy.runpod.net"]
    return []


@op(retry_policy=_NO_RETRY)
def check_running_pods(config: GpuPodConfig) -> str:
    """Fail closed: list errors or an existing pod of this kind abort create."""
    if config.model_kind not in _POD_KINDS:
        raise Failure("model_kind must be llm, embedding, or kev")
    try:
        existing = pods_for_kind(config.model_kind)
    except Exception as exc:
        raise Failure(f"Runpod list failed; not creating {config.model_kind}: {exc}") from exc
    if existing:
        raise Failure(
            f"{_pod_name(config.model_kind)} already running "
            f"({[p.get('id') for p in existing]})"
        )
    return config.model_kind


@op(retry_policy=_NO_RETRY)
def pick_cheap_gpu(context: OpExecutionContext, model_kind: str) -> dict[str, Any]:
    """Cheapest in-stock GPU ≤ cap, or wait up to 10 minutes. No 4090 default."""
    choice = choose_gpu(model_kind)
    choice["model_kind"] = model_kind
    context.log.info(
        "picked %s on %s at $%s/hr (pinned=%s); %s candidates under cap",
        choice["gpuTypeId"],
        choice["cloudType"],
        choice.get("usd_per_hour"),
        choice.get("pinned"),
        len(choice.get("candidates") or []),
    )
    return choice


def _hf_token_env(env: dict[str, str]) -> dict[str, str]:
    token = os.environ.get("HF_TOKEN") or os.environ.get("HUGGING_FACE_HUB_TOKEN")
    if token:
        env["HF_TOKEN"] = token
        env["HUGGING_FACE_HUB_TOKEN"] = token
    return env


def _pod_payload(model_kind: str, gpu_id: str, cloud: str) -> dict[str, Any]:
    if model_kind == "embedding":
        image = _embed_image(gpu_id)
        env = _hf_token_env(
            {
                "VLLM_API_KEY": _required("VLLM_API_KEY"),
                "CUDA_VISIBLE_DEVICES": "0",
                "NVIDIA_DRIVER_CAPABILITIES": "compute,utility",
            }
        )
    elif model_kind == "kev":
        image = os.environ.get("RUNPOD_KEV_IMAGE") or _KEV_IMAGE
        env = _hf_token_env(
            {
                "VLLM_API_KEY": _required("VLLM_API_KEY"),
                "KEV_API_KEY": _required("VLLM_API_KEY"),
                "KEV_MODEL": os.environ.get("KEV_MODEL") or _KEV_MODEL,
                "KEV_DTYPE": "bf16",
            }
        )
    else:
        image = os.environ.get("RUNPOD_IMAGE_NAME", _LLM_IMAGE)
        env = {
            "VLLM_API_KEY": _required("VLLM_API_KEY"),
            "VLLM_WORKER_MULTIPROC_METHOD": "spawn",
            "VLLM_USE_V1": "0",
        }
    return {
        "name": _pod_name(model_kind),
        "imageName": image,
        "computeType": "GPU",
        "gpuTypeIds": [gpu_id],
        "gpuCount": 1,
        "cloudType": cloud,
        "containerDiskInGb": int(os.environ.get("RUNPOD_CONTAINER_DISK_GB", "80")),
        "dockerEntrypoint": _docker_entrypoint(model_kind),
        "dockerStartCmd": _docker_start_cmd(model_kind),
        # tcp → publicIp:mappedPort on hosts that support it. http → proxy on
        # cheap Community boxes with no public IP (typical embedding GPUs).
        "ports": [f"{_required('VLLM_PORT')}/tcp", f"{_required('VLLM_PORT')}/http"],
        "env": env,
    }


def _try_order(choice: dict[str, Any], options: list[tuple[float, str, str, int]] | None = None) -> list[dict[str, Any]]:
    """Cheapest-first unique (gpu, cloud) rows to try one POST each."""
    rows: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    if options:
        source = [
            {"gpuTypeId": g, "cloudType": c, "usd_per_hour": p} for p, g, c, _m in options
        ]
    else:
        source = list(choice.get("candidates") or [])
        if not source:
            source = [
                {
                    "gpuTypeId": choice["gpuTypeId"],
                    "cloudType": choice["cloudType"],
                    "usd_per_hour": choice.get("usd_per_hour"),
                }
            ]
    for row in source:
        key = (row["gpuTypeId"], row["cloudType"])
        if key in seen:
            continue
        seen.add(key)
        rows.append(row)
    return rows


def _create_pod(context: OpExecutionContext, choice: dict[str, Any]) -> dict[str, Any]:
    """Create one Pod. Try each cheap GPU one at a time if create returns 500."""
    model_kind = choice["model_kind"]
    deadline = time.time() + (0 if choice.get("pinned") else _gpu_wait_seconds())
    last_error = "not attempted"
    first_pass = True
    while True:
        options = None
        if not first_pass and not choice.get("pinned"):
            try:
                options = _cheap_in_stock(model_kind)
            except Exception as exc:
                last_error = str(exc)
                options = []
        queue = _try_order(choice, options)
        context.log.info(
            "trying %s GPU(s) in order: %s",
            len(queue),
            [(r["gpuTypeId"], r["cloudType"], r.get("usd_per_hour")) for r in queue],
        )
        for i, row in enumerate(queue, start=1):
            gpu_id = row["gpuTypeId"]
            cloud = row["cloudType"]
            usd = row.get("usd_per_hour")
            payload = _pod_payload(model_kind, gpu_id, cloud)
            context.log.info(
                "%s/%s %s on %s $%s/hr image=%s",
                i,
                len(queue),
                gpu_id,
                cloud,
                usd,
                payload.get("imageName"),
            )
            try:
                created = _request("POST", "/pods", json=payload)
            except Exception as exc:
                last_error = f"{gpu_id} {cloud}: {exc}"
                context.log.warning("unavailable, next GPU: %s", last_error)
                continue
            created["model_kind"] = model_kind
            created["cloudType"] = cloud
            created["gpuTypeId"] = created.get("gpuTypeId") or gpu_id
            created["usd_per_hour"] = usd
            return created
        if choice.get("pinned") or time.time() >= deadline:
            raise Failure(
                f"Runpod create failed for {model_kind} after trying "
                f"{len(queue)} GPU(s) at or under ${_max_usd():.2f}/hr: {last_error}"
            )
        first_pass = False
        time.sleep(20)


@op(retry_policy=_NO_RETRY)
def create_pod(context: OpExecutionContext, choice: dict[str, Any]) -> dict[str, Any]:
    return _create_pod(context, choice)


def _probe_kev(base: str) -> tuple[bool, str]:
    headers = {
        "Authorization": f"Bearer {_required('VLLM_API_KEY')}",
        "Content-Type": "application/json",
    }
    timeout = httpx.Timeout(connect=3.0, read=20.0, write=8.0, pool=8.0)
    url = f"{base}/v1/systemone"
    payload = {
        "state": "ping",
        "model": "kev-latest",
        "questions": {"ping": {"type": "noul", "instructions": "Is this a ping?"}},
    }
    try:
        response = httpx.post(url, headers=headers, json=payload, timeout=timeout)
    except Exception as exc:
        return False, f"{url}: {exc}"
    if response.status_code == 200:
        return True, base
    return False, f"{url}: {response.status_code} {response.text[:200]}"


def _probe_ready(base: str, kind: str = "llm") -> tuple[bool, str]:
    """True if llm /v1/models, TEI /health, or Kev /v1/systemone answers 200."""
    if kind == "kev":
        ready, last = _probe_kev(base)
        if ready:
            return True, base
        health_ok, health_last = _probe_ready(base, "embedding")
        if health_ok:
            return True, base
        return False, f"{last}; {health_last}"
    headers = {"Authorization": f"Bearer {_required('VLLM_API_KEY')}"}
    timeout = httpx.Timeout(connect=3.0, read=8.0, write=8.0, pool=8.0)
    last = "not probed"
    paths = ("/health", "/info") if kind == "embedding" else ("/v1/models", "/health")
    for path in paths:
        url = f"{base}{path}"
        try:
            response = httpx.get(url, headers=headers, timeout=timeout)
        except Exception as exc:
            last = f"{url}: {exc}"
            continue
        if response.status_code != 200:
            last = f"{url}: {response.status_code} {response.text[:200]}"
            continue
        if path == "/v1/models":
            try:
                body = response.json()
            except Exception:
                last = f"{url}: 200 non-json"
                continue
            if isinstance(body, dict) and body.get("data") is not None:
                return True, base
            last = f"{url}: 200 missing data"
            continue
        return True, base
    return False, last


def _graphql_pod(pod_id: str) -> dict[str, Any]:
    query = (
        "query Pod($podId: String!) { pod(input: {podId: $podId}) { id "
        "runtime { ports { ip isIpPublic privatePort publicPort type } } } }"
    )
    with httpx.Client(timeout=60.0) as client:
        response = client.post(
            "https://api.runpod.io/graphql",
            headers=_headers(),
            json={"query": query, "variables": {"podId": pod_id}},
        )
        if response.is_error:
            return {}
        body = response.json()
    if body.get("errors"):
        return {}
    pod = ((body.get("data") or {}).get("pod")) or {}
    return pod if isinstance(pod, dict) else {}


def _refresh_pod(pod: dict[str, Any]) -> dict[str, Any]:
    pod_id = pod.get("id")
    if not pod_id:
        return pod
    fresh = _pod_record(
        _request("GET", f"/pods/{pod_id}", params={"includeMachine": True})
    ) or pod
    port = os.environ.get("VLLM_PORT") or "8000"
    if not _public_ip(fresh) or not _mapped_public_port(fresh, port):
        extra = _graphql_pod(str(pod_id))
        if extra.get("publicIp"):
            fresh["publicIp"] = extra["publicIp"]
        if extra.get("runtime"):
            fresh["runtime"] = extra["runtime"]
    fresh["model_kind"] = pod.get("model_kind", fresh.get("model_kind"))
    return fresh


@op
def wait_vllm_ready(context: OpExecutionContext, pod: dict[str, Any]) -> dict[str, Any]:
    """Poll mapped TCP /v1/models (llm), /v1/systemone (kev), or /health (TEI)."""
    kind = pod.get("model_kind", "llm")
    if pod.get("health_base"):
        ready, _ = _probe_ready(pod["health_base"], kind)
        if ready:
            return pod
    deadline = time.time() + (45 * 60 if kind == "kev" else 30 * 60 if kind == "llm" else 20 * 60)
    port = _required("VLLM_PORT")
    pod_id = pod.get("id")
    last_error = "not polled"
    while time.time() < deadline:
        try:
            fresh = _refresh_pod(pod) if pod_id else pod
            mapped = _mapped_public_port(fresh, port)
            urls = _health_urls(fresh, port)
            if not urls:
                last_error = "no health URL yet (waiting for public IP / mapped port)"
            context.log.info(
                "health probe kind=%s ip=%s mapped=%s urls=%s",
                kind,
                _public_ip(fresh) or "-",
                mapped or "-",
                urls,
            )
            for base in urls:
                ready, last_error = _probe_ready(base, kind)
                if ready:
                    context.log.info("ready at %s", base)
                    pod.update(fresh)
                    pod["health_base"] = base
                    pod["model_kind"] = kind
                    return pod
            context.log.info("waiting for HTTP ready: %s", last_error)
        except Exception as extra:
            last_error = str(extra)
            context.log.info("waiting for HTTP ready: %s", last_error)
        time.sleep(10)
    if pod_id:
        try:
            _request("DELETE", f"/pods/{pod_id}")
            context.log.warning("deleted unready %s %s", kind, pod_id)
        except Exception as extra:
            context.log.warning("failed to delete unready %s: %s", pod_id, extra)
    raise Failure(f"{kind} not ready for {pod_id}: {last_error}")


@job(
    tags={"needs_runpod": "true"},
    description="Start one GPU pod (vLLM llm, Kev, or TEI embedding) and wait until HTTP health is 200.",
)
def runpod_gpu_pod():
    wait_vllm_ready(create_pod(pick_cheap_gpu(check_running_pods())))


def _gpu_jobs_busy(context: SensorEvaluationContext) -> bool:
    runs = context.instance.get_runs(
        filters=RunsFilter(statuses=list(_BUSY), tags={"needs_runpod": "true"}),
        limit=20,
    )
    return any(run.job_name != "runpod_idle_cleanup" for run in runs)


@sensor(
    default_status=DefaultSensorStatus.RUNNING,
    minimum_interval_seconds=60,
    description="Reap Runpod pods on daemon start; later only when no needs_runpod jobs are active.",
)
def runpod_idle_cleanup(context: SensorEvaluationContext):
    global _startup_reap_done
    if not _runpod_configured():
        return SkipReason("RUNPOD_API_KEY unset; skip pod cleanup")
    if not _startup_reap_done:
        ids = kill_tagged_pods()
        _startup_reap_done = True
        return SkipReason(f"daemon start reap: stopped {ids or 'none'}")
    if _gpu_jobs_busy(context):
        return SkipReason("needs_runpod job is active; leaving pods up")
    ids = kill_tagged_pods()
    return SkipReason(f"idle: stopped {ids or 'none'}")


@definitions
def defs():
    from dagster import Definitions

    return Definitions(jobs=[runpod_gpu_pod], sensors=[runpod_idle_cleanup])
