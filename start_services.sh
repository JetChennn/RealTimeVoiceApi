#!/usr/bin/env bash
# Manage the local realtime voice stack.
#
# Usage:
#   ./start_services.sh start   [all|asr|kbservice|thinker|tts|tts-vllm|tts-main|gateway]
#   ./start_services.sh stop    [all|asr|kbservice|thinker|tts|tts-vllm|tts-main|gateway]
#   ./start_services.sh restart [all|asr|kbservice|thinker|tts|tts-vllm|tts-main|gateway]
#   ./start_services.sh status  [all|asr|kbservice|thinker|tts|tts-vllm|tts-main|gateway]
#   ./start_services.sh help
#
# Omitting the target keeps the original all-services behavior.
#
# Deployment defaults mirror the approved deployment:
#   ASR:       GPU 0, Qwen3-ASR-1.7B, port 8001
#   KBService: port 8003
#   Thinker:   port 8002
#   TTS main:  GPUs 1-6, five workers per GPU, port 9000
#   TTS vLLM:  GPU 7, port 9001
#   Gateway:   port 8000
#
# Common overrides:
#   STARTUP_TIMEOUT_SECONDS, STOP_TIMEOUT_SECONDS, INTERNAL_HOST, HEALTH_HOST
#   ASR_MODEL_PATH, ASR_CUDA_VISIBLE_DEVICES, ASR_GPU_MEMORY_UTILIZATION,
#   ASR_MAX_MODEL_LEN, ASR_MAX_NUM_SEQS, ASR_MAX_NUM_BATCHED_TOKENS
#   TTS_MODEL_PATH, TTS_MAIN_GPU_IDS, TTS_MAIN_NUM_WORKERS,
#   TTS_VLLM_CUDA_VISIBLE_DEVICES, TTS_VLLM_GPU_MEMORY_UTILIZATION
#   GATEWAY_MAX_SESSIONS, GATEWAY_CPU_WORKERS, GATEWAY_CPU_PENDING_JOBS

set -Eeuo pipefail

readonly SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
readonly WORKSPACE_DIR="$(cd "$SCRIPT_DIR/.." && pwd -P)"
readonly ASR_DIR="${ASR_DIR:-$WORKSPACE_DIR/Qwen3-ASR}"
readonly KB_DIR="${KB_DIR:-$WORKSPACE_DIR/KBService}"
readonly THINKER_DIR="${THINKER_DIR:-$WORKSPACE_DIR/BerryThinker}"
readonly TTS_DIR="${TTS_DIR:-$WORKSPACE_DIR/PromptTTSD}"
readonly RUN_DIR="${RUN_DIR:-$SCRIPT_DIR/.run}"
readonly LOG_DIR="${LOG_DIR:-$SCRIPT_DIR/logs}"

readonly HEALTH_HOST="${HEALTH_HOST:-127.0.0.1}"
readonly INTERNAL_HOST="${INTERNAL_HOST:-127.0.0.1}"
readonly ASR_PORT="${ASR_PORT:-8001}"
readonly KB_PORT="${KB_PORT:-8003}"
readonly THINKER_PORT="${THINKER_PORT:-8002}"
readonly TTS_MAIN_PORT="${TTS_MAIN_PORT:-9000}"
readonly TTS_VLLM_PORT="${TTS_VLLM_PORT:-9001}"
readonly GATEWAY_PORT="${GATEWAY_PORT:-8000}"

readonly ASR_BIN="${ASR_BIN:-$ASR_DIR/.venv/bin/qwen-asr-serve}"
readonly ASR_MODEL_PATH="${ASR_MODEL_PATH:-$ASR_DIR/models/Qwen3-ASR-1.7B}"
readonly ASR_SERVED_MODEL_NAME="${ASR_SERVED_MODEL_NAME:-Qwen/Qwen3-ASR-1.7B}"
readonly ASR_CUDA_VISIBLE_DEVICES="${ASR_CUDA_VISIBLE_DEVICES:-0}"
readonly ASR_DTYPE="${ASR_DTYPE:-bfloat16}"
readonly ASR_TENSOR_PARALLEL_SIZE="${ASR_TENSOR_PARALLEL_SIZE:-1}"
readonly ASR_GPU_MEMORY_UTILIZATION="${ASR_GPU_MEMORY_UTILIZATION:-0.85}"
readonly ASR_MAX_MODEL_LEN="${ASR_MAX_MODEL_LEN:-4096}"
readonly ASR_MAX_NUM_SEQS="${ASR_MAX_NUM_SEQS:-64}"
readonly ASR_MAX_NUM_BATCHED_TOKENS="${ASR_MAX_NUM_BATCHED_TOKENS:-8192}"
readonly ASR_LIMIT_MM_PER_PROMPT="${ASR_LIMIT_MM_PER_PROMPT:-{\"audio\":1}}"
readonly ASR_OVERRIDE_GENERATION_CONFIG="${ASR_OVERRIDE_GENERATION_CONFIG:-{\"max_new_tokens\":512,\"temperature\":0.0}}"

readonly KB_PYTHON="${KB_PYTHON:-$KB_DIR/.venv/bin/python}"
readonly THINKER_PYTHON="${THINKER_PYTHON:-$THINKER_DIR/.venv/bin/python}"
readonly TTS_PYTHON="${TTS_PYTHON:-$TTS_DIR/.venv/bin/python}"
readonly GATEWAY_PYTHON="${GATEWAY_PYTHON:-$SCRIPT_DIR/.venv/bin/python}"

readonly TTS_MODEL_PATH="${TTS_MODEL_PATH:-$TTS_DIR/pretrained_models/SoulX-Podcast-1.7B}"
readonly TTS_VLLM_CUDA_VISIBLE_DEVICES="${TTS_VLLM_CUDA_VISIBLE_DEVICES:-7}"
readonly TTS_VLLM_GPU_MEMORY_UTILIZATION="${TTS_VLLM_GPU_MEMORY_UTILIZATION:-0.65}"
readonly TTS_VLLM_MAX_MODEL_LEN="${TTS_VLLM_MAX_MODEL_LEN:-8192}"
readonly TTS_VLLM_TENSOR_PARALLEL_SIZE="${TTS_VLLM_TENSOR_PARALLEL_SIZE:-1}"
readonly TTS_MAIN_GPU_IDS="${TTS_MAIN_GPU_IDS:-1,2,3,4,5,6}"
readonly TTS_MAIN_NUM_WORKERS="${TTS_MAIN_NUM_WORKERS:-5}"

readonly GATEWAY_MAX_SESSIONS="${GATEWAY_MAX_SESSIONS:-30}"
readonly GATEWAY_CPU_WORKERS="${GATEWAY_CPU_WORKERS:-8}"
readonly GATEWAY_CPU_PENDING_JOBS="${GATEWAY_CPU_PENDING_JOBS:-256}"
readonly STARTUP_TIMEOUT_SECONDS="${STARTUP_TIMEOUT_SECONDS:-900}"
readonly STOP_TIMEOUT_SECONDS="${STOP_TIMEOUT_SECONDS:-30}"

ASR_CHAT_TEMPLATE="$(cat <<'EOF'
{%- set ns = namespace(system_text="") -%}
{%- for m in messages -%}
  {%- if m.role == 'system' -%}
    {%- if m.content is string -%}
      {%- set ns.system_text = ns.system_text + m.content -%}
    {%- else -%}
      {%- for c in m.content -%}
        {%- if c.type == 'text' and (c.text is defined) -%}
          {%- set ns.system_text = ns.system_text + c.text -%}
        {%- endif -%}
      {%- endfor -%}
    {%- endif -%}
  {%- endif -%}
{%- endfor -%}

{%- set ns2 = namespace(audio_tokens="") -%}
{%- for m in messages -%}
  {%- if m.content is not string -%}
    {%- for c in m.content -%}
      {%- if c.type == 'audio' or ('audio' in c) or ('audio_url' in c) -%}
        {%- set ns2.audio_tokens = ns2.audio_tokens + "<|audio_start|><|audio_pad|><|audio_end|>" -%}
      {%- endif -%}
    {%- endfor -%}
  {%- endif -%}
{%- endfor -%}

{{- '<|im_start|>system\n' + (ns.system_text if ns.system_text is string else '') + '<|im_end|>\n' -}}
{{- '<|im_start|>user\n' + ns2.audio_tokens + '<|im_end|>\n' -}}
{%- if add_generation_prompt -%}
{{- '<|im_start|>assistant\nlanguage Chinese<asr_text>' -}}
{%- endif -%}
EOF
)"
readonly ASR_CHAT_TEMPLATE

declare -a STARTED_SERVICES=()

error() {
    echo "Error: $*" >&2
}

usage() {
    cat <<EOF
Usage:
  $0 start   [all|asr|kbservice|thinker|tts|tts-vllm|tts-main|gateway]
  $0 stop    [all|asr|kbservice|thinker|tts|tts-vllm|tts-main|gateway]
  $0 restart [all|asr|kbservice|thinker|tts|tts-vllm|tts-main|gateway]
  $0 status  [all|asr|kbservice|thinker|tts|tts-vllm|tts-main|gateway]
  $0 help

The target defaults to "all". The "tts" target manages tts-vllm and tts-main.
EOF
}

pid_file() {
    printf '%s/%s.pid\n' "$RUN_DIR" "$1"
}

log_path_file() {
    printf '%s/%s.logpath\n' "$RUN_DIR" "$1"
}

new_log_file() {
    local service="$1"
    local timestamp
    local candidate
    local suffix=0
    timestamp="$(date '+%Y%m%d_%H%M%S_%3N')"
    candidate="$LOG_DIR/${service}_${timestamp}.log"
    while [[ -e "$candidate" ]]; do
        suffix=$((suffix + 1))
        candidate="$LOG_DIR/${service}_${timestamp}_${suffix}.log"
    done
    printf '%s\n' "$candidate"
}

service_marker() {
    case "$1" in
        asr) printf '%s\n' 'qwen-asr-serve' ;;
        kbservice) printf '%s\n' 'scripts/server.py' ;;
        thinker) printf '%s\n' 'apps/thinker_api.py' ;;
        tts-vllm) printf '%s\n' 'llm_service.server:app' ;;
        tts-main) printf '%s\n' 'main_service.server' ;;
        gateway) printf '%s\n' 'realtime_voice.main:app' ;;
        *) return 1 ;;
    esac
}

pid_is_alive() {
    local pid="$1"
    local state
    [[ "$pid" =~ ^[1-9][0-9]*$ ]] || return 1
    kill -0 "$pid" 2>/dev/null || return 1
    if [[ -r "/proc/$pid/stat" ]]; then
        state="$(awk '{print $3}' "/proc/$pid/stat")"
        [[ "$state" != "Z" ]] || return 1
    fi
}

is_running() {
    local service="$1"
    local file
    local pid
    file="$(pid_file "$service")"
    [[ -s "$file" ]] || return 1
    pid="$(<"$file")"
    pid_is_alive "$pid" && process_matches_service "$service" "$pid"
}

process_matches_service() {
    local service="$1"
    local pid="$2"
    local marker
    local command_line
    marker="$(service_marker "$service")" || return 1
    [[ -r "/proc/$pid/cmdline" ]] || return 1
    command_line="$(tr '\0' ' ' < "/proc/$pid/cmdline")"
    [[ "$command_line" == *"$marker"* ]]
}

group_is_running() {
    kill -0 -- "-$1" 2>/dev/null
}

service_health_url() {
    case "$1" in
        asr) printf 'http://%s:%s/health\n' "$HEALTH_HOST" "$ASR_PORT" ;;
        kbservice) printf 'http://%s:%s/health\n' "$HEALTH_HOST" "$KB_PORT" ;;
        thinker) printf 'http://%s:%s/health\n' "$HEALTH_HOST" "$THINKER_PORT" ;;
        tts-vllm) printf 'http://%s:%s/health\n' "$HEALTH_HOST" "$TTS_VLLM_PORT" ;;
        tts-main) printf 'http://%s:%s/health\n' "$HEALTH_HOST" "$TTS_MAIN_PORT" ;;
        gateway) printf 'http://%s:%s/health\n' "$HEALTH_HOST" "$GATEWAY_PORT" ;;
        *) return 1 ;;
    esac
}

json_health_ok() {
    local kind="$1"
    "$GATEWAY_PYTHON" -c '
import json
import sys

kind = sys.argv[1]
try:
    data = json.load(sys.stdin)
except Exception:
    raise SystemExit(1)

if kind == "kbservice":
    ok = data.get("status") == "ok"
elif kind == "thinker":
    ok = data.get("ok") is True
elif kind == "tts-vllm":
    ok = data.get("status") == "healthy"
elif kind == "tts-main":
    workers = data.get("workers") or []
    expected_gpus = [item.strip() for item in sys.argv[2].split(",") if item.strip()]
    actual_gpus = [str(item) for item in (data.get("gpu_ids") or [])]
    expected_workers = len(expected_gpus) * int(sys.argv[3])
    ok = (
        data.get("status") == "healthy"
        and workers
        and all(worker.get("alive") is True for worker in workers)
        and sorted(actual_gpus) == sorted(expected_gpus)
        and data.get("num_workers") == expected_workers
    )
elif kind == "gateway":
    ok = data.get("ready") is True
else:
    ok = False
raise SystemExit(0 if ok else 1)
' "$kind" "$TTS_MAIN_GPU_IDS" "$TTS_MAIN_NUM_WORKERS"
}

is_service_ready() {
    local service="$1"
    local url
    local body
    url="$(service_health_url "$service")"
    if [[ "$service" == "asr" ]]; then
        curl --noproxy '*' --fail --silent --max-time 3 "$url" >/dev/null
        return
    fi
    body="$(curl --noproxy '*' --fail --silent --max-time 3 "$url")" || return 1
    printf '%s' "$body" | json_health_ok "$service"
}

load_gateway_config() {
    local env_file="$SCRIPT_DIR/.env"
    if [[ ! -f "$env_file" ]]; then
        env_file="$SCRIPT_DIR/.env.example"
    fi
    [[ -f "$env_file" ]] || { error "missing gateway environment file: $env_file"; return 1; }

    # shellcheck disable=SC1090
    source "$env_file"

    [[ "${RTVA_PORT:-}" == "$GATEWAY_PORT" ]] || { error "RTVA_PORT must be $GATEWAY_PORT"; return 1; }
    [[ "${RTVA_ASR_BASE_URL:-}" == "http://127.0.0.1:$ASR_PORT" ]] || { error "RTVA_ASR_BASE_URL must point to port $ASR_PORT"; return 1; }
    [[ "${RTVA_THINKER_BASE_URL:-}" == "http://127.0.0.1:$THINKER_PORT" ]] || { error "RTVA_THINKER_BASE_URL must point to port $THINKER_PORT"; return 1; }
    [[ "${RTVA_TTS_BASE_URL:-}" == "http://127.0.0.1:$TTS_MAIN_PORT" ]] || { error "RTVA_TTS_BASE_URL must point to port $TTS_MAIN_PORT"; return 1; }

    RTVA_HOST="${RTVA_HOST:-0.0.0.0}"
    RTVA_KEEP_ALIVE_SECONDS="${RTVA_KEEP_ALIVE_SECONDS:-10}"
    [[ "$RTVA_KEEP_ALIVE_SECONDS" =~ ^[1-9][0-9]*$ ]] || { error "RTVA_KEEP_ALIVE_SECONDS must be a positive integer"; return 1; }
}

validate_common_runtime() {
    command -v curl >/dev/null 2>&1 || { error "curl is required"; return 1; }
    command -v setsid >/dev/null 2>&1 || { error "setsid is required"; return 1; }
    [[ -x "$GATEWAY_PYTHON" ]] || { error "gateway Python not found: $GATEWAY_PYTHON"; return 1; }
    mkdir -p "$RUN_DIR" "$LOG_DIR"
}

validate_service_runtime() {
    local service="$1"
    validate_common_runtime || return 1
    case "$service" in
        asr)
            [[ -x "$ASR_BIN" ]] || { error "ASR launcher not found: $ASR_BIN"; return 1; }
            [[ -d "$ASR_MODEL_PATH" ]] || { error "ASR 1.7B model directory not found: $ASR_MODEL_PATH"; return 1; }
            ;;
        kbservice)
            [[ -x "$KB_PYTHON" ]] || { error "KBService Python not found: $KB_PYTHON"; return 1; }
            [[ -f "$KB_DIR/scripts/server.py" ]] || { error "KBService entry point not found"; return 1; }
            ;;
        thinker)
            [[ -x "$THINKER_PYTHON" ]] || { error "Thinker Python not found: $THINKER_PYTHON"; return 1; }
            [[ -f "$THINKER_DIR/apps/thinker_api.py" ]] || { error "Thinker entry point not found"; return 1; }
            ;;
        tts-vllm)
            [[ -x "$TTS_PYTHON" ]] || { error "TTS Python not found: $TTS_PYTHON"; return 1; }
            [[ -d "$TTS_MODEL_PATH" ]] || { error "TTS model directory not found: $TTS_MODEL_PATH"; return 1; }
            [[ -f "$TTS_DIR/llm_service/server.py" ]] || { error "TTS vLLM entry point not found"; return 1; }
            ;;
        tts-main)
            [[ -x "$TTS_PYTHON" ]] || { error "TTS Python not found: $TTS_PYTHON"; return 1; }
            [[ -d "$TTS_MODEL_PATH" ]] || { error "TTS model directory not found: $TTS_MODEL_PATH"; return 1; }
            [[ -f "$TTS_DIR/main_service/server.py" ]] || { error "TTS main entry point not found"; return 1; }
            ;;
        gateway)
            [[ -f "$SCRIPT_DIR/src/realtime_voice/main.py" ]] || { error "gateway entry point not found"; return 1; }
            load_gateway_config || return 1
            ;;
        *)
            error "unknown service: $service"
            return 1
            ;;
    esac
}

ensure_turn_end_model() {
    local enabled="${RTVA_TURN_END_SEMANTIC_ENABLED:-true}"
    local configured_path="${RTVA_TURN_END_MODEL_PATH:-models/turn-end-livekit}"
    local model_path
    enabled="${enabled,,}"
    case "$enabled" in
        false|0|no|off)
            echo "Semantic turn-end detection is disabled; skipping model check."
            return
            ;;
        true|1|yes|on) ;;
        *) error "RTVA_TURN_END_SEMANTIC_ENABLED must be a boolean value"; return 1 ;;
    esac
    if [[ "$configured_path" == /* ]]; then
        model_path="$configured_path"
    else
        model_path="$SCRIPT_DIR/$configured_path"
    fi
    if "$GATEWAY_PYTHON" "$SCRIPT_DIR/scripts/download_turn_end_model.py" --output "$model_path" --check >/dev/null 2>&1; then
        echo "Semantic turn-end model is ready: $model_path"
        return
    fi
    echo "Semantic turn-end model is missing or invalid; downloading pinned assets."
    "$GATEWAY_PYTHON" "$SCRIPT_DIR/scripts/download_turn_end_model.py" --output "$model_path"
    "$GATEWAY_PYTHON" "$SCRIPT_DIR/scripts/download_turn_end_model.py" --output "$model_path" --check
}

ensure_speaker_model() {
    local enabled="${RTVA_SPEAKER_VERIFICATION_ENABLED:-false}"
    local configured_path="${RTVA_SPEAKER_MODEL_PATH:-models/wespeaker/cnceleb_resnet34.onnx}"
    local model_path
    enabled="${enabled,,}"
    case "$enabled" in
        false|0|no|off) return ;;
        true|1|yes|on) ;;
        *) error "RTVA_SPEAKER_VERIFICATION_ENABLED must be a boolean value"; return 1 ;;
    esac
    if [[ "$configured_path" == /* ]]; then
        model_path="$configured_path"
    else
        model_path="$SCRIPT_DIR/$configured_path"
    fi
    [[ -f "$model_path" ]] || { error "WeSpeaker ONNX model not found: $model_path"; return 1; }
    echo "WeSpeaker model is ready: $model_path"
}

start_process() {
    local service="$1"
    shift
    local pid_path
    local output
    local pid
    pid_path="$(pid_file "$service")"
    output="$(new_log_file "$service")"

    if is_running "$service"; then
        echo "$service is already starting or running (PID $(<"$pid_path"))."
        return
    fi
    rm -f "$pid_path" "$(log_path_file "$service")"
    : > "$output"

    setsid --fork bash -c '
        pid_path="$1"
        shift
        printf "%s\n" "$$" > "$pid_path"
        exec "$@"
    ' start_services.sh "$pid_path" "$@" > "$output" 2>&1 < /dev/null &

    for _ in {1..100}; do
        if [[ -s "$pid_path" ]]; then
            pid="$(<"$pid_path")"
            if pid_is_alive "$pid"; then
                break
            fi
        fi
        sleep 0.1
    done
    [[ -s "$pid_path" ]] || { error "$service did not create a PID file; see $output"; return 1; }
    pid="$(<"$pid_path")"
    pid_is_alive "$pid" || { error "$service exited immediately; see $output"; return 1; }
    printf '%s\n' "$output" > "$(log_path_file "$service")"
    STARTED_SERVICES+=("$service")
    echo "Started $service (PID $pid); log: $output"
}

start_process_in_dir() {
    local service="$1"
    local workdir="$2"
    shift 2
    start_process "$service" bash -c '
        set -Eeuo pipefail
        workdir="$1"
        shift
        cd "$workdir"
        exec "$@"
    ' start_service "$workdir" "$@"
}

launch_service() {
    local service="$1"
    local tts_vllm_log_level="${TTS_VLLM_LOG_LEVEL:-INFO}"
    validate_service_runtime "$service" || return 1
    if is_service_ready "$service"; then
        if is_running "$service"; then
            echo "$service is already ready (PID $(<"$(pid_file "$service")"))."
        else
            echo "$service is already ready (externally managed); reusing it."
        fi
        return
    fi
    if is_running "$service"; then
        echo "$service is already starting (PID $(<"$(pid_file "$service")"))."
        return
    fi

    case "$service" in
        asr)
            start_process_in_dir asr "$ASR_DIR" \
                env \
                    CUDA_VISIBLE_DEVICES="$ASR_CUDA_VISIBLE_DEVICES" \
                    PYTHONPATH="$ASR_DIR${PYTHONPATH:+:$PYTHONPATH}" \
                "$ASR_BIN" "$ASR_MODEL_PATH" \
                    --served-model-name "$ASR_SERVED_MODEL_NAME" \
                    --host "$INTERNAL_HOST" \
                    --port "$ASR_PORT" \
                    --tensor-parallel-size "$ASR_TENSOR_PARALLEL_SIZE" \
                    --dtype "$ASR_DTYPE" \
                    --gpu-memory-utilization "$ASR_GPU_MEMORY_UTILIZATION" \
                    --max-model-len "$ASR_MAX_MODEL_LEN" \
                    --max-num-seqs "$ASR_MAX_NUM_SEQS" \
                    --max-num-batched-tokens "$ASR_MAX_NUM_BATCHED_TOKENS" \
                    --limit-mm-per-prompt "$ASR_LIMIT_MM_PER_PROMPT" \
                    --generation-config vllm \
                    --override-generation-config "$ASR_OVERRIDE_GENERATION_CONFIG" \
                    --chat-template "$ASR_CHAT_TEMPLATE"
            ;;
        kbservice)
            start_process_in_dir kbservice "$KB_DIR" \
                env HOST=0.0.0.0 PORT="$KB_PORT" \
                "$KB_PYTHON" scripts/server.py
            ;;
        thinker)
            start_process thinker bash -c '
                set -Eeuo pipefail
                thinker_dir="$1"
                thinker_python="$2"
                thinker_host="$3"
                thinker_port="$4"
                thinker_cuda_devices="$5"
                cd "$thinker_dir"
                if [[ -f .env.local ]]; then
                    set -a
                    source .env.local
                    set +a
                fi
                export MIO_API_PORT="$thinker_port"
                export PYTHONPATH="$thinker_dir${PYTHONPATH:+:$PYTHONPATH}"
                if [[ -n "$thinker_cuda_devices" ]]; then
                    export CUDA_VISIBLE_DEVICES="$thinker_cuda_devices"
                fi
                unset ALL_PROXY all_proxy
                exec "$thinker_python" apps/thinker_api.py \
                    --host "$thinker_host" \
                    --port "$thinker_port" \
                    --timeout-keep-alive "${MIO_API_KEEP_ALIVE_SECONDS:-15}"
            ' start_thinker "$THINKER_DIR" "$THINKER_PYTHON" "$INTERNAL_HOST" "$THINKER_PORT" "${THINKER_CUDA_VISIBLE_DEVICES:-}"
            ;;
        tts-vllm)
            start_process_in_dir tts-vllm "$TTS_DIR" \
                env \
                    VLLM_USE_V1=1 \
                    MODEL_PATH="$TTS_MODEL_PATH" \
                    GPU_MEMORY_UTILIZATION="$TTS_VLLM_GPU_MEMORY_UTILIZATION" \
                    MAX_MODEL_LEN="$TTS_VLLM_MAX_MODEL_LEN" \
                    TENSOR_PARALLEL_SIZE="$TTS_VLLM_TENSOR_PARALLEL_SIZE" \
                    LLM_SERVICE_HOST="$INTERNAL_HOST" \
                    LLM_SERVICE_PORT="$TTS_VLLM_PORT" \
                    LOG_LEVEL="$tts_vllm_log_level" \
                    WARMUP_ENABLED="${TTS_VLLM_WARMUP_ENABLED:-true}" \
                    RAS_ENABLED="${TTS_VLLM_RAS_ENABLED:-true}" \
                    RAS_WIN_SIZE="${TTS_VLLM_RAS_WIN_SIZE:-25}" \
                    RAS_TAU_R="${TTS_VLLM_RAS_TAU_R:-0.2}" \
                    CUDA_VISIBLE_DEVICES="$TTS_VLLM_CUDA_VISIBLE_DEVICES" \
                "$TTS_PYTHON" -m uvicorn llm_service.server:app \
                    --host "$INTERNAL_HOST" \
                    --port "$TTS_VLLM_PORT" \
                    --log-level "${tts_vllm_log_level,,}" \
                    --no-access-log
            ;;
        tts-main)
            start_process_in_dir tts-main "$TTS_DIR" \
                env \
                    -u CUDA_VISIBLE_DEVICES \
                    -u ALL_PROXY \
                    -u all_proxy \
                    -u HTTP_PROXY \
                    -u http_proxy \
                    -u HTTPS_PROXY \
                    -u https_proxy \
                    PYTHONUNBUFFERED=1 \
                    NO_PROXY=127.0.0.1,localhost \
                    no_proxy=127.0.0.1,localhost \
                    MODEL_PATH="$TTS_MODEL_PATH" \
                    FP16_FLOW="${TTS_MAIN_FP16_FLOW:-false}" \
                    FLOW_N_TIMESTEPS="${TTS_MAIN_FLOW_N_TIMESTEPS:-10}" \
                    GPU_IDS="$TTS_MAIN_GPU_IDS" \
                    NUM_WORKERS="$TTS_MAIN_NUM_WORKERS" \
                    LLM_SERVICE_URL="http://$HEALTH_HOST:$TTS_VLLM_PORT" \
                    API_HOST="$INTERNAL_HOST" \
                    API_PORT="$TTS_MAIN_PORT" \
                    REFERENCE_CACHE_CAPACITY="${TTS_REFERENCE_CACHE_CAPACITY:-10}" \
                    REFERENCE_WARMUP_ENABLED="${TTS_REFERENCE_WARMUP_ENABLED:-true}" \
                    WORKER_STARTUP_TIMEOUT="${TTS_WORKER_STARTUP_TIMEOUT:-300}" \
                    REQUEST_RESULT_TIMEOUT="${TTS_REQUEST_RESULT_TIMEOUT:-300}" \
                    WORKER_RESTART_ENABLED="${TTS_WORKER_RESTART_ENABLED:-true}" \
                    WORKER_RESTART_MAX="${TTS_WORKER_RESTART_MAX:-3}" \
                    SOULX_STREAM_TOKEN_HOP_LEN="${SOULX_STREAM_TOKEN_HOP_LEN:-25}" \
                    SOULX_STREAM_TOKEN_MAX_HOP_LEN="${SOULX_STREAM_TOKEN_MAX_HOP_LEN:-100}" \
                    SOULX_STREAM_SCALE_FACTOR="${SOULX_STREAM_SCALE_FACTOR:-2}" \
                    SOULX_STREAM_MEL_CACHE_LEN="${SOULX_STREAM_MEL_CACHE_LEN:-8}" \
                    FLOW_TRT_ENABLED="${TTS_FLOW_TRT_ENABLED:-true}" \
                "$TTS_PYTHON" -u -m main_service.server
            ;;
        gateway)
            start_process_in_dir gateway "$SCRIPT_DIR" \
                env \
                    RTVA_MAX_SESSIONS="$GATEWAY_MAX_SESSIONS" \
                    RTVA_CPU_WORKERS="$GATEWAY_CPU_WORKERS" \
                    RTVA_CPU_PENDING_JOBS="$GATEWAY_CPU_PENDING_JOBS" \
                    RTVA_ASR_CONCURRENCY="$ASR_MAX_NUM_SEQS" \
                    RTVA_ASR_MAX_WAITERS="$GATEWAY_MAX_SESSIONS" \
                "$GATEWAY_PYTHON" -m uvicorn realtime_voice.main:app \
                    --app-dir src \
                    --host "$RTVA_HOST" \
                    --port "$RTVA_PORT" \
                    --timeout-keep-alive "$RTVA_KEEP_ALIVE_SECONDS"
            ;;
    esac
}

wait_for_service() {
    local service="$1"
    local deadline=$((SECONDS + STARTUP_TIMEOUT_SECONDS))
    local output=""
    while (( SECONDS < deadline )); do
        if is_service_ready "$service"; then
            echo "$service is ready: $(service_health_url "$service")"
            return
        fi
        if [[ -f "$(pid_file "$service")" ]] && ! is_running "$service"; then
            if [[ -f "$(log_path_file "$service")" ]]; then
                output="$(<"$(log_path_file "$service")")"
                echo "--- Last 80 lines of $output ---" >&2
                tail -n 80 "$output" >&2 || true
            fi
            error "$service exited before becoming ready"
            return 1
        fi
        sleep 2
    done
    error "$service was not ready within ${STARTUP_TIMEOUT_SECONDS}s: $(service_health_url "$service")"
    return 1
}

require_gateway_dependencies() {
    local failed=0
    local service
    for service in asr thinker tts-vllm tts-main; do
        if ! is_service_ready "$service"; then
            error "gateway dependency is not healthy: $service ($(service_health_url "$service"))"
            failed=1
        fi
    done
    (( failed == 0 ))
}

array_contains() {
    local wanted="$1"
    local item
    for item in "${STARTED_SERVICES[@]}"; do
        [[ "$item" == "$wanted" ]] && return 0
    done
    return 1
}

stop_service() {
    local service="$1"
    local pid_path
    local pid
    local deadline
    pid_path="$(pid_file "$service")"
    if [[ ! -s "$pid_path" ]]; then
        echo "$service is not managed by this script; nothing to stop."
        return
    fi
    pid="$(<"$pid_path")"
    if ! pid_is_alive "$pid"; then
        echo "$service has a stale PID file; cleaning it up."
        rm -f "$pid_path" "$(log_path_file "$service")"
        return
    fi
    if ! process_matches_service "$service" "$pid"; then
        echo "$service has a stale PID file (PID $pid belongs to another process); cleaning it up."
        rm -f "$pid_path" "$(log_path_file "$service")"
        return
    fi

    echo "Stopping $service (process group $pid)"
    kill -TERM -- "-$pid" 2>/dev/null || kill -TERM "$pid" 2>/dev/null || true
    deadline=$((SECONDS + STOP_TIMEOUT_SECONDS))
    while (( SECONDS < deadline )); do
        group_is_running "$pid" || break
        sleep 1
    done
    if group_is_running "$pid"; then
        echo "Force-stopping $service (process group $pid)" >&2
        kill -KILL -- "-$pid" 2>/dev/null || kill -KILL "$pid" 2>/dev/null || true
    fi
    rm -f "$pid_path" "$(log_path_file "$service")"
}

cleanup_failed_start() {
    local status=$?
    cleanup_started_services "$status" "Startup failed"
}

cleanup_interrupted_start() {
    local signal="$1"
    local status=1
    case "$signal" in
        INT) status=130 ;;
        TERM) status=143 ;;
    esac
    cleanup_started_services "$status" "Startup interrupted by SIG$signal"
}

cleanup_started_services() {
    local status="$1"
    local reason="$2"
    local index
    trap - ERR INT TERM
    echo "$reason; stopping processes started by this command." >&2
    for (( index=${#STARTED_SERVICES[@]} - 1; index >= 0; index-- )); do
        stop_service "${STARTED_SERVICES[index]}" || true
    done
    exit "$status"
}

install_startup_traps() {
    trap cleanup_failed_start ERR
    trap 'cleanup_interrupted_start INT' INT
    trap 'cleanup_interrupted_start TERM' TERM
}

clear_startup_traps() {
    trap - ERR INT TERM
}

start_single() {
    local service="$1"
    case "$service" in
        tts-main)
            is_service_ready tts-vllm || { error "tts-main requires a healthy tts-vllm service on port $TTS_VLLM_PORT"; return 1; }
            ;;
        gateway)
            validate_service_runtime gateway || return 1
            ensure_turn_end_model || return 1
            ensure_speaker_model || return 1
            require_gateway_dependencies || return 1
            ;;
    esac
    launch_service "$service" || return 1
    wait_for_service "$service"
}

start_tts() {
    launch_service tts-vllm || return 1
    wait_for_service tts-vllm || return 1
    launch_service tts-main || return 1
    wait_for_service tts-main
}

start_all() {
    local kb_degraded=0
    validate_common_runtime || return 1
    load_gateway_config || return 1
    ensure_turn_end_model || return 1
    ensure_speaker_model || return 1

    install_startup_traps

    launch_service asr
    if ! launch_service kbservice; then
        echo "Warning: KBService could not be launched; continuing without it." >&2
        kb_degraded=1
    fi
    launch_service thinker
    launch_service tts-vllm

    wait_for_service asr
    wait_for_service thinker
    wait_for_service tts-vllm
    if (( kb_degraded == 0 )) && ! wait_for_service kbservice; then
        echo "Warning: KBService did not become healthy; continuing without it." >&2
        kb_degraded=1
        if array_contains kbservice; then
            stop_service kbservice || true
        fi
    fi

    launch_service tts-main
    wait_for_service tts-main
    require_gateway_dependencies
    launch_service gateway
    wait_for_service gateway

    clear_startup_traps
    if (( kb_degraded != 0 )); then
        echo "Core voice stack is ready, but KBService is degraded." >&2
        return 2
    fi
    echo "All services are ready. Gateway: http://$HEALTH_HOST:$GATEWAY_PORT/health"
}

status_service() {
    local service="$1"
    local pid_path
    local details=""
    pid_path="$(pid_file "$service")"
    if is_service_ready "$service"; then
        if is_running "$service"; then
            details="running, healthy, PID $(<"$pid_path")"
        else
            details="running, healthy, externally managed"
        fi
    elif is_running "$service"; then
        details="running, not ready, PID $(<"$pid_path")"
    else
        details="stopped or unreachable"
    fi
    if [[ -f "$(log_path_file "$service")" ]]; then
        details="$details, log $(<"$(log_path_file "$service")")"
    fi
    printf '%-12s %s\n' "$service:" "$details"
}

status_target() {
    case "$1" in
        all)
            local service
            for service in asr kbservice thinker tts-vllm tts-main gateway; do
                status_service "$service"
            done
            ;;
        tts)
            status_service tts-vllm
            status_service tts-main
            ;;
        *) status_service "$1" ;;
    esac
}

stop_target() {
    case "$1" in
        all)
            local service
            local failed=0
            for service in gateway tts-main tts-vllm thinker kbservice asr; do
                if ! stop_service "$service"; then
                    failed=1
                fi
            done
            return "$failed"
            ;;
        tts)
            local failed=0
            if ! stop_service tts-main; then
                failed=1
            fi
            if ! stop_service tts-vllm; then
                failed=1
            fi
            return "$failed"
            ;;
        *) stop_service "$1" ;;
    esac
}

start_target() {
    case "$1" in
        all) start_all ;;
        tts) start_tts ;;
        *) start_single "$1" ;;
    esac
}

validate_target() {
    case "$1" in
        all|asr|kbservice|thinker|tts|tts-vllm|tts-main|gateway) return ;;
        *) error "unknown target: $1"; usage >&2; return 1 ;;
    esac
}

main() {
    local action="${1:-start}"
    local target="${2:-all}"
    (( $# <= 2 )) || { usage >&2; return 1; }

    case "$action" in
        help|-h|--help)
            usage
            return
            ;;
        start|stop|restart|status) ;;
        *) error "unknown action: $action"; usage >&2; return 1 ;;
    esac
    validate_target "$target" || return 1

    case "$action" in
        start)
            install_startup_traps
            start_target "$target"
            clear_startup_traps
            ;;
        stop) stop_target "$target" ;;
        restart)
            stop_target "$target"
            install_startup_traps
            start_target "$target"
            clear_startup_traps
            ;;
        status) status_target "$target" ;;
    esac
}

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
    main "$@"
fi
