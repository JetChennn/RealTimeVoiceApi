#!/usr/bin/env bash
# Start the local RealTimeVoiceApi stack: ASR, Thinker, TTS, and the gateway.
#
# Usage:
#   ./start_services.sh [start|stop|status|restart]
#
# Optional environment overrides:
#   TTS_MODEL_PATH                 Path to the PromptTTSD model directory.
#   ASR_GPU_MEMORY_UTILIZATION     vLLM GPU-memory fraction (default: 0.80).
#   ASR_CUDA_VISIBLE_DEVICES       GPU(s) available to ASR (default: 0).
#   TTS_CUDA_VISIBLE_DEVICES       GPU(s) available to TTS (default: 1).
#   TTS_VLLM_CUDA_VISIBLE_DEVICES  GPU(s) used by the TTS vLLM engine (default: 1).
#   ASR_MAX_MODEL_LEN              ASR context length (default: 4096).
#   ASR_MAX_NUM_SEQS               ASR vLLM concurrency (default: 30).
#   ASR_MAX_NUM_BATCHED_TOKENS     ASR vLLM batch budget (default: 8192).
#   GATEWAY_MAX_SESSIONS           WebSocket session limit (default: 30).
#   GATEWAY_CPU_WORKERS            VAD worker threads (default: 8).
#   THINKER_CUDA_VISIBLE_DEVICES   GPU(s) available to Thinker.
#   STARTUP_TIMEOUT_SECONDS        Per-service readiness timeout (default: 900).

set -Eeuo pipefail

readonly SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
readonly WORKSPACE_DIR="$(cd "$SCRIPT_DIR/.." && pwd -P)"
readonly ASR_DIR="$WORKSPACE_DIR/asr"
readonly THINKER_DIR="$WORKSPACE_DIR/BerryThinker"
readonly TTS_DIR="$WORKSPACE_DIR/PromptTTSD"
readonly RUN_DIR="$SCRIPT_DIR/.run"
readonly LOG_DIR="$SCRIPT_DIR/logs"

readonly ASR_PORT=8001
readonly THINKER_PORT=8002
readonly TTS_PORT=9000
readonly INTERNAL_HOST="${INTERNAL_HOST:-127.0.0.1}"
readonly ASR_BIN="$ASR_DIR/.venv/bin/qwen-asr-serve"
readonly ASR_MODEL_PATH="${ASR_MODEL_PATH:-$ASR_DIR/Qwen3-ASR-0.6B}"
readonly THINKER_PYTHON="${THINKER_PYTHON:-$THINKER_DIR/.venv/bin/python}"
readonly TTS_PYTHON="${TTS_PYTHON:-$TTS_DIR/.venv/bin/python}"
readonly GATEWAY_PYTHON="${GATEWAY_PYTHON:-$SCRIPT_DIR/.venv/bin/python}"
readonly TTS_MODEL_PATH="${TTS_MODEL_PATH:-$WORKSPACE_DIR/../persistence/workspace/model/2026_7_27}"
readonly ASR_CUDA_VISIBLE_DEVICES="${ASR_CUDA_VISIBLE_DEVICES:-0}"
readonly TTS_CUDA_VISIBLE_DEVICES="${TTS_CUDA_VISIBLE_DEVICES:-1}"
readonly TTS_VLLM_CUDA_VISIBLE_DEVICES="${TTS_VLLM_CUDA_VISIBLE_DEVICES:-1}"
readonly ASR_GPU_MEMORY_UTILIZATION="${ASR_GPU_MEMORY_UTILIZATION:-0.80}"
readonly ASR_MAX_MODEL_LEN="${ASR_MAX_MODEL_LEN:-4096}"
readonly ASR_MAX_NUM_SEQS="${ASR_MAX_NUM_SEQS:-30}"
readonly ASR_MAX_NUM_BATCHED_TOKENS="${ASR_MAX_NUM_BATCHED_TOKENS:-8192}"
readonly GATEWAY_MAX_SESSIONS="${GATEWAY_MAX_SESSIONS:-30}"
readonly GATEWAY_CPU_WORKERS="${GATEWAY_CPU_WORKERS:-8}"
readonly GATEWAY_CPU_PENDING_JOBS="${GATEWAY_CPU_PENDING_JOBS:-256}"
readonly STARTUP_TIMEOUT_SECONDS="${STARTUP_TIMEOUT_SECONDS:-900}"

export SCRIPT_DIR WORKSPACE_DIR ASR_DIR THINKER_DIR TTS_DIR
export ASR_PORT THINKER_PORT TTS_PORT INTERNAL_HOST
export ASR_BIN ASR_MODEL_PATH THINKER_PYTHON TTS_PYTHON GATEWAY_PYTHON TTS_MODEL_PATH
export ASR_CUDA_VISIBLE_DEVICES TTS_CUDA_VISIBLE_DEVICES TTS_VLLM_CUDA_VISIBLE_DEVICES ASR_GPU_MEMORY_UTILIZATION
export ASR_MAX_MODEL_LEN ASR_MAX_NUM_SEQS ASR_MAX_NUM_BATCHED_TOKENS

declare -a STARTED_SERVICES=()

die() {
    echo "Error: $*" >&2
    exit 1
}

pid_file() {
    printf '%s/%s.pid\n' "$RUN_DIR" "$1"
}

log_file() {
    printf '%s/%s.log\n' "$LOG_DIR" "$1"
}

is_running() {
    local service="$1"
    local file
    local pid
    local state
    file="$(pid_file "$service")"
    [[ -f "$file" ]] || return 1
    pid="$(<"$file")"
    [[ "$pid" =~ ^[1-9][0-9]*$ ]] || return 1
    kill -0 "$pid" 2>/dev/null || return 1

    # `kill -0` still succeeds for a zombie. Treat it as stopped so a failed
    # service is reported immediately instead of waiting for the full timeout.
    if [[ -r "/proc/$pid/stat" ]]; then
        state="$(awk '{print $3}' "/proc/$pid/stat")"
        [[ "$state" != "Z" ]] || return 1
    fi
}

endpoint_ready() {
    curl --fail --silent --max-time 3 "$1" >/dev/null
}

load_gateway_config() {
    local env_file="$SCRIPT_DIR/.env"
    if [[ ! -f "$env_file" ]]; then
        env_file="$SCRIPT_DIR/.env.example"
    fi

    [[ -f "$env_file" ]] || die "missing gateway environment file: $env_file"
    set -a
    # shellcheck disable=SC1090
    source "$env_file"
    set +a

    [[ "${RTVA_PORT:-}" == "8000" ]] || die "RTVA_PORT must be 8000"
    [[ "${RTVA_ASR_BASE_URL:-}" == "http://127.0.0.1:8001" ]] || die "RTVA_ASR_BASE_URL must be http://127.0.0.1:8001"
    [[ "${RTVA_THINKER_BASE_URL:-}" == "http://127.0.0.1:8002" ]] || die "RTVA_THINKER_BASE_URL must be http://127.0.0.1:8002"
    [[ "${RTVA_TTS_BASE_URL:-}" == "http://127.0.0.1:9000" ]] || die "RTVA_TTS_BASE_URL must be http://127.0.0.1:9000"

    # Deployment defaults for 30 concurrent WebSocket sessions. Keep these as
    # launcher-specific values so a user's .env remains untouched.
    export RTVA_MAX_SESSIONS="$GATEWAY_MAX_SESSIONS"
    export RTVA_CPU_WORKERS="$GATEWAY_CPU_WORKERS"
    export RTVA_CPU_PENDING_JOBS="$GATEWAY_CPU_PENDING_JOBS"
    export RTVA_ASR_CONCURRENCY="$ASR_MAX_NUM_SEQS"
    export RTVA_ASR_MAX_WAITERS="$GATEWAY_MAX_SESSIONS"
}

validate_runtime() {
    command -v curl >/dev/null 2>&1 || die "curl is required for readiness checks"
    command -v setsid >/dev/null 2>&1 || die "setsid is required to detach service processes"
    [[ -x "$ASR_BIN" ]] || die "ASR launcher not found: $ASR_BIN"
    [[ -d "$ASR_MODEL_PATH" ]] || die "ASR model directory not found: $ASR_MODEL_PATH"
    [[ -x "$THINKER_PYTHON" ]] || die "Thinker Python interpreter not found: $THINKER_PYTHON"
    [[ -f "$THINKER_DIR/apps/thinker_api.py" ]] || die "Thinker entry point not found"
    [[ -x "$TTS_PYTHON" ]] || die "TTS Python interpreter not found: $TTS_PYTHON"
    [[ -f "$TTS_DIR/api/dialogue_tts_api.py" ]] || die "TTS entry point not found"
    [[ -x "$GATEWAY_PYTHON" ]] || die "Gateway Python interpreter not found: $GATEWAY_PYTHON"
    [[ -f "$TTS_MODEL_PATH/flow.pt" ]] || die "TTS model is invalid (flow.pt missing): $TTS_MODEL_PATH"
    [[ "$ASR_CUDA_VISIBLE_DEVICES" != "$TTS_CUDA_VISIBLE_DEVICES" ]] || die "ASR and TTS must use different GPUs"
}

start_process() {
    local service="$1"
    shift
    local pid_path
    local output
    pid_path="$(pid_file "$service")"
    output="$(log_file "$service")"

    rm -f "$pid_path"

    # --fork creates a new session and parents the service independently of
    # this launcher. The wrapper records its own PID before exec'ing the
    # service command, so `stop` only ever targets a service we started.
    setsid --fork bash -c '
        pid_path="$1"
        shift
        printf "%s\\n" "$$" > "$pid_path"
        exec "$@"
    ' start_services.sh "$pid_path" "$@" >> "$output" 2>&1 < /dev/null &

    for _ in {1..50}; do
        if [[ -s "$pid_path" ]] && kill -0 "$(<"$pid_path")" 2>/dev/null; then
            break
        fi
        sleep 0.1
    done
    [[ -s "$pid_path" ]] || die "$service did not create its PID file; see $output"
    is_running "$service" || die "$service exited immediately; see $output"
    STARTED_SERVICES+=("$service")
    echo "Started $service (PID $(<"$pid_path")); log: $output"
}

wait_for_http() {
    local service="$1"
    local url="$2"
    local deadline=$((SECONDS + STARTUP_TIMEOUT_SECONDS))
    local pid_path
    pid_path="$(pid_file "$service")"

    while (( SECONDS < deadline )); do
        # A connection refusal is expected while a model is loading. Services
        # without one of our PID files are treated as externally managed and
        # may be reused as soon as their health endpoint becomes ready.
        if endpoint_ready "$url"; then
            echo "$service is ready: $url"
            return
        fi
        if [[ -f "$pid_path" ]] && ! is_running "$service"; then
            echo "--- Last 80 lines of $(log_file "$service") ---" >&2
            tail -n 80 "$(log_file "$service")" >&2 || true
            die "$service exited before becoming ready"
        fi
        sleep 2
    done

    die "$service was not ready within ${STARTUP_TIMEOUT_SECONDS}s: $url"
}

stop_service() {
    local service="$1"
    local pid_path
    local pid
    pid_path="$(pid_file "$service")"
    [[ -f "$pid_path" ]] || return
    pid="$(<"$pid_path")"

    if is_running "$service"; then
        echo "Stopping $service (PID $pid)"
        kill "$pid" 2>/dev/null || true
        for _ in {1..15}; do
            is_running "$service" || break
            sleep 1
        done
        if is_running "$service"; then
            echo "Force-stopping $service (PID $pid)" >&2
            kill -KILL "$pid" 2>/dev/null || true
        fi
    fi
    rm -f "$pid_path"
}

cleanup_failed_start() {
    local status=$?
    if (( status != 0 )); then
        for (( index=${#STARTED_SERVICES[@]} - 1; index >= 0; index-- )); do
            stop_service "${STARTED_SERVICES[index]}"
        done
    fi
    exit "$status"
}

start_asr() {
    local url="http://$INTERNAL_HOST:$ASR_PORT/health"
    if endpoint_ready "$url"; then
        echo "ASR is already ready; reusing it."
        return
    fi
    if is_running asr; then
        echo "ASR is already starting (PID $(<"$(pid_file asr)")); reusing it."
        return
    fi
    start_process asr bash -c '
        set -Eeuo pipefail
        export CUDA_VISIBLE_DEVICES="$ASR_CUDA_VISIBLE_DEVICES"
        exec "$ASR_BIN" "$ASR_MODEL_PATH" \
            --gpu-memory-utilization "$ASR_GPU_MEMORY_UTILIZATION" \
            --max-model-len "$ASR_MAX_MODEL_LEN" \
            --max-num-seqs "$ASR_MAX_NUM_SEQS" \
            --max-num-batched-tokens "$ASR_MAX_NUM_BATCHED_TOKENS" \
            --host "$INTERNAL_HOST" \
            --port "$ASR_PORT"
    '
}

start_thinker() {
    local url="http://$INTERNAL_HOST:$THINKER_PORT/health"
    if endpoint_ready "$url"; then
        echo "Thinker is already ready; reusing it."
        return
    fi
    if is_running thinker; then
        echo "Thinker is already starting (PID $(<"$(pid_file thinker)")); reusing it."
        return
    fi
    start_process thinker bash -c '
        set -Eeuo pipefail
        cd "$THINKER_DIR"
        if [[ -f .env.local ]]; then
            set -a
            # shellcheck disable=SC1091
            source .env.local
            set +a
        fi
        export MIO_API_PORT="$THINKER_PORT"
        export PYTHONPATH="$THINKER_DIR${PYTHONPATH:+:$PYTHONPATH}"
        if [[ -n "${THINKER_CUDA_VISIBLE_DEVICES:-}" ]]; then
            export CUDA_VISIBLE_DEVICES="$THINKER_CUDA_VISIBLE_DEVICES"
        fi
        # Ark SDK 会读取 ALL_PROXY；当前环境中的 socks5 代理需要未安装的
        # socksio 依赖。保留 HTTP(S)_PROXY，同时禁止继承 SOCKS 代理。
        unset ALL_PROXY all_proxy
        exec "$THINKER_PYTHON" apps/thinker_api.py --host "$INTERNAL_HOST" --port "$THINKER_PORT"
    '
}

start_tts() {
    local url="http://$INTERNAL_HOST:$TTS_PORT/health"
    if endpoint_ready "$url"; then
        echo "TTS is already ready; reusing it."
        return
    fi
    if is_running tts; then
        echo "TTS is already starting (PID $(<"$(pid_file tts)")); reusing it."
        return
    fi
    start_process tts bash -c '
        set -Eeuo pipefail
        cd "$TTS_DIR"
        export MODEL_PATH="$TTS_MODEL_PATH"
        export LLM_ENGINE="${TTS_LLM_ENGINE:-vllm}"
        export FP16_FLOW=false
        export SOULX_STREAM_TOKEN_HOP_LEN=15
        export FLOW_N_TIMESTEPS=10
        export VLLM_CUDA_VISIBLE_DEVICES="$TTS_VLLM_CUDA_VISIBLE_DEVICES"
        export CUDA_VISIBLE_DEVICES="$TTS_CUDA_VISIBLE_DEVICES"
        exec "$TTS_PYTHON" -m uvicorn api.dialogue_tts_api:app \
            --host "$INTERNAL_HOST" \
            --port "$TTS_PORT"
    '
}

start_gateway() {
    local url="http://127.0.0.1:$RTVA_PORT/health"
    if endpoint_ready "$url"; then
        echo "Gateway is already ready; reusing it."
        return
    fi
    if is_running gateway; then
        echo "Gateway is already starting (PID $(<"$(pid_file gateway)")); reusing it."
        return
    fi
    start_process gateway bash -c '
        set -Eeuo pipefail
        cd "$SCRIPT_DIR"
        exec "$GATEWAY_PYTHON" -m uvicorn realtime_voice.main:app \
            --app-dir src \
            --host "$RTVA_HOST" \
            --port "$RTVA_PORT"
    '
}

start_all() {
    load_gateway_config
    validate_runtime
    mkdir -p "$RUN_DIR" "$LOG_DIR"

    trap cleanup_failed_start ERR
    start_asr
    start_thinker
    start_tts
    start_gateway
    wait_for_http asr "http://$INTERNAL_HOST:$ASR_PORT/health"
    wait_for_http thinker "http://$INTERNAL_HOST:$THINKER_PORT/health"
    wait_for_http tts "http://$INTERNAL_HOST:$TTS_PORT/health"
    wait_for_http gateway "http://127.0.0.1:$RTVA_PORT/health"
    trap - ERR
    echo "All services are ready. Gateway: http://127.0.0.1:$RTVA_PORT/health"
}

status_all() {
    for service in asr thinker tts gateway; do
        local url
        case "$service" in
            asr) url="http://$INTERNAL_HOST:$ASR_PORT/health" ;;
            thinker) url="http://$INTERNAL_HOST:$THINKER_PORT/health" ;;
            tts) url="http://$INTERNAL_HOST:$TTS_PORT/health" ;;
            gateway) url="http://127.0.0.1:8000/health" ;;
        esac
        if endpoint_ready "$url" && is_running "$service"; then
            echo "$service: running (PID $(<"$(pid_file "$service")"))"
        elif endpoint_ready "$url"; then
            echo "$service: running (externally managed)"
        elif is_running "$service"; then
            echo "$service: starting (PID $(<"$(pid_file "$service")"))"
        else
            echo "$service: stopped"
        fi
    done
}

stop_all() {
    for service in gateway tts thinker asr; do
        stop_service "$service"
    done
}

case "${1:-start}" in
    start)
        start_all
        ;;
    stop)
        stop_all
        ;;
    status)
        status_all
        ;;
    restart)
        stop_all
        start_all
        ;;
    *)
        die "usage: $0 [start|stop|status|restart]"
        ;;
esac
