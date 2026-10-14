#!/usr/bin/env bash
# TransitPulse native bootstrap, phase A (no Python dependencies; Linux x86_64, e.g. a Google Colab CPU VM).
#
#   bash scripts/bootstrap_colab.sh --workdir W [--repo-dir R] [--profile colab-native|local-native]
#                                   [--strict] [--offline] [--no-tunnel]
#
# Phase A: lock W/runtime/bootstrap.lock (fail fast if held), check platform/tools/disk, install the pinned
# uv (download -> official .sha256 from the same GitHub release -> verify -> extract into W/tools/uv/<ver>),
# isolate the uv environment (UV_* in W, managed Python only), `uv python install` from .python-version,
# `uv lock` ONLY when uv.lock is missing (origin recorded in W/runtime/lock_origin.json; uv locks every optional
# extra), `uv sync --locked --extra dashboard` (Streamlit/PyDeck; ALWAYS synced, also with --no-tunnel, because
# the local dashboard uses it too). Phase B also installs the pinned cloudflared binary unless --no-tunnel is
# given; a cloudflared failure only disables the public tunnel (warning) unless --strict.
# Then exec phase B: R/.venv/bin/python -I R/scripts/prepare_runtime.py (JDK, Kafka, Cassandra, connector,
# configs, runtime.json, probes). Idempotent; reruns reuse caches; never deletes data.
#
# Output contract: progress on stderr; exactly ONE JSON document on stdout (phase B prints it on success
# and on its own failures; phase A prints one only when it fails). Exit codes: 0 ok, 2 usage,
# 3 preflight, 4 conflict (lock held), 7 artifact/checksum, 1 other.
set -Eeuo pipefail
umask 022

# Keep the original stdout on fd 3 for the final JSON; everything else goes to stderr.
exec 3>&1 1>&2

UV_VERSION_DEFAULT="0.12.23"
UV_TARGET="x86_64-unknown-linux-gnu"
MIN_DISK_KIB=$((6 * 1024 * 1024))
WARN_DISK_KIB=$((10 * 1024 * 1024))

WORKDIR=""
REPO_DIR=""
PROFILE="colab-native"
STRICT=0
OFFLINE=0
NO_TUNNEL=0

log() { printf '[bootstrap %s] %s\n' "$(date -u +%H:%M:%SZ)" "$*" >&2; }

json_str() {
  # JSON string literal of $1 (escapes backslash, quote and control characters).
  local s="$1"
  s="${s//\\/\\\\}"
  s="${s//\"/\\\"}"
  s="${s//$'\n'/\\n}"
  s="${s//$'\r'/\\r}"
  s="${s//$'\t'/\\t}"
  s="$(printf '%s' "$s" | tr -d '\000-\010\013\014\016-\037')"
  printf '"%s"' "$s"
}

fail() {
  # fail EXIT_CODE CODE MESSAGE [HINT] [LOG_EXCERPT]
  local exit_code="$1" code="$2" message="$3" hint="${4:-}" excerpt="${5:-}"
  trap - ERR
  log "ERROR ${code}: ${message}"
  if [[ -n "$hint" ]]; then log "hint: ${hint}"; fi
  local hint_json="null" excerpt_json="null"
  if [[ -n "$hint" ]]; then hint_json="$(json_str "$hint")"; fi
  if [[ -n "$excerpt" ]]; then excerpt_json="$(json_str "$excerpt")"; fi
  printf '{"ok": false, "action": "bootstrap", "phase": "A", "workdir": %s, "error": {"code": %s, "message": %s, "hint": %s, "log_excerpt": %s}}\n' \
    "$(json_str "${WORKDIR:-}")" "$(json_str "$code")" "$(json_str "$message")" "$hint_json" "$excerpt_json" >&3
  exit "$exit_code"
}

on_err() {
  local rc="$1" line="$2" cmd="$3"
  fail 1 "BOOTSTRAP_PHASE_A_FAILED" "command failed (exit ${rc}) at line ${line}: ${cmd}" \
    "see the progress lines above"
}
trap 'on_err "$?" "$LINENO" "$BASH_COMMAND"' ERR

usage() {
  cat >&2 <<'EOF'
usage: bash scripts/bootstrap_colab.sh --workdir W [--repo-dir R] [--profile colab-native|local-native]
                                       [--strict] [--offline] [--no-tunnel]
  --workdir   work directory for tools, caches, data, logs (e.g. /content/transitpulse-runtime)
  --repo-dir  repository root (default: the parent of this script's directory)
  --profile   config profile written into runtime.json (default colab-native)
  --strict    fail when an artifact checksum is not pinned in deploy/runtime-manifest.json
  --offline   never download; use the caches of a previous bootstrap
  --no-tunnel do not install cloudflared (no public Cloudflare quick tunnel); the dashboard extra
              (Streamlit/PyDeck) is still synced
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --workdir) [[ $# -ge 2 ]] || { usage; fail 2 USAGE "--workdir needs a value"; }; WORKDIR="$2"; shift 2 ;;
    --workdir=*) WORKDIR="${1#*=}"; shift ;;
    --repo-dir) [[ $# -ge 2 ]] || { usage; fail 2 USAGE "--repo-dir needs a value"; }; REPO_DIR="$2"; shift 2 ;;
    --repo-dir=*) REPO_DIR="${1#*=}"; shift ;;
    --profile) [[ $# -ge 2 ]] || { usage; fail 2 USAGE "--profile needs a value"; }; PROFILE="$2"; shift 2 ;;
    --profile=*) PROFILE="${1#*=}"; shift ;;
    --strict) STRICT=1; shift ;;
    --offline) OFFLINE=1; shift ;;
    --no-tunnel) NO_TUNNEL=1; shift ;;
    -h|--help) usage; printf '{"ok": true, "action": "help", "error": null}\n' >&3; exit 0 ;;
    *) usage; fail 2 USAGE "unknown argument: $1" "run with --help" ;;
  esac
done

[[ -n "$WORKDIR" ]] || { usage; fail 2 USAGE "--workdir is required"; }
case "$PROFILE" in
  colab-native|local-native) ;;
  *) fail 2 USAGE "unsupported profile: ${PROFILE}" "use colab-native or local-native" ;;
esac
if [[ -z "$REPO_DIR" ]]; then
  REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)"
fi
[[ -d "$REPO_DIR" ]] || fail 2 USAGE "repository directory not found: ${REPO_DIR}"
REPO_DIR="$(cd "$REPO_DIR" && pwd -P)"
[[ -f "$REPO_DIR/pyproject.toml" && -f "$REPO_DIR/.python-version" ]] || \
  fail 3 REPO_INVALID "${REPO_DIR} is not the TransitPulse repository (pyproject.toml/.python-version missing)"

# Native launchers (bash scripts, Ivy URIs) break on whitespace and quotes: require a conservative charset.
case "$WORKDIR" in
  /*) ;;
  *) WORKDIR="$(pwd -P)/${WORKDIR}" ;;
esac
if [[ ! "$WORKDIR" =~ ^/[A-Za-z0-9._+/-]*$ ]]; then
  fail 3 UNSAFE_PATH "work directory '${WORKDIR}' contains unsupported characters" \
    "use only letters, digits, '.', '_', '+', '-' and '/'"
fi
if [[ ! "$REPO_DIR" =~ ^/[A-Za-z0-9._+/-]*$ ]]; then
  fail 3 UNSAFE_PATH "repository directory '${REPO_DIR}' contains unsupported characters" \
    "clone into a path with only letters, digits, '.', '_', '+', '-' and '/'"
fi

# ---------------------------------------------------------------- platform and tools
[[ "$(uname -s)" == "Linux" ]] || fail 3 UNSUPPORTED_PLATFORM "OS $(uname -s) is not supported" "use Linux x86_64"
[[ "$(uname -m)" == "x86_64" ]] || fail 3 UNSUPPORTED_PLATFORM "architecture $(uname -m) is not supported" \
  "the pinned binaries are x86_64"
for tool in curl tar sha256sum flock df awk; do
  command -v "$tool" >/dev/null 2>&1 || fail 3 TOOL_MISSING "required tool not found: ${tool}" \
    "install ${tool} (present on Google Colab by default)"
done

mkdir -p "$WORKDIR/runtime" "$WORKDIR/tools/.trash" "$WORKDIR/cache/downloads/.partial" "$WORKDIR/cache/uv" \
         "$WORKDIR/tools/python" "$WORKDIR/logs/bootstrap" "$WORKDIR/tmp"
WORKDIR="$(cd "$WORKDIR" && pwd -P)"

# ---------------------------------------------------------------- lock (held through phase B via fd 9)
exec 9>>"$WORKDIR/runtime/bootstrap.lock"
if ! flock -n 9; then
  fail 4 BOOTSTRAP_LOCKED "another bootstrap is running for ${WORKDIR}" "wait for it to finish"
fi

avail_kib="$(df -Pk "$WORKDIR" | awk 'NR==2 {print $4}')"
if [[ ! "$avail_kib" =~ ^[0-9]+$ ]]; then
  fail 3 DISK_UNKNOWN "cannot read free disk space for ${WORKDIR}"
fi
if (( avail_kib < MIN_DISK_KIB )); then
  fail 3 DISK_TOO_SMALL "only $((avail_kib / 1024 / 1024)) GiB free on the filesystem of ${WORKDIR}; 6 GiB required" \
    "free disk space or choose another --workdir"
fi
if (( avail_kib < WARN_DISK_KIB )); then
  log "WARNING: $((avail_kib / 1024 / 1024)) GiB free (< 10 GiB recommended)"
fi
log "workdir=${WORKDIR} repo=${REPO_DIR} profile=${PROFILE} strict=${STRICT} offline=${OFFLINE} no_tunnel=${NO_TUNNEL}"

# ---------------------------------------------------------------- uv entry from the committed manifest
MANIFEST="$REPO_DIR/deploy/runtime-manifest.json"
[[ -f "$MANIFEST" ]] || fail 7 MANIFEST_MISSING "${MANIFEST} not found"
UV_VERSION="$UV_VERSION_DEFAULT"
UV_URL="https://github.com/astral-sh/uv/releases/download/${UV_VERSION_DEFAULT}/uv-${UV_TARGET}.tar.gz"
UV_TOP="uv-${UV_TARGET}"
UV_INSTALL_REL="tools/uv/${UV_VERSION_DEFAULT}"
if command -v python3 >/dev/null 2>&1; then
  # Read-only: the system python only parses JSON (-I -S: no site packages, no user env).
  if uv_info="$(python3 -I -S - "$MANIFEST" <<'PY'
import json, sys
with open(sys.argv[1], encoding="utf-8") as fh:
    manifest = json.load(fh)
for art in manifest["artifacts"]:
    if art.get("id") == "uv":
        print(art["version"])
        print(art["urls"][0])
        print(art["top_dir"])
        print(art["install_to"])
        break
else:
    sys.exit(3)
PY
)"; then
    UV_VERSION="$(printf '%s\n' "$uv_info" | sed -n '1p')"
    UV_URL="$(printf '%s\n' "$uv_info" | sed -n '2p')"
    UV_TOP="$(printf '%s\n' "$uv_info" | sed -n '3p')"
    UV_INSTALL_REL="$(printf '%s\n' "$uv_info" | sed -n '4p')"
  else
    fail 7 MANIFEST_INVALID "cannot read the uv entry of ${MANIFEST}"
  fi
else
  log "python3 not found: using the built-in uv pin ${UV_VERSION_DEFAULT} (phase B re-checks it against the manifest)"
fi
[[ "$UV_URL" == https://* ]] || fail 7 MANIFEST_INVALID "uv URL must be https: ${UV_URL}"
[[ "$UV_TOP" =~ ^[A-Za-z0-9._+-]+$ ]] || fail 7 MANIFEST_INVALID "invalid uv top_dir: ${UV_TOP}"
[[ "$UV_INSTALL_REL" =~ ^[A-Za-z0-9._+/-]+$ && "$UV_INSTALL_REL" != *..* ]] || \
  fail 7 MANIFEST_INVALID "invalid uv install_to: ${UV_INSTALL_REL}"

UV_DIR="$WORKDIR/$UV_INSTALL_REL"
UV_BIN="$UV_DIR/uv"
UV_ASSET="${UV_URL##*/}"
DL_DIR="$WORKDIR/cache/downloads"
PARTIAL_DIR="$DL_DIR/.partial"

uv_version_ok() {
  local out
  [[ -x "$UV_BIN" ]] || return 1
  out="$("$UV_BIN" --version 2>/dev/null || true)"
  [[ "$out" == "uv ${UV_VERSION}"* ]]
}

fetch() {
  # fetch URL DEST: download through a .partial file, rename only on success.
  local url="$1" dest="$2" tmp
  tmp="$PARTIAL_DIR/$(basename "$dest").$$"
  rm -f "$tmp"
  if ! curl -fsSL --proto '=https' --tlsv1.2 --retry 3 --retry-delay 2 --connect-timeout 20 --max-time 900 \
       -o "$tmp" "$url"; then
    rm -f "$tmp"
    return 1
  fi
  mv -f "$tmp" "$dest"
}

install_uv() {
  local archive="$DL_DIR/$UV_ASSET" official="$DL_DIR/$UV_ASSET.sha256.official"
  local expected actual staging listing name
  if [[ ! -f "$official" ]]; then
    (( OFFLINE == 0 )) || fail 7 OFFLINE_ARTIFACT_MISSING "uv ${UV_VERSION} checksum not cached and --offline was given" \
      "run bootstrap once with network access"
    fetch "${UV_URL}.sha256" "$official" || fail 7 DOWNLOAD_FAILED "cannot download ${UV_URL}.sha256" \
      "check network access to github.com"
  fi
  expected="$(awk 'NR==1 {print tolower($1)}' "$official")"
  [[ "$expected" =~ ^[0-9a-f]{64}$ ]] || { rm -f "$official"; fail 7 CHECKSUM_UNPARSEABLE "official uv checksum file is malformed"; }
  if [[ -f "$archive" ]]; then
    actual="$(sha256sum "$archive" | awk '{print $1}')"
    if [[ "$actual" != "$expected" ]]; then
      log "cached ${UV_ASSET} does not match the official checksum; moving it aside and re-downloading"
      mkdir -p "$DL_DIR/.trash"
      mv -f "$archive" "$DL_DIR/.trash/${UV_ASSET}.$(date -u +%Y%m%dT%H%M%S)"
    fi
  fi
  if [[ ! -f "$archive" ]]; then
    (( OFFLINE == 0 )) || fail 7 OFFLINE_ARTIFACT_MISSING "uv ${UV_VERSION} archive not cached and --offline was given"
    log "downloading ${UV_URL}"
    local tmp="$PARTIAL_DIR/${UV_ASSET}.$$"
    rm -f "$tmp"
    if ! curl -fsSL --proto '=https' --tlsv1.2 --retry 3 --retry-delay 2 --connect-timeout 20 --max-time 900 \
         -o "$tmp" "$UV_URL"; then
      rm -f "$tmp"
      fail 7 DOWNLOAD_FAILED "cannot download ${UV_URL}" "check network access to github.com"
    fi
    actual="$(sha256sum "$tmp" | awk '{print $1}')"
    if [[ "$actual" != "$expected" ]]; then
      rm -f "$tmp"
      fail 7 CHECKSUM_MISMATCH "sha256 of ${UV_ASSET} is ${actual}, official value is ${expected}" \
        "nothing was extracted; re-run bootstrap"
    fi
    mv -f "$tmp" "$archive"
  fi
  # Safe extraction: every member must live under the expected top directory, no '..' components.
  listing="$WORKDIR/tmp/uv-listing.$$"
  tar -tzf "$archive" > "$listing" || { rm -f "$listing"; fail 7 EXTRACT_FAILED "cannot list ${archive}"; }
  while IFS= read -r name; do
    name="${name#./}"
    if [[ "$name" == /* || "$name" == ".." || "$name" == ../* || "$name" == */../* || "$name" == */.. ]]; then
      rm -f "$listing"; fail 7 UNSAFE_ARCHIVE "unsafe member in ${UV_ASSET}: ${name}"
    fi
    if [[ "$name" != "$UV_TOP" && "$name" != "$UV_TOP/" && "$name" != "$UV_TOP"/* ]]; then
      rm -f "$listing"; fail 7 UNSAFE_ARCHIVE "member outside ${UV_TOP}/ in ${UV_ASSET}: ${name}"
    fi
  done < "$listing"
  rm -f "$listing"
  staging="$WORKDIR/tools/uv/.staging.$$"
  rm -rf "$staging"
  mkdir -p "$staging"
  tar -xzf "$archive" -C "$staging" --no-same-owner || { rm -rf "$staging"; fail 7 EXTRACT_FAILED "cannot extract ${archive}"; }
  [[ -x "$staging/$UV_TOP/uv" ]] || { rm -rf "$staging"; fail 7 EXTRACT_FAILED "uv binary missing in ${UV_ASSET}"; }
  if [[ -e "$UV_DIR" ]]; then
    mv "$UV_DIR" "$WORKDIR/tools/.trash/uv-${UV_VERSION}.$(date -u +%Y%m%dT%H%M%S).$$"
  fi
  mkdir -p "$(dirname "$UV_DIR")"
  mv "$staging/$UV_TOP" "$UV_DIR"
  rm -rf "$staging"
}

if uv_version_ok; then
  log "uv ${UV_VERSION} already installed (${UV_BIN})"
else
  install_uv
  uv_version_ok || fail 7 UV_MISMATCH "${UV_BIN} does not report uv ${UV_VERSION}"
  log "installed $("$UV_BIN" --version)"
fi

# ---------------------------------------------------------------- isolate uv and Python from the caller
while IFS= read -r var; do
  unset "$var"
done < <(compgen -e | grep -E '^(UV_|PYSPARK_)' || true)
unset PYTHONPATH PYTHONHOME PYTHONSTARTUP PYTHONUSERBASE VIRTUAL_ENV CONDA_PREFIX SPARK_HOME JAVA_HOME \
      CLASSPATH JAVA_TOOL_OPTIONS _JAVA_OPTIONS || true
export UV_CACHE_DIR="$WORKDIR/cache/uv"
export UV_PYTHON_INSTALL_DIR="$WORKDIR/tools/python"
# `uv python install` also links python3.x executables into a bin dir (default ~/.local/bin): keep it in W.
export UV_PYTHON_BIN_DIR="$WORKDIR/tools/python/bin"
mkdir -p "$UV_PYTHON_BIN_DIR"
export UV_PYTHON_PREFERENCE="only-managed"
export UV_NO_CONFIG=1
export UV_HTTP_TIMEOUT=300
export UV_PROJECT_ENVIRONMENT="$REPO_DIR/.venv"
export UV_NO_PROGRESS=1
if (( OFFLINE == 1 )); then
  export UV_OFFLINE=1
fi
export TMPDIR="$WORKDIR/tmp"

PY_PIN="$(tr -d '[:space:]' < "$REPO_DIR/.python-version")"
[[ "$PY_PIN" =~ ^3\.11\.[0-9]+$ ]] || fail 3 PYTHON_PIN_INVALID ".python-version must pin a 3.11.x patch, found '${PY_PIN}'"

UV_LOG="$WORKDIR/logs/bootstrap/uv.log"
run_uv() {
  # run_uv EXIT_CODE CODE HINT -- uv args...: stream output to stderr and the uv log; fail with EXIT_CODE on error
  # (7 = artifact download/verification such as a managed Python, 1 = lock/sync failures).
  local exit_code="$1" code="$2" hint="$3"
  shift 4
  log "uv $*"
  local rc=0
  "$UV_BIN" "$@" 2>&1 | tee -a "$UV_LOG" >&2 || rc=$?
  if (( rc != 0 )); then
    fail "$exit_code" "$code" "uv $1 failed (exit ${rc})" "$hint" "$(tail -n 40 "$UV_LOG" 2>/dev/null || true)"
  fi
}

printf '\n===== %s bootstrap phase A\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" >> "$UV_LOG"
run_uv 7 PYTHON_INSTALL_FAILED \
  "uv ${UV_VERSION} may not know Python ${PY_PIN}; pick a 3.11 patch listed by 'uv python list --only-downloads'" \
  -- python install "$PY_PIN"

LOCK_FILE="$REPO_DIR/uv.lock"
LOCK_ORIGIN_FILE="$WORKDIR/runtime/lock_origin.json"
LOCK_ORIGIN="committed"
if [[ ! -f "$LOCK_FILE" ]]; then
  (( OFFLINE == 0 )) || fail 3 LOCK_MISSING "uv.lock is missing and --offline was given" \
    "commit uv.lock or run once with network access"
  if [[ "$PROFILE" == "colab-native" ]]; then LOCK_ORIGIN="generated_on_colab"; else LOCK_ORIGIN="generated_locally"; fi
  log "uv.lock not found: generating it (lock_origin=${LOCK_ORIGIN}); export it with export_artifacts.py and commit it"
  run_uv 1 LOCK_FAILED "check pyproject.toml pins and network access to pypi.org" -- lock --project "$REPO_DIR" \
    --python "$PY_PIN"
  lock_sha="$(sha256sum "$LOCK_FILE" | awk '{print $1}')"
  printf '{"lock_origin": "%s", "lock_sha256": "%s", "generated_at": "%s", "uv_version": "%s"}\n' \
    "$LOCK_ORIGIN" "$lock_sha" "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$UV_VERSION" > "$LOCK_ORIGIN_FILE.tmp"
  mv -f "$LOCK_ORIGIN_FILE.tmp" "$LOCK_ORIGIN_FILE"
else
  lock_sha="$(sha256sum "$LOCK_FILE" | awk '{print $1}')"
  if [[ -f "$LOCK_ORIGIN_FILE" ]] && grep -q "\"lock_sha256\": \"${lock_sha}\"" "$LOCK_ORIGIN_FILE"; then
    LOCK_ORIGIN="$(sed -n 's/.*"lock_origin": "\([a-z_]*\)".*/\1/p' "$LOCK_ORIGIN_FILE" | head -n 1)"
    [[ -n "$LOCK_ORIGIN" ]] || LOCK_ORIGIN="committed"
  fi
fi
log "uv.lock sha256=${lock_sha} origin=${LOCK_ORIGIN}"

run_uv 1 SYNC_FAILED \
  "if uv.lock is stale (pyproject.toml changed) regenerate it with 'uv lock' and commit it; pyspark builds from its sdist (2-5 min)" \
  -- sync --locked --extra dashboard --project "$REPO_DIR" --python "$PY_PIN"

CORE_PY="$REPO_DIR/.venv/bin/python"
[[ -x "$CORE_PY" ]] || fail 3 VENV_MISSING "${CORE_PY} not found after uv sync"
core_version="$("$CORE_PY" -I -c 'import sys; print("%d.%d.%d" % sys.version_info[:3])')"
[[ "$core_version" == "$PY_PIN" ]] || fail 3 PYTHON_VERSION_MISMATCH \
  ".venv python is ${core_version}, .python-version pins ${PY_PIN}" "delete ${REPO_DIR}/.venv and re-run bootstrap"
log "phase A done: core python ${core_version}; starting phase B (prepare_runtime.py)"

# ---------------------------------------------------------------- phase B (keeps the lock on fd 9)
PHASE_B_ARGS=(--workdir "$WORKDIR" --repo-dir "$REPO_DIR" --profile "$PROFILE" --lock-origin "$LOCK_ORIGIN" --lock-held)
if (( STRICT == 1 )); then PHASE_B_ARGS+=(--strict); fi
if (( OFFLINE == 1 )); then PHASE_B_ARGS+=(--offline); fi
if (( NO_TUNNEL == 1 )); then PHASE_B_ARGS+=(--no-tunnel); fi
trap - ERR
exec 1>&3 3>&-
exec "$CORE_PY" -I "$REPO_DIR/scripts/prepare_runtime.py" "${PHASE_B_ARGS[@]}"
