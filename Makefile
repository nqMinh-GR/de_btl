# TransitPulse MBTA — command interface (spec md §19.3). Requires GNU make >= 3.82 (recipes use `>` as prefix).
#
#   PROFILE=local-native  (default) native runner on Linux: scripts/*.py with .venv/bin/python, data in WORKDIR
#   PROFILE=colab-native  same native runner with the Colab work directory (/content/transitpulse-runtime)
#   PROFILE=local-docker  Docker Compose (NOT_RUN in this repository's verification; needs a committed uv.lock)
#
# Every command prints one JSON document on stdout (progress on stderr). No target deletes data: `stop` keeps
# volumes, checkpoints and raw data; `reset` is a dry run unless CONFIRM=<token printed by the dry run> is given.

.RECIPEPREFIX := >
SHELL := /bin/bash
.SHELLFLAGS := -eu -o pipefail -c
.DEFAULT_GOAL := help

PROFILE ?= local-native
REPO := $(CURDIR)
ifeq ($(PROFILE),colab-native)
WORKDIR ?= $(if $(TP_WORKDIR),$(TP_WORKDIR),/content/transitpulse-runtime)
else
WORKDIR ?= $(if $(TP_WORKDIR),$(TP_WORKDIR),$(REPO)/.runtime)
endif
PY ?= $(REPO)/.venv/bin/python
COMPOSE ?= docker compose

ROUTE_IDS ?=
DURATION_SECONDS ?= 7200
DEMO_DURATION ?= 600
RECORDING ?=
STATIC_ZIP ?=
SPEED ?= 10
SCENARIO ?=
REPETITIONS ?=
MAX_SECONDS ?=
RUN_ID ?=
OFFLINE ?=
CHECK_REALTIME ?= 1
ALL ?=
TARGETS ?= session
CONFIRM ?=
MODE ?= synthetic
SEED ?=
COLAB_WORKDIR ?= /content/transitpulse-runtime
API_URL ?= http://127.0.0.1:8000/api/v1
PYTEST_ARGS ?=

ifeq ($(filter $(PROFILE),local-native colab-native local-docker),)
$(error PROFILE must be local-native, colab-native or local-docker (got '$(PROFILE)'))
endif

yes = $(filter 1 yes true YES TRUE,$(1))
require = $(if $(strip $($(1))),,$(error $(1) is required: $(2)))
NATIVE_FLAGS = --profile $(PROFILE) --workdir $(WORKDIR)

.PHONY: help venv-check doctor build infra-up init source-check list-routes static-load start-live app-up \
        start-replay status test test-spark test-integration lint check golden-smoke benchmark report stop reset \
        colab-setup colab-start colab-status colab-stop

help:
> @echo "TransitPulse MBTA (PROFILE=$(PROFILE), WORKDIR=$(WORKDIR))"
> @echo ""
> @echo "  make doctor                                   prerequisites, versions, ports, memory/disk for PROFILE"
> @echo "  make build                                    native: bootstrap runtime (uv, Python, JDK, Kafka, Cassandra, jars)"
> @echo "                                                docker: build the app image (needs uv.lock)"
> @echo "  make infra-up                                 start Kafka + Cassandra (idempotent)"
> @echo "  make init                                     topics + CQL migrations (idempotent, never drops)"
> @echo "  make source-check [ROUTE_IDS=a,b] [OFFLINE=1] probe the MBTA sources -> reports/source_probe_report.json"
> @echo "  make list-routes [ZIP=path]                   bus routes of the GTFS ZIP (+ realtime vehicle counts)"
> @echo "  make static-load ROUTE_IDS=a,b [STATIC_ZIP=p] load the static scope (Spark batch)"
> @echo "  make start-live ROUTE_IDS=a,b DURATION_SECONDS=7200"
> @echo "  make start-replay RECORDING=<manifest> SPEED=10"
> @echo "  make app-up                                   dashboard (Streamlit) against the API"
> @echo "  make status                                   session and component status"
> @echo "  make test | test-spark | test-integration     unit+runtime | Spark | real-stack integration (IT01-IT16)"
> @echo "  make lint | check                             ruff | repository checks (notebooks, config, fixtures, compose)"
> @echo "  make golden-smoke                             golden fixture through the real stack vs expected.json"
> @echo "  make benchmark SCENARIO=replay_10x [RECORDING=...] [REPETITIONS=3] [MAX_SECONDS=...]"
> @echo "  make report RUN_ID=<run_id>                   FR09 batch report over committed curated batches"
> @echo "  make stop [ALL=1]                             stop the session (ALL=1: also Kafka/Cassandra); data kept"
> @echo "  make reset TARGETS=session [CONFIRM=token]    dry run by default; never outside WORKDIR"
> @echo "  make colab-setup | colab-start MODE=synthetic|replay|live | colab-status | colab-stop"

venv-check:
> @test -x "$(PY)" || { echo "missing $(PY): run 'make build' (bootstrap) first" >&2; exit 3; }

ifeq ($(PROFILE),local-docker)
# ------------------------------------------------------------------------------------------- local-docker (NOT_RUN)

doctor:
> docker version --format '{{.Server.Version}}' >/dev/null || { echo "Docker daemon not reachable" >&2; exit 3; }
> $(COMPOSE) version
> $(COMPOSE) config --quiet
> @test -f uv.lock || echo "WARNING: uv.lock is missing; 'make build PROFILE=local-docker' needs a committed lock" >&2

build:
> @test -f uv.lock || { echo "uv.lock is required for the image (uv sync --frozen): run 'uv lock' or commit the lock exported by the Colab bootstrap" >&2; exit 3; }
> $(COMPOSE) --profile static --profile tools --profile live --profile replay --profile app build

infra-up:
> $(COMPOSE) up -d --wait kafka cassandra

init:
> $(COMPOSE) run --rm init

source-check:
> $(COMPOSE) --profile tools run --rm --no-deps -T tools transitpulse source-check $(if $(ROUTE_IDS),--route-ids $(ROUTE_IDS)) $(if $(call yes,$(OFFLINE)),--offline)

list-routes:
> $(COMPOSE) --profile tools run --rm --no-deps -T tools transitpulse list-routes $(if $(ZIP),--zip $(ZIP),--download) $(if $(call yes,$(CHECK_REALTIME)),--check-realtime)

static-load:
> $(call require,ROUTE_IDS,verified bus route ids from 'make list-routes')
> ROUTE_IDS="$(ROUTE_IDS)" STATIC_ZIP="$(STATIC_ZIP)" $(COMPOSE) --profile static run --rm static-loader

start-live:
> $(call require,ROUTE_IDS,verified bus route ids from 'make list-routes')
> $(COMPOSE) up -d --wait kafka cassandra
> out="$$($(COMPOSE) --profile tools run --rm --no-deps -T tools transitpulse run-create --source-type live --route-ids "$(ROUTE_IDS)")"; \
  echo "$$out"; \
  run_id="$$(printf '%s\n' "$$out" | sed -n 's/.*"run_id": *"\([^"]*\)".*/\1/p' | head -n1)"; \
  test -n "$$run_id" || { echo "run-create returned no run_id" >&2; exit 1; }; \
  TP_RUN_ID="$$run_id" TP_DURATION_SECONDS="$(DURATION_SECONDS)" $(COMPOSE) --profile live up -d processor collector api dashboard; \
  echo "started run $$run_id; status: $(COMPOSE) --profile live ps; logs: $(COMPOSE) logs -f processor collector"

start-replay:
> $(call require,RECORDING,path of recording_manifest.json INSIDE the container such as /work/data/inputs/REC/recording_manifest.json)
> $(COMPOSE) up -d --wait kafka cassandra
> out="$$($(COMPOSE) --profile tools run --rm --no-deps -T tools transitpulse run-create --source-type replay --recording-manifest "$(RECORDING)" --speed "$(SPEED)" $(if $(ROUTE_IDS),--route-ids "$(ROUTE_IDS)"))"; \
  echo "$$out"; \
  run_id="$$(printf '%s\n' "$$out" | sed -n 's/.*"run_id": *"\([^"]*\)".*/\1/p' | head -n1)"; \
  session_id="$$(printf '%s\n' "$$out" | sed -n 's/.*"session_id": *"\([^"]*\)".*/\1/p' | head -n1)"; \
  test -n "$$run_id" && test -n "$$session_id" || { echo "run-create returned no run_id/session_id" >&2; exit 1; }; \
  TP_RUN_ID="$$run_id" TP_SESSION_ID="$$session_id" TP_RECORDING="$(RECORDING)" TP_SPEED="$(SPEED)" \
    $(COMPOSE) --profile replay up -d processor publisher api dashboard; \
  echo "started replay run $$run_id (session $$session_id)"

app-up:
> $(COMPOSE) --profile app up -d api dashboard
> @echo "dashboard: http://127.0.0.1:8501  API: http://127.0.0.1:8000/api/v1"

status:
> $(COMPOSE) --profile live --profile replay --profile app ps

benchmark:
> @echo "benchmarks run on the native runner (PROFILE=local-native or colab-native); local-docker is not supported" >&2; exit 6

test-integration:
> @echo "integration tests drive the native supervisor (runtime.json); run them with PROFILE=local-native" >&2; exit 6

golden-smoke:
> @echo "golden-smoke runs on the native runner (scripts/demo.py golden-smoke)" >&2; exit 6

report:
> $(call require,RUN_ID,the run id printed by start-live / start-replay)
> $(COMPOSE) --profile tools run --rm --no-deps -T tools transitpulse report --run-id "$(RUN_ID)"

stop:
> $(COMPOSE) --profile live --profile replay --profile app --profile static --profile tools stop
> @echo "services stopped; named volumes (raw, checkpoints, curated, kafka, cassandra, work) are kept"

reset:
> @echo "local-docker reset is manual by design: review 'docker volume ls --filter label=com.docker.compose.project=transitpulse'"
> @echo "and remove a volume explicitly ('docker volume rm <name>') only if you accept losing that data."
> $(COMPOSE) config --volumes

else
# ------------------------------------------------------------------------------------------- native (local / Colab)

doctor: venv-check
> $(PY) scripts/doctor.py $(NATIVE_FLAGS)

build:
> bash scripts/bootstrap_colab.sh --workdir "$(WORKDIR)" --repo-dir "$(REPO)" --profile $(PROFILE)

infra-up: venv-check
> $(PY) scripts/infra.py start $(NATIVE_FLAGS)

init: venv-check
> $(PY) scripts/infra.py init $(NATIVE_FLAGS)

source-check: venv-check
> $(PY) -m transitpulse.ingestion source-check $(NATIVE_FLAGS) $(if $(ROUTE_IDS),--route-ids $(ROUTE_IDS)) $(if $(call yes,$(OFFLINE)),--offline)

list-routes: venv-check
> $(PY) -m transitpulse.static list-routes $(NATIVE_FLAGS) $(if $(ZIP),--zip $(ZIP),--download) $(if $(call yes,$(CHECK_REALTIME)),--check-realtime)

static-load: venv-check
> $(call require,ROUTE_IDS,verified bus route ids from 'make list-routes')
> $(PY) -m transitpulse.static load $(NATIVE_FLAGS) $(if $(STATIC_ZIP),--zip $(STATIC_ZIP),--download) --route-ids $(ROUTE_IDS)

start-live: venv-check
> $(call require,ROUTE_IDS,verified bus route ids from 'make list-routes')
> $(PY) scripts/demo.py start --mode live --route-ids $(ROUTE_IDS) --duration-seconds $(DURATION_SECONDS) --detach $(NATIVE_FLAGS)

start-replay: venv-check
> $(call require,RECORDING,path of the recording_manifest.json to replay)
> $(PY) scripts/demo.py start --mode replay --recording-manifest $(RECORDING) --speed $(SPEED) $(if $(STATIC_ZIP),--static-zip $(STATIC_ZIP)) --duration-seconds $(DEMO_DURATION) --detach $(NATIVE_FLAGS)

app-up: venv-check
> @echo "Streamlit dashboard on http://127.0.0.1:8501 (API $(API_URL)); Ctrl-C stops the dashboard only" >&2
> cd app && TP_API_URL=$(API_URL) TP_WORKDIR=$(abspath $(WORKDIR)) TP_PROFILE=$(PROFILE) $(PY) -m streamlit run streamlit_app.py \
  --server.address 127.0.0.1 --server.port 8501 --server.headless true --browser.gatherUsageStats false

status: venv-check
> $(PY) scripts/demo.py status $(NATIVE_FLAGS)

test-integration: venv-check
> mkdir -p "$(WORKDIR)/reports/integration"
> TP_WORKDIR=$(WORKDIR) TP_PROFILE=$(PROFILE) $(PY) -m pytest tests/integration -m integration -v \
  --junitxml="$(WORKDIR)/reports/integration/junit.xml" $(PYTEST_ARGS)

golden-smoke: venv-check
> $(PY) scripts/demo.py golden-smoke $(NATIVE_FLAGS)

benchmark: venv-check
> $(call require,SCENARIO,a scenario of benchmarks/scenarios.yaml such as replay_10x or stress_100)
> $(PY) benchmarks/run_benchmark.py --scenario $(SCENARIO) $(NATIVE_FLAGS) $(if $(RECORDING),--recording-manifest $(RECORDING)) $(if $(REPETITIONS),--repetitions $(REPETITIONS)) $(if $(MAX_SECONDS),--max-seconds $(MAX_SECONDS))

report: venv-check
> $(call require,RUN_ID,the run id printed by start-live / start-replay)
> $(PY) -m transitpulse.processing.report_job $(NATIVE_FLAGS) --run-id $(RUN_ID)

stop: venv-check
> $(PY) scripts/demo.py stop $(if $(call yes,$(ALL)),--all) $(NATIVE_FLAGS)

reset: venv-check
> $(PY) scripts/demo.py reset --targets $(TARGETS) $(if $(CONFIRM),--confirm $(CONFIRM)) $(NATIVE_FLAGS)

endif

# ------------------------------------------------------------------------------------------- profile-independent

# Unit + runtime tests (no services); ct_case results go to $(WORKDIR)/reports/ct_report.json.
test: venv-check
> TP_WORKDIR=$(WORKDIR) PYTHONPATH=tests $(PY) -m pytest -p plugins.test_report tests/unit tests/runtime \
  -m "unit or runtime" -q $(PYTEST_ARGS)

test-spark: venv-check
> $(PY) -m pytest tests/spark -m spark -q $(PYTEST_ARGS)

lint: venv-check
> $(PY) -m ruff check .

check: venv-check
> $(PY) -m transitpulse.verify.repo_checks

# Colab wrappers (the notebook calls the same scripts; these are for a Colab terminal).
colab-setup:
> bash scripts/bootstrap_colab.sh --workdir "$(COLAB_WORKDIR)" --repo-dir "$(REPO)" --profile colab-native
> $(PY) scripts/infra.py start --profile colab-native --workdir "$(COLAB_WORKDIR)"
> $(PY) scripts/infra.py init --profile colab-native --workdir "$(COLAB_WORKDIR)"

colab-start: venv-check
> $(if $(filter live,$(MODE)),$(call require,ROUTE_IDS,live mode needs verified route ids))
> $(if $(filter replay,$(MODE)),$(call require,RECORDING,replay mode needs RECORDING=<recording_manifest.json>))
> $(PY) scripts/demo.py start --mode $(MODE) --detach --profile colab-native --workdir "$(COLAB_WORKDIR)" \
  $(if $(filter live,$(MODE)),--route-ids $(ROUTE_IDS) --duration-seconds $(DURATION_SECONDS),--duration-seconds $(DEMO_DURATION)) \
  $(if $(filter replay,$(MODE)),--recording-manifest $(RECORDING) --speed $(SPEED)) $(if $(SEED),--seed $(SEED))

colab-status: venv-check
> $(PY) scripts/demo.py status --profile colab-native --workdir "$(COLAB_WORKDIR)"

colab-stop: venv-check
> $(PY) scripts/demo.py stop --all --profile colab-native --workdir "$(COLAB_WORKDIR)"

# React Frontend commands
react-dev:
> cd frontend && npm run dev

react-build:
> cd frontend && npm run build

