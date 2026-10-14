# syntax=docker/dockerfile:1.7
# TransitPulse MBTA application image for the Compose profile `local-docker`.
#
# STATUS: NOT_RUN in this repository's verification — the build environment had no Docker daemon access, so this
# image has never been built here and no digest is recorded. The verified path is the native runner (Colab/Linux).
#
# Requirements:
#   * a committed `uv.lock` (`uv sync --frozen` refuses to run without it): run `uv lock` once, or commit the lock
#     exported by the first Colab bootstrap (`lock_origin=generated_on_colab`, see docs/versions.md);
#   * network access at build time (uv, Python 3.11.17, wheels, Maven Central jars).
# Pinned inputs: uv 0.12.23 (official image tag), Python 3.11.17 (uv-managed), Temurin 17.0.20.1_1 JDK base,
# Spark Kafka connector 3.5.7 for Scala 2.12 with its direct closure (verified against Maven Central .sha1).

ARG UV_VERSION=0.12.23
FROM ghcr.io/astral-sh/uv:${UV_VERSION} AS uv

# Pinned JDK tag (Temurin 17.0.20.1+1 on Ubuntu jammy); the image digest is recorded at the first build
# (docs/versions.md) because it could not be resolved without Docker access.
FROM eclipse-temurin:17.0.20.1_1-jdk-jammy AS app

ARG PYTHON_VERSION=3.11.17
ARG SPARK_KAFKA_VERSION=3.5.7
ARG KAFKA_CLIENTS_VERSION=3.4.1
ARG COMMONS_POOL2_VERSION=2.11.1

ENV DEBIAN_FRONTEND=noninteractive \
    TZ=UTC \
    LANG=C.UTF-8 \
    UV_PYTHON_INSTALL_DIR=/opt/uv-python \
    UV_PYTHON_PREFERENCE=only-managed \
    UV_PROJECT_ENVIRONMENT=/opt/venv \
    UV_LINK_MODE=copy \
    UV_COMPILE_BYTECODE=1 \
    UV_NO_CACHE=1 \
    PATH=/opt/venv/bin:${PATH} \
    PYSPARK_PYTHON=/opt/venv/bin/python \
    PYSPARK_DRIVER_PYTHON=/opt/venv/bin/python \
    TP_PROFILE=local-docker \
    TP_WORKDIR=/work \
    TP_REPO_DIR=/app \
    TP_SPARK_JARS_DIR=/opt/spark-jars

RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates curl tini \
 && rm -rf /var/lib/apt/lists/*

COPY --from=uv /uv /uvx /usr/local/bin/
RUN uv python install "${PYTHON_VERSION}"

# Spark Kafka connector closure (jars that pyspark does not ship), verified against Maven Central sha1 files.
RUN set -eux; \
    mkdir -p /opt/spark-jars; \
    cd /opt/spark-jars; \
    base=https://repo1.maven.org/maven2; \
    for rel in \
      "org/apache/spark/spark-sql-kafka-0-10_2.12/${SPARK_KAFKA_VERSION}/spark-sql-kafka-0-10_2.12-${SPARK_KAFKA_VERSION}.jar" \
      "org/apache/spark/spark-token-provider-kafka-0-10_2.12/${SPARK_KAFKA_VERSION}/spark-token-provider-kafka-0-10_2.12-${SPARK_KAFKA_VERSION}.jar" \
      "org/apache/kafka/kafka-clients/${KAFKA_CLIENTS_VERSION}/kafka-clients-${KAFKA_CLIENTS_VERSION}.jar" \
      "org/apache/commons/commons-pool2/${COMMONS_POOL2_VERSION}/commons-pool2-${COMMONS_POOL2_VERSION}.jar"; \
    do \
      f="$(basename "$rel")"; \
      curl -fsSL --retry 3 -o "$f" "$base/$rel"; \
      curl -fsSL --retry 3 -o "$f.sha1" "$base/$rel.sha1"; \
      echo "$(cut -c1-40 "$f.sha1")  $f" | sha1sum -c -; \
      rm -f "$f.sha1"; \
    done; \
    ls -l /opt/spark-jars

WORKDIR /app

# Dependencies first (layer cache), then the project itself. --frozen: the lock is used as committed, never updated.
COPY pyproject.toml uv.lock .python-version README.md ./
RUN uv sync --frozen --no-install-project --no-dev --extra dashboard --python "${PYTHON_VERSION}"

COPY src ./src
COPY config ./config
COPY scripts ./scripts
COPY deploy ./deploy
COPY app ./app
COPY benchmarks ./benchmarks
COPY tests/fixtures ./tests/fixtures
COPY docker ./docker
RUN uv sync --frozen --no-dev --extra dashboard --python "${PYTHON_VERSION}" \
 && /opt/venv/bin/python -c "import pyspark, confluent_kafka, cassandra, fastapi, streamlit; print(pyspark.__version__)"

# Unprivileged runtime user; the work directory and its volume mount points are owned by it so that fresh named
# volumes inherit the ownership.
RUN groupadd --system --gid 10001 tp \
 && useradd --system --uid 10001 --gid tp --home-dir /work --shell /usr/sbin/nologin tp \
 && mkdir -p /work/data/raw /work/data/curated /work/data/recordings /work/data/static /work/data/inputs \
             /work/checkpoints /work/runtime /work/runs /work/state /work/sessions /work/processes /work/faults \
             /work/metrics /work/logs /work/reports /work/artifacts /work/tmp /work/spark-local \
 && chown -R tp:tp /work

USER tp
WORKDIR /app
ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["transitpulse", "--help"]
