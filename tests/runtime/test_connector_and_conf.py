"""Connector closure classification (pure) and Cassandra conf merge / root handling."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from transitpulse.config import load_config
from transitpulse.runtime.cassandra_native import CassandraError, cassandra_env, root_flag_needed
from transitpulse.runtime.configs import cassandra_conf_files, merge_cassandra_yaml
from transitpulse.runtime.connector import classify_jars, index_ivy_cache, parse_probe_output
from transitpulse.runtime.manifest import runtime_from_dict
from transitpulse.runtime.paths import WorkPaths

INDEX = {
    "org.apache.spark_spark-sql-kafka-0-10_2.12-3.5.7.jar": ("org.apache.spark", "spark-sql-kafka-0-10_2.12", "3.5.7"),
    "org.apache.spark_spark-token-provider-kafka-0-10_2.12-3.5.7.jar":
        ("org.apache.spark", "spark-token-provider-kafka-0-10_2.12", "3.5.7"),
    "org.apache.kafka_kafka-clients-3.4.1.jar": ("org.apache.kafka", "kafka-clients", "3.4.1"),
    "org.apache.commons_commons-pool2-2.11.1.jar": ("org.apache.commons", "commons-pool2", "2.11.1"),
    "org.lz4_lz4-java-1.8.0.jar": ("org.lz4", "lz4-java", "1.8.0"),
    "org.slf4j_slf4j-api-2.0.6.jar": ("org.slf4j", "slf4j-api", "2.0.6"),
}
SPARK_JARS = {"lz4-java-1.8.0.jar", "slf4j-api-2.0.7.jar", "spark-core_2.12-3.5.7.jar"}


def test_classify_keeps_connector_and_drops_spark_provided() -> None:
    res = classify_jars(sorted(INDEX), INDEX, SPARK_JARS, spark_version="3.5.7")
    assert not res["errors"]
    kept = {r["module"] for r in res["keep"]}
    assert kept == {"spark-sql-kafka-0-10_2.12", "spark-token-provider-kafka-0-10_2.12", "kafka-clients",
                    "commons-pool2"}
    provided = {r["module"]: r for r in res["provided_by_spark"]}
    assert set(provided) == {"lz4-java", "slf4j-api"}
    assert provided["slf4j-api"].get("version_differs") is True
    assert any("slf4j-api" in w for w in res["warnings"])


def test_classify_rejects_scala_213_and_wrong_spark_version() -> None:
    bad = dict(INDEX)
    bad["org.apache.spark_spark-sql-kafka-0-10_2.13-3.5.7.jar"] = ("org.apache.spark", "spark-sql-kafka-0-10_2.13", "3.5.7")
    bad["org.apache.spark_spark-tags_2.12-3.5.1.jar"] = ("org.apache.spark", "spark-tags_2.12", "3.5.1")
    res = classify_jars(sorted(bad), bad, SPARK_JARS, spark_version="3.5.7")
    assert any("2.13" in e for e in res["errors"])
    assert any("3.5.1" in e for e in res["errors"])


def test_classify_requires_kafka_clients() -> None:
    idx = {k: v for k, v in INDEX.items() if v[1] != "kafka-clients"}
    res = classify_jars(sorted(idx), idx, SPARK_JARS, spark_version="3.5.7")
    assert any("kafka-clients" in e for e in res["errors"])


def test_index_ivy_cache_layout(tmp_path: Path) -> None:
    jar = tmp_path / "cache" / "org.apache.kafka" / "kafka-clients" / "jars" / "kafka-clients-3.4.1.jar"
    jar.parent.mkdir(parents=True)
    jar.write_bytes(b"jar")
    assert index_ivy_cache(tmp_path) == {"org.apache.kafka_kafka-clients-3.4.1.jar":
                                         ("org.apache.kafka", "kafka-clients", "3.4.1")}


def test_index_ivy_cache_includes_bundle_packaging(tmp_path: Path) -> None:
    """kafka-clients pulls org.xerial.snappy:snappy-java (packaging bundle) -> Ivy stores it under bundles/."""
    layout = {
        "org.apache.kafka/kafka-clients/jars/kafka-clients-3.4.1.jar": ("org.apache.kafka", "kafka-clients", "3.4.1"),
        "org.xerial.snappy/snappy-java/bundles/snappy-java-1.1.10.5.jar":
            ("org.xerial.snappy", "snappy-java", "1.1.10.5"),
        "org.apache.spark/spark-sql-kafka-0-10_2.12/jars/spark-sql-kafka-0-10_2.12-3.5.7.jar":
            ("org.apache.spark", "spark-sql-kafka-0-10_2.12", "3.5.7"),
        "org.apache.kafka/kafka-clients/sources/kafka-clients-3.4.1-sources.jar": None,
    }
    for rel in layout:
        p = tmp_path / "cache" / rel
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(b"jar")
    index = index_ivy_cache(tmp_path)
    assert index["org.xerial.snappy_snappy-java-1.1.10.5.jar"] == ("org.xerial.snappy", "snappy-java", "1.1.10.5")
    assert len(index) == 3
    retrieved = ["org.apache.kafka_kafka-clients-3.4.1.jar", "org.xerial.snappy_snappy-java-1.1.10.5.jar",
                 "org.apache.spark_spark-sql-kafka-0-10_2.12-3.5.7.jar"]
    res = classify_jars(retrieved, index, {"snappy-java-1.1.10.5.jar"}, spark_version="3.5.7")
    assert not res["errors"]
    assert [p["module"] for p in res["provided_by_spark"]] == ["snappy-java"]


def test_probe_output_marker_parsing() -> None:
    out = 'noise\nTP_PROBE_RESULT={"ok": true, "spark_version": "3.5.7"}\n'
    assert parse_probe_output(out) == {"ok": True, "spark_version": "3.5.7"}
    assert parse_probe_output("no marker") is None


def test_merge_removes_legacy_aliases() -> None:
    base = {"key_cache_size_in_mb": None, "memtable_heap_space_in_mb": 2048, "cluster_name": "Test Cluster",
            "num_tokens": 16}
    merged = merge_cassandra_yaml(base, {"key_cache_size": "32MiB", "memtable_heap_space": "256MiB",
                                         "cluster_name": "TransitPulseColab"})
    assert "key_cache_size_in_mb" not in merged and "memtable_heap_space_in_mb" not in merged
    assert merged["cluster_name"] == "TransitPulseColab" and merged["num_tokens"] == 16


def test_cassandra_conf_rendering_keeps_paths_under_workdir(tmp_path: Path, repo_dir: Path) -> None:
    home = tmp_path / "apache-cassandra-5.0.9"
    conf = home / "conf"
    (conf / "triggers").mkdir(parents=True)
    (conf / "cassandra.yaml").write_text("cluster_name: 'Test Cluster'\nnum_tokens: 16\n"
                                         "data_file_directories:\n  - /var/lib/cassandra/data\n")
    (conf / "cassandra-env.sh").write_text("# env\n")
    (conf / "cassandra_latest.yaml").write_text("x: 1\n")
    (conf / "cassandra-topology.properties.example").write_text("x\n")
    (conf / "triggers" / "README.txt").write_text("triggers\n")
    paths = WorkPaths.from_workdir(tmp_path / "w")
    files = cassandra_conf_files(home, (repo_dir / "deploy/native/cassandra/overrides.yaml").read_text(), paths)
    assert "cassandra_latest.yaml" not in files and not any(f.endswith(".example") for f in files)
    assert "triggers/README.txt" in files and "cassandra-env.sh" in files
    doc = yaml.safe_load(files["cassandra.yaml"])
    root = str(paths.cassandra_root)
    assert doc["data_file_directories"] == [f"{root}/data"]
    for key in ("commitlog_directory", "saved_caches_directory", "hints_directory", "cdc_raw_directory"):
        assert doc[key].startswith(root)
    assert doc["listen_address"] == "127.0.0.1" and doc["native_transport_port"] == 9042
    assert doc["num_tokens"] == 16


def _runtime(tmp_path: Path):
    return runtime_from_dict({
        "workdir": str(tmp_path), "repo_dir": "/r", "python": {"core_python": "/r/.venv/bin/python"},
        "java": {"java_home": "/jdk"}, "kafka": {"home": "/k"}, "cassandra": {"home": "/c", "conf_dir": "/cc"},
        "spark": {"spark_home": "/s", "connector_jars": []}, "env": {"JAVA_HOME": "/jdk", "PATH": "/jdk/bin:/usr/bin"},
    })


def test_root_flag_rules(tmp_path: Path) -> None:
    colab = load_config("colab-native", workdir=tmp_path)
    local = load_config("local-native", workdir=tmp_path)
    assert root_flag_needed(colab, is_root=True) is True
    assert root_flag_needed(colab, is_root=False) is False
    assert root_flag_needed(local, is_root=False) is False
    with pytest.raises(CassandraError):
        root_flag_needed(local, is_root=True)


def test_cassandra_env_sets_heap_without_heap_newsize(tmp_path: Path) -> None:
    cfg = load_config("colab-native", workdir=tmp_path)
    env = cassandra_env(_runtime(tmp_path), WorkPaths.from_workdir(tmp_path), cfg)
    assert env["MAX_HEAP_SIZE"] == "4G" and env["MAX_DIRECT_MEMORY_SIZE"] == "2G"
    assert "HEAP_NEWSIZE" not in env
    assert "@OWNER_TOKEN@" in env["JVM_EXTRA_OPTS"]
    assert env["CASSANDRA_CONF"].startswith(str(WorkPaths.from_workdir(tmp_path).root))
