"""Kafka format-once decision and the rendered server.properties (no JVM involved)."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from transitpulse.config import load_config
from transitpulse.runtime.configs import ConfigRenderError, kafka_template_values, render_template
from transitpulse.runtime.kafka_native import KafkaError, decide_format, read_properties
from transitpulse.runtime.paths import WorkPaths

META = "#\n#Tue Oct 06 10:15:00 UTC 2026\nnode.id=1\ndirectory.id=AbCdEfGhIjKlMnOpQrStUw\nversion=1\ncluster.id={cid}\n"


def _cluster_json(path: Path, cid: str) -> None:
    path.write_text(json.dumps({"cluster_id": cid, "node_id": 1}))


def test_empty_dir_formats(tmp_path: Path) -> None:
    data = tmp_path / "data"
    data.mkdir()
    d = decide_format(data, tmp_path / "cluster.json")
    assert d.action == "FORMAT" and d.cluster_id is None


def test_empty_dir_reuses_recorded_cluster_id_for_a_retry(tmp_path: Path) -> None:
    data = tmp_path / "data"
    data.mkdir()
    _cluster_json(tmp_path / "cluster.json", "MkU3OEVBNTcwNTJENDM2Qg")
    d = decide_format(data, tmp_path / "cluster.json")
    assert d.action == "FORMAT" and d.cluster_id == "MkU3OEVBNTcwNTJENDM2Qg"


def test_formatted_with_matching_id_is_reused_never_reformatted(tmp_path: Path) -> None:
    data = tmp_path / "data"
    data.mkdir()
    (data / "meta.properties").write_text(META.format(cid="abc123abc123abc123abc1"))
    _cluster_json(tmp_path / "cluster.json", "abc123abc123abc123abc1")
    assert decide_format(data, tmp_path / "cluster.json").action == "REUSE"


def test_meta_without_cluster_json_is_adopted(tmp_path: Path) -> None:
    data = tmp_path / "data"
    data.mkdir()
    (data / "meta.properties").write_text(META.format(cid="abc123abc123abc123abc1"))
    d = decide_format(data, tmp_path / "cluster.json")
    assert d.action == "ADOPT" and d.cluster_id == "abc123abc123abc123abc1"


def test_cluster_id_mismatch_fails(tmp_path: Path) -> None:
    data = tmp_path / "data"
    data.mkdir()
    (data / "meta.properties").write_text(META.format(cid="abc123abc123abc123abc1"))
    _cluster_json(tmp_path / "cluster.json", "zzz999zzz999zzz999zzz9")
    with pytest.raises(KafkaError) as exc:
        decide_format(data, tmp_path / "cluster.json")
    assert exc.value.code == "KAFKA_METADATA_MISMATCH"
    assert "reset" in (exc.value.hint or "")


def test_wrong_node_id_fails(tmp_path: Path) -> None:
    data = tmp_path / "data"
    data.mkdir()
    (data / "meta.properties").write_text(META.format(cid="x" * 22).replace("node.id=1", "node.id=2"))
    with pytest.raises(KafkaError):
        decide_format(data, tmp_path / "cluster.json")


def test_unknown_content_without_meta_fails(tmp_path: Path) -> None:
    data = tmp_path / "data"
    data.mkdir()
    (data / "mbta.control.v1-0").mkdir()
    with pytest.raises(KafkaError) as exc:
        decide_format(data, tmp_path / "cluster.json")
    assert exc.value.code == "KAFKA_DIR_UNKNOWN_CONTENT"


def test_read_properties_ignores_comments(tmp_path: Path) -> None:
    p = tmp_path / "meta.properties"
    p.write_text(META.format(cid="c" * 22))
    props = read_properties(p)
    assert props["node.id"] == "1" and props["cluster.id"] == "c" * 22 and "#" not in "".join(props)


def test_rendered_server_properties_has_required_kraft_settings(repo_dir: Path, tmp_path: Path) -> None:
    cfg = load_config("colab-native", workdir=tmp_path / "w")
    paths = WorkPaths.from_workdir(tmp_path / "w")
    tmpl = (repo_dir / "deploy" / "native" / "kafka" / "server.properties.tmpl").read_text()
    out = render_template(tmpl, kafka_template_values(cfg, paths))
    props = dict(line.split("=", 1) for line in out.splitlines() if line and not line.startswith("#"))
    assert props["process.roles"] == "broker,controller"
    assert props["node.id"] == "1"
    assert props["controller.quorum.bootstrap.servers"] == "127.0.0.1:9093"
    assert props["listeners"] == "PLAINTEXT://127.0.0.1:9092,CONTROLLER://127.0.0.1:9093"
    assert props["advertised.listeners"] == "PLAINTEXT://127.0.0.1:9092,CONTROLLER://127.0.0.1:9093"
    assert props["auto.create.topics.enable"] == "false"
    assert props["log.dirs"] == str(paths.kafka_data)
    assert props["message.max.bytes"] == str(cfg.kafka.topic_max_message_bytes)
    assert int(props["message.max.bytes"]) > cfg.kafka.max_snapshot_bytes
    assert props["log.retention.ms"] == str(int(cfg.kafka.retention_hours * 3600 * 1000))
    assert "controller.quorum.voters" not in props


def test_render_template_rejects_unknown_placeholders() -> None:
    with pytest.raises(ConfigRenderError):
        render_template("a={{MISSING}}", {})
    assert render_template("a={{X}}", {"X": "1"}) == "a=1"
