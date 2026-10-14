"""Safe tar extraction: traversal, absolute paths, escaping links, devices and foreign top dirs are rejected."""

from __future__ import annotations

import io
import tarfile
from pathlib import Path

import pytest

from transitpulse.runtime.artifacts import ArtifactError, check_tar_members, safe_extract

TOP = "kafka_2.13-4.2.2"


def _tar(path: Path, members: list[tuple[tarfile.TarInfo, bytes | None]]) -> Path:
    with tarfile.open(path, "w:gz") as tf:
        for info, data in members:
            if data is not None:
                info.size = len(data)
                tf.addfile(info, io.BytesIO(data))
            else:
                tf.addfile(info)
    return path


def _file(name: str, data: bytes = b"x", mode: int = 0o644) -> tuple[tarfile.TarInfo, bytes]:
    info = tarfile.TarInfo(name)
    info.mode = mode
    return info, data


def _dir(name: str) -> tuple[tarfile.TarInfo, None]:
    info = tarfile.TarInfo(name)
    info.type = tarfile.DIRTYPE
    info.mode = 0o755
    return info, None


def _link(name: str, target: str, kind: bytes = tarfile.SYMTYPE) -> tuple[tarfile.TarInfo, None]:
    info = tarfile.TarInfo(name)
    info.type = kind
    info.linkname = target
    return info, None


def test_valid_archive_extracts_and_keeps_exec_bit(tmp_path: Path) -> None:
    archive = _tar(tmp_path / "ok.tgz", [
        _dir(TOP), _dir(f"{TOP}/bin"), _file(f"{TOP}/bin/kafka-server-start.sh", b"#!/bin/bash\n", 0o755),
        _file(f"{TOP}/libs/kafka_2.13-4.2.2.jar", b"jar"),
        _link(f"{TOP}/bin/alias.sh", "kafka-server-start.sh"),
    ])
    dest = safe_extract(archive, TOP, tmp_path / "tools" / "kafka", trash_dir=tmp_path / "trash")
    assert dest == tmp_path / "tools" / "kafka" / TOP
    script = dest / "bin" / "kafka-server-start.sh"
    assert script.read_bytes() == b"#!/bin/bash\n"
    assert script.stat().st_mode & 0o100
    assert (dest / "libs" / "kafka_2.13-4.2.2.jar").is_file()
    assert not any(p.name.startswith(".staging-") for p in (tmp_path / "tools" / "kafka").iterdir())


def test_existing_destination_is_moved_to_trash_not_deleted(tmp_path: Path) -> None:
    parent = tmp_path / "tools" / "kafka"
    (parent / TOP).mkdir(parents=True)
    (parent / TOP / "partial.txt").write_text("crashed install")
    archive = _tar(tmp_path / "ok.tgz", [_dir(TOP), _file(f"{TOP}/LICENSE", b"license")])
    safe_extract(archive, TOP, parent, trash_dir=tmp_path / "trash")
    trashed = list((tmp_path / "trash").iterdir())
    assert len(trashed) == 1 and (trashed[0] / "partial.txt").read_text() == "crashed install"
    assert (parent / TOP / "LICENSE").read_text() == "license"


@pytest.mark.parametrize("members", [
    [_file("../evil.sh")],
    [_file(f"{TOP}/../../evil.sh")],
    [_file("/etc/passwd")],
    [_file("other_top/file.txt")],
    [_link(f"{TOP}/bin/escape", "../../../etc/passwd")],
    [_link(f"{TOP}/bin/abs", "/etc/passwd")],
    [_link(f"{TOP}/bin/hard", "/etc/shadow", tarfile.LNKTYPE)],
])
def test_unsafe_members_rejected(tmp_path: Path, members: list) -> None:
    archive = _tar(tmp_path / "bad.tgz", [_dir(TOP), *members])
    with pytest.raises(ArtifactError) as exc:
        safe_extract(archive, TOP, tmp_path / "dest", trash_dir=tmp_path / "trash")
    assert exc.value.code == "UNSAFE_ARCHIVE"
    assert not (tmp_path / "dest" / TOP).exists()
    assert not (tmp_path / "etc").exists()


def test_device_and_fifo_rejected() -> None:
    dev = tarfile.TarInfo(f"{TOP}/dev")
    dev.type = tarfile.CHRTYPE
    fifo = tarfile.TarInfo(f"{TOP}/fifo")
    fifo.type = tarfile.FIFOTYPE
    for member in (dev, fifo):
        with pytest.raises(ArtifactError):
            check_tar_members([member], TOP)


def test_member_count_and_size_caps() -> None:
    members = [tarfile.TarInfo(f"{TOP}/f{i}") for i in range(5)]
    with pytest.raises(ArtifactError):
        check_tar_members(members, TOP, max_members=3)
    big = tarfile.TarInfo(f"{TOP}/big")
    big.size = 10
    with pytest.raises(ArtifactError):
        check_tar_members([big], TOP, max_total_bytes=5)


def test_dot_slash_prefix_is_accepted() -> None:
    assert check_tar_members([tarfile.TarInfo(f"./{TOP}/README")], TOP) == 1
