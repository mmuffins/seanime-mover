import io
import logging
import os
import time
from pathlib import Path

import pytest

import mover
from downloader_clean_queue import CleanQueueStats
from mover import READY_AGE_SECONDS, clean_queue, scan_once


@pytest.fixture
def mover_env(tmp_path: Path):
    source = tmp_path / "source"
    dest = tmp_path / "dest"
    source.mkdir()
    dest.mkdir()

    log_stream = io.StringIO()
    logger = logging.getLogger(f"test_mover_{tmp_path.name}")
    logger.handlers.clear()
    logger.setLevel(logging.INFO)
    logger.propagate = False

    handler = logging.StreamHandler(log_stream)
    handler.setFormatter(logging.Formatter("%(levelname)s %(message)s"))
    logger.addHandler(handler)

    yield {
        "source": source,
        "dest": dest,
        "logger": logger,
        "log_stream": log_stream,
    }

    logger.handlers.clear()


def write_file(source: Path, relative_path: str, content: bytes, age_seconds: int = READY_AGE_SECONDS + 5) -> Path:
    path = source / relative_path
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)
    file_time = time.time() - age_seconds
    path.touch()
    os.utime(path, (file_time, file_time))
    return path


def test_moves_nested_file_and_creates_placeholder(mover_env) -> None:
    source_path = write_file(mover_env["source"], "a/b/example.txt", b"payload")

    stats = scan_once(mover_env["source"], mover_env["dest"], mover_env["logger"], now=time.time())

    assert stats.moved == 1
    assert (mover_env["dest"] / "example.txt").exists()
    assert (mover_env["dest"] / "example.txt").read_bytes() == b"payload"
    assert source_path.exists()
    assert source_path.stat().st_size == 0


def test_flattens_source_structure(mover_env) -> None:
    write_file(mover_env["source"], "one/two/three/data.bin", b"abc")

    scan_once(mover_env["source"], mover_env["dest"], mover_env["logger"], now=time.time())

    assert (mover_env["dest"] / "data.bin").exists()
    assert not (mover_env["dest"] / "one").exists()


def test_skips_zero_byte_files(mover_env) -> None:
    placeholder = write_file(mover_env["source"], "nested/placeholder.txt", b"")

    stats = scan_once(mover_env["source"], mover_env["dest"], mover_env["logger"], now=time.time())

    assert stats.skipped_too_small == 1
    assert not (mover_env["dest"] / "placeholder.txt").exists()
    assert placeholder.stat().st_size == 0


def test_skips_files_at_or_below_minimum_size(mover_env, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(mover, "MIN_FILE_SIZE_BYTES", 1)
    small_file = write_file(mover_env["source"], "nested/small.txt", b"a")
    large_file = write_file(mover_env["source"], "nested/large.txt", b"ab")

    stats = scan_once(mover_env["source"], mover_env["dest"], mover_env["logger"], now=time.time())

    assert stats.skipped_too_small == 1
    assert small_file.exists()
    assert small_file.read_bytes() == b"a"
    assert (mover_env["dest"] / "large.txt").read_bytes() == b"ab"


def test_skips_tmp_directories(mover_env) -> None:
    tmp_file = write_file(mover_env["source"], "a/.tmp123/deeper/file.txt", b"payload")

    stats = scan_once(mover_env["source"], mover_env["dest"], mover_env["logger"], now=time.time())

    assert stats.skipped_tmp_dirs == 1
    assert tmp_file.exists()
    assert not (mover_env["dest"] / "file.txt").exists()


def test_skips_recent_files(mover_env) -> None:
    recent_file = write_file(mover_env["source"], "recent/file.txt", b"payload", age_seconds=5)

    stats = scan_once(mover_env["source"], mover_env["dest"], mover_env["logger"], now=time.time())

    assert stats.skipped_too_recent == 1
    assert recent_file.exists()
    assert not (mover_env["dest"] / "file.txt").exists()


def test_collision_logs_error_and_leaves_source_untouched(mover_env) -> None:
    source_path = write_file(mover_env["source"], "a/b/shared.txt", b"source-data")
    (mover_env["dest"] / "shared.txt").write_bytes(b"dest-data")

    stats = scan_once(mover_env["source"], mover_env["dest"], mover_env["logger"], now=time.time())

    assert stats.collisions == 1
    assert source_path.read_bytes() == b"source-data"
    assert (mover_env["dest"] / "shared.txt").read_bytes() == b"dest-data"
    assert "Destination collision" in mover_env["log_stream"].getvalue()


def test_placeholder_is_not_moved_again(mover_env) -> None:
    source_path = write_file(mover_env["source"], "a/b/repeat.txt", b"payload")

    first_stats = scan_once(mover_env["source"], mover_env["dest"], mover_env["logger"], now=time.time())
    second_stats = scan_once(
        mover_env["source"],
        mover_env["dest"],
        mover_env["logger"],
        now=time.time() + READY_AGE_SECONDS + 5,
    )

    assert first_stats.moved == 1
    assert second_stats.moved == 0
    assert second_stats.skipped_too_small == 1
    assert source_path.stat().st_size == 0
    assert (mover_env["dest"] / "repeat.txt").read_bytes() == b"payload"


def test_clean_queue_runs_downloader_cleanup(monkeypatch: pytest.MonkeyPatch) -> None:
    logger = logging.getLogger("test_clean_queue")
    logger.handlers.clear()
    logger.setLevel(logging.INFO)
    logger.propagate = False

    calls: list[object] = []

    def fake_run_clean_queue(emit) -> CleanQueueStats:
        calls.append(emit)
        emit("queue cleanup message")
        return CleanQueueStats()

    monkeypatch.setattr(mover, "run_clean_queue", fake_run_clean_queue)

    clean_queue(logger)

    assert len(calls) == 1


def test_clean_queue_raises_when_cleanup_reports_failures(monkeypatch: pytest.MonkeyPatch) -> None:
    logger = logging.getLogger("test_clean_queue_failures")
    logger.handlers.clear()
    logger.setLevel(logging.INFO)
    logger.propagate = False

    monkeypatch.setattr(
        mover,
        "run_clean_queue",
        lambda emit: CleanQueueStats(failed=1),
    )

    with pytest.raises(RuntimeError, match="Queue cleanup reported 1 failure"):
        clean_queue(logger)
