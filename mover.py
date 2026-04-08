from __future__ import annotations

import logging
import os
import shutil
import signal
import sys
import time
from dataclasses import dataclass
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path

from downloader_clean_queue import run_clean_queue


def get_env_int(name: str, default: int) -> int:
    return int(os.getenv(name, str(default)))


SOURCE_DIR = Path(os.getenv("SOURCE_DIR", "/source"))
DEST_DIR = Path(os.getenv("DEST_DIR", "/dest"))
LOG_DIR = Path(os.getenv("LOG_DIR", "/config"))
LOG_FILE = LOG_DIR / "mover.log"
LAST_CLEAN_QUEUE_FILE = LOG_DIR / "clean_queue.last_success"
SCAN_INTERVAL_SECONDS = get_env_int("SCAN_INTERVAL_SECONDS", 60)
READY_AGE_SECONDS = get_env_int("READY_AGE_SECONDS", 60)
LOG_RETENTION_DAYS = get_env_int("LOG_RETENTION_DAYS", 30)
MIN_FILE_SIZE_BYTES = max(0, get_env_int("MIN_FILE_SIZE_BYTES", 0))
CLEAN_QUEUE_INTERVAL_SECONDS = get_env_int("CLEAN_QUEUE_INTERVAL_SECONDS", 24 * 60 * 60)


@dataclass
class ScanStats:
    moved: int = 0
    skipped_too_small: int = 0
    skipped_too_recent: int = 0
    skipped_tmp_dirs: int = 0
    collisions: int = 0
    errors: int = 0


def configure_logging(log_dir: Path = LOG_DIR) -> logging.Logger:
    log_dir.mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger("mover_script")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    logger.propagate = False

    formatter = logging.Formatter(
        fmt="%(asctime)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    file_handler = TimedRotatingFileHandler(
        filename=log_dir / "mover.log",
        when="midnight",
        interval=1,
        backupCount=LOG_RETENTION_DAYS,
        encoding="utf-8",
    )
    file_handler.setFormatter(formatter)
    file_handler.setLevel(logging.INFO)

    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)
    stream_handler.setLevel(logging.DEBUG)

    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)
    return logger


def is_in_tmp_directory(source_root: Path, file_path: Path) -> bool:
    relative_parts = file_path.relative_to(source_root).parts[:-1]
    return any(part.startswith(".tmp") for part in relative_parts)


def should_skip_file(source_root: Path, file_path: Path, now: float) -> str | None:
    if is_in_tmp_directory(source_root, file_path):
        return "tmp"

    try:
        stat_result = file_path.stat()
    except FileNotFoundError:
        return "missing"

    if stat_result.st_size <= MIN_FILE_SIZE_BYTES:
        return "too-small"

    if now - stat_result.st_mtime < READY_AGE_SECONDS:
        return "too-recent"

    return None


def move_file(file_path: Path, dest_root: Path, logger: logging.Logger) -> str:
    destination_path = dest_root / file_path.name
    if destination_path.exists():
        logger.error(
            "Destination collision for '%s'; leaving source file in place",
            file_path,
        )
        return "collision"

    try:
        shutil.move(str(file_path), str(destination_path))
        file_path.touch(exist_ok=True)
    except Exception:
        logger.exception("Failed to move '%s' to '%s'", file_path, destination_path)
        return "error"

    logger.info("Moved '%s' to '%s' and created placeholder", file_path, destination_path)
    return "moved"


def scan_once(
    source_root: Path = SOURCE_DIR,
    dest_root: Path = DEST_DIR,
    logger: logging.Logger | None = None,
    now: float | None = None,
) -> ScanStats:
    logger = logger or configure_logging()
    now = time.time() if now is None else now
    stats = ScanStats()

    source_root.mkdir(parents=True, exist_ok=True)
    dest_root.mkdir(parents=True, exist_ok=True)

    for current_root, dirnames, filenames in os.walk(source_root, topdown=True):
        current_path = Path(current_root)

        skipped_dirs = [name for name in dirnames if name.startswith(".tmp")]
        if skipped_dirs:
            stats.skipped_tmp_dirs += len(skipped_dirs)
            for dirname in skipped_dirs:
                logger.info("Skipping directory tree '%s'", current_path / dirname)
            dirnames[:] = [name for name in dirnames if not name.startswith(".tmp")]

        for filename in filenames:
            file_path = current_path / filename
            skip_reason = should_skip_file(source_root, file_path, now)

            if skip_reason == "missing":
                continue
            if skip_reason == "tmp":
                stats.skipped_tmp_dirs += 1
                continue
            if skip_reason == "too-small":
                stats.skipped_too_small += 1
                continue
            if skip_reason == "too-recent":
                stats.skipped_too_recent += 1
                continue

            move_result = move_file(file_path, dest_root, logger)
            if move_result == "moved":
                stats.moved += 1
            elif move_result == "collision":
                stats.collisions += 1
            else:
                stats.errors += 1

    if any(
        [
            stats.skipped_too_small,
            stats.skipped_too_recent,
            stats.skipped_tmp_dirs,
            stats.collisions,
            stats.errors,
            stats.moved,
        ]
    ):
        logger.debug(
            "Scan summary: moved=%d skipped_too_small=%d skipped_too_recent=%d "
            "skipped_tmp_dirs=%d collisions=%d errors=%d",
            stats.moved,
            stats.skipped_too_small,
            stats.skipped_too_recent,
            stats.skipped_tmp_dirs,
            stats.collisions,
            stats.errors,
        )

    return stats


def clean_queue(logger: logging.Logger) -> None:
    emit = logger.info
    stats = run_clean_queue(emit=emit)
    if stats.failed:
        raise RuntimeError(f"Queue cleanup reported {stats.failed} failure(s)")


def read_last_clean_queue_timestamp(
    state_file: Path = LAST_CLEAN_QUEUE_FILE,
    logger: logging.Logger | None = None,
) -> float | None:
    try:
        raw_value = state_file.read_text(encoding="utf-8").strip()
    except FileNotFoundError:
        return None
    except OSError:
        if logger is not None:
            logger.exception("Failed to read clean queue state file '%s'", state_file)
        return None

    if not raw_value:
        return None

    try:
        return float(raw_value)
    except ValueError:
        if logger is not None:
            logger.warning("Ignoring invalid clean queue state in '%s'", state_file)
        return None


def write_last_clean_queue_timestamp(
    timestamp: float,
    state_file: Path = LAST_CLEAN_QUEUE_FILE,
) -> None:
    state_file.parent.mkdir(parents=True, exist_ok=True)
    state_file.write_text(f"{timestamp:.6f}\n", encoding="utf-8")


def get_next_clean_queue_at(
    now: float,
    logger: logging.Logger,
    state_file: Path = LAST_CLEAN_QUEUE_FILE,
) -> float:
    last_successful_run = read_last_clean_queue_timestamp(state_file, logger)
    if last_successful_run is None:
        logger.info("No previous clean_queue state found; cleanup is due immediately")
        return now

    next_due_at = last_successful_run + CLEAN_QUEUE_INTERVAL_SECONDS
    if next_due_at <= now:
        logger.info("clean_queue is due immediately based on persisted state")
        return now

    return next_due_at


def run_forever() -> None:
    logger = configure_logging()
    stop_requested = False
    startup_time = time.time()
    next_scan_at = startup_time
    next_clean_queue_at = get_next_clean_queue_at(startup_time, logger)

    def handle_signal(signum: int, _frame: object) -> None:
        nonlocal stop_requested
        logger.info("Received signal %s, shutting down", signum)
        stop_requested = True

    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)

    logger.info(
        "Starting mover with source=%s dest=%s logs=%s scan_interval=%ss ready_age=%ss "
        "min_file_size_bytes=%s retention_days=%s",
        SOURCE_DIR,
        DEST_DIR,
        LOG_DIR,
        SCAN_INTERVAL_SECONDS,
        READY_AGE_SECONDS,
        MIN_FILE_SIZE_BYTES,
        LOG_RETENTION_DAYS,
    )

    while not stop_requested:
        now = time.time()

        if now >= next_scan_at:
            try:
                scan_once(SOURCE_DIR, DEST_DIR, logger, now=now)
            except Exception:
                logger.exception("Unhandled error during scan")
            next_scan_at = now + SCAN_INTERVAL_SECONDS

        if now >= next_clean_queue_at:
            try:
                clean_queue(logger)
                successful_run_at = time.time()
                write_last_clean_queue_timestamp(successful_run_at)
                logger.info("Ran clean_queue")
                next_clean_queue_at = successful_run_at + CLEAN_QUEUE_INTERVAL_SECONDS
            except Exception:
                logger.exception("Unhandled error during clean_queue")
                next_clean_queue_at = now + CLEAN_QUEUE_INTERVAL_SECONDS

        if not stop_requested:
            sleep_until = min(next_scan_at, next_clean_queue_at)
            time.sleep(max(0.0, sleep_until - time.time()))


if __name__ == "__main__":
    run_forever()
