from __future__ import annotations

import os
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any


def get_env_int(name: str, default: int) -> int:
    return int(os.getenv(name, str(default)))


DOWNLOADER_API_KEY = os.getenv("DOWNLOADER_API_KEY", "REPLACE_ME")
DOWNLOADER_API_VERSION = os.getenv("DOWNLOADER_API_VERSION", "v1")
DOWNLOADER_QUEUE_MAX_AGE_DAYS = get_env_int("DOWNLOADER_QUEUE_MAX_AGE_DAYS", 60)
CURRENT_PROVIDER_BASE_URL = "https://api.torbox.app"
CURRENT_PROVIDER_DOWNLOAD_TYPES = {
    "usenet": {
        "service_name": "usenet",
        "list_endpoint": "/api/usenet/mylist",
        "control_endpoint": "/api/usenet/controlusenetdownload",
        "delete_id_key": "usenet_id",
    },
    "webdl": {
        "service_name": "web_downloads_debrid",
        "list_endpoint": "/api/webdl/mylist",
        "control_endpoint": "/api/webdl/controlwebdownload",
        "delete_id_key": "webdl_id",
    },
    "torrent": {
        "service_name": "torrents",
        "list_endpoint": "/api/torrents/mylist",
        "control_endpoint": "/api/torrents/controltorrent",
        "delete_id_key": "torrent_id",
    },
}


@dataclass
class QueuedDownload:
    queue_type: str
    item_id: str
    name: str
    created_at: datetime | None


@dataclass
class CleanQueueStats:
    fetched: int = 0
    deleted: int = 0
    skipped_missing_timestamp: int = 0
    skipped_too_new: int = 0
    failed: int = 0


def emit_output(message: str, emit: Any | None = None) -> None:
    if emit is None:
        print(message)
        return
    emit(message)


def create_downloader_client() -> Any:
    if DOWNLOADER_API_KEY == "REPLACE_ME":
        raise RuntimeError("Set DOWNLOADER_API_KEY before running queue cleanup.")

    try:
        from torbox_api import TorboxApi
    except ImportError as exc:
        raise RuntimeError(
            "Install the current downloader SDK before running this script: pip install torbox-api"
        ) from exc

    return TorboxApi(access_token=DOWNLOADER_API_KEY)


def list_queued_downloads(client: Any, queue_type: str) -> list[QueuedDownload]:
    response_body, status_code, _content_type = send_list_request(client, queue_type)
    ensure_successful_response(response_body, status_code, f"list downloads type={queue_type}")
    raw_items = extract_queued_items(response_body)
    normalized_items: list[QueuedDownload] = []

    for raw_item in raw_items:
        normalized_items.append(normalize_queued_download(raw_item, queue_type))

    return normalized_items


def delete_queued_download(client: Any, queued_download: QueuedDownload) -> None:
    provider_config = get_provider_download_type(queued_download.queue_type)
    request_body = {
        provider_config["delete_id_key"]: int(queued_download.item_id),
        "operation": "delete",
        "all": False,
    }
    response_body, status_code, _content_type = send_delete_request(
        client,
        queued_download.queue_type,
        request_body,
    )
    ensure_successful_response(
        response_body,
        status_code,
        f"delete {queued_download.queue_type} id={queued_download.item_id}",
    )


def send_list_request(client: Any, queue_type: str) -> tuple[dict[str, Any], int, str]:
    provider_config = get_provider_download_type(queue_type)
    service = get_provider_service(client, queue_type)
    serializer = build_provider_serializer(
        service,
        f"{CURRENT_PROVIDER_BASE_URL}/{{api_version}}{provider_config['list_endpoint']}",
    )
    request = (
        serializer.add_path("api_version", DOWNLOADER_API_VERSION)
        .add_query("limit", "1000")
        .add_query("bypass_cache", "true")
        .serialize()
        .set_method("GET")
    )
    return service.send_request(request)


def send_delete_request(
    client: Any,
    queue_type: str,
    request_body: dict[str, Any],
) -> tuple[dict[str, Any], int, str]:
    provider_config = get_provider_download_type(queue_type)
    service = get_provider_service(client, queue_type)
    serializer = build_provider_serializer(
        service,
        f"{CURRENT_PROVIDER_BASE_URL}/{{api_version}}{provider_config['control_endpoint']}",
    )
    request = (
        serializer.add_path("api_version", DOWNLOADER_API_VERSION)
        .serialize()
        .set_method("POST")
        .set_body(request_body)
    )
    return service.send_request(request)


def build_provider_serializer(service: Any, url: str) -> Any:
    try:
        from torbox_api.net.transport.serializer import Serializer
    except ImportError as exc:
        raise RuntimeError(
            "Install the current downloader SDK before running this script: pip install torbox-api"
        ) from exc

    return Serializer(url, [service.get_access_token()])


def get_provider_download_type(queue_type: str) -> dict[str, str]:
    try:
        return CURRENT_PROVIDER_DOWNLOAD_TYPES[queue_type]
    except KeyError as exc:
        raise ValueError(f"Unsupported download type: {queue_type}") from exc


def get_provider_service(client: Any, queue_type: str) -> Any:
    provider_config = get_provider_download_type(queue_type)
    return getattr(client, provider_config["service_name"])


def ensure_successful_response(response_body: Any, status_code: int, action: str) -> None:
    if 200 <= status_code < 300 and not response_body_indicates_failure(response_body):
        return

    message = extract_response_message(response_body)
    if message:
        raise RuntimeError(f"Request failed during {action}: status={status_code} message={message}")
    raise RuntimeError(f"Request failed during {action}: status={status_code}")


def response_body_indicates_failure(response_body: Any) -> bool:
    return isinstance(response_body, dict) and response_body.get("success") is False


def extract_response_message(response_body: Any) -> str | None:
    if not isinstance(response_body, dict):
        return None

    for key in ("message", "error", "detail"):
        value = response_body.get(key)
        if value:
            return str(value)
    return None


def extract_queued_items(response_body: Any) -> list[dict[str, Any]]:
    if isinstance(response_body, list):
        return [item for item in response_body if isinstance(item, dict)]

    if not isinstance(response_body, dict):
        return []

    if isinstance(response_body.get("data"), list):
        return [item for item in response_body["data"] if isinstance(item, dict)]

    if isinstance(response_body.get("data"), dict):
        nested_items = response_body["data"].get("downloads") or response_body["data"].get("queued")
        if isinstance(nested_items, list):
            return [item for item in nested_items if isinstance(item, dict)]

    for value in response_body.values():
        if isinstance(value, list) and all(isinstance(item, dict) for item in value):
            return list(value)

    return []


def normalize_queued_download(raw_item: dict[str, Any], queue_type: str) -> QueuedDownload:
    item_id = raw_item.get("id")
    if item_id is None:
        item_id = raw_item.get("id_")
    if item_id is None:
        raise ValueError(f"Queued download for type {queue_type} is missing an id")

    return QueuedDownload(
        queue_type=queue_type,
        item_id=str(item_id),
        name=get_queued_download_name(raw_item),
        created_at=parse_queue_item_created_at(raw_item),
    )


def get_queued_download_name(raw_item: dict[str, Any]) -> str:
    for key in ("name", "filename", "file_name", "title", "hash"):
        value = raw_item.get(key)
        if value:
            return str(value)
    return "<unnamed>"


def parse_queue_item_created_at(raw_item: dict[str, Any]) -> datetime | None:
    for key in ("created_at", "createdAt", "added_at", "addedAt", "queued_at", "queuedAt"):
        raw_value = raw_item.get(key)
        if raw_value:
            return parse_datetime(raw_value)
    return None


def parse_datetime(raw_value: Any) -> datetime | None:
    if isinstance(raw_value, datetime):
        if raw_value.tzinfo is None:
            return raw_value.replace(tzinfo=timezone.utc)
        return raw_value.astimezone(timezone.utc)

    if not isinstance(raw_value, str):
        return None

    normalized = raw_value.strip()
    if not normalized:
        return None

    if normalized.endswith("Z"):
        normalized = normalized[:-1] + "+00:00"

    for candidate in (
        normalized,
        normalized.replace(" ", "T"),
        normalized.split(".", 1)[0] + "+00:00" if "." in normalized and "+" not in normalized else normalized,
    ):
        try:
            parsed = datetime.fromisoformat(candidate)
            if parsed.tzinfo is None:
                return parsed.replace(tzinfo=timezone.utc)
            return parsed.astimezone(timezone.utc)
        except ValueError:
            continue

    return None


def item_is_old_enough(queued_download: QueuedDownload, now: datetime, max_age_days: int) -> bool:
    if queued_download.created_at is None:
        return False
    return queued_download.created_at <= now - timedelta(days=max_age_days)


def process_queue(
    client: Any,
    now: datetime | None = None,
    max_age_days: int = DOWNLOADER_QUEUE_MAX_AGE_DAYS,
    emit: Any | None = None,
) -> CleanQueueStats:
    now = now or datetime.now(timezone.utc)
    stats = CleanQueueStats()

    for queue_type in CURRENT_PROVIDER_DOWNLOAD_TYPES:
        try:
            queued_downloads = list_queued_downloads(client, queue_type)
            stats.fetched += len(queued_downloads)
            emit_output(f"Found {len(queued_downloads)} download items for type={queue_type}", emit)
        except Exception as exc:
            stats.failed += 1
            emit_output(f"Failed to list downloads for type={queue_type}: {exc}", emit)
            continue

        for queued_download in queued_downloads:
            if queued_download.created_at is None:
                stats.skipped_missing_timestamp += 1
                emit_output(
                    f"Skipping download item without usable timestamp: "
                    f"type={queued_download.queue_type} id={queued_download.item_id} "
                    f"name={queued_download.name}",
                    emit,
                )
                continue

            if not item_is_old_enough(queued_download, now, max_age_days):
                stats.skipped_too_new += 1
                continue

            emit_output(
                f"Deleting download item: type={queued_download.queue_type} id={queued_download.item_id} "
                f"name={queued_download.name} created_at={queued_download.created_at.isoformat()}",
                emit,
            )
            try:
                delete_queued_download(client, queued_download)
                stats.deleted += 1
            except Exception as exc:
                stats.failed += 1
                emit_output(
                    f"Failed to delete download item: type={queued_download.queue_type} "
                    f"id={queued_download.item_id} error={exc}",
                    emit,
                )

    emit_output(
        "Summary: "
        f"fetched={stats.fetched} deleted={stats.deleted} "
        f"skipped_missing_timestamp={stats.skipped_missing_timestamp} "
        f"skipped_too_new={stats.skipped_too_new} failed={stats.failed}",
        emit,
    )
    return stats


def run_clean_queue(emit: Any | None = None) -> CleanQueueStats:
    if DOWNLOADER_QUEUE_MAX_AGE_DAYS < 0:
        raise RuntimeError("DOWNLOADER_QUEUE_MAX_AGE_DAYS must be zero or greater.")

    client = create_downloader_client()
    return process_queue(client, emit=emit)


def main() -> int:
    try:
        stats = run_clean_queue()
    except Exception as exc:
        print(f"Queue cleanup failed: {exc}")
        return 1

    return 1 if stats.failed else 0


if __name__ == "__main__":
    sys.exit(main())
