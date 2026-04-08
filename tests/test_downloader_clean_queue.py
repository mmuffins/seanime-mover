from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

import downloader_clean_queue as cleaner


class FakeClient:
    def __init__(self, list_results: dict[str, object] | None = None, delete_errors: set[str] | None = None) -> None:
        self.list_results = list_results or {}
        self.delete_errors = delete_errors or set()
        self.deleted: list[cleaner.QueuedDownload] = []


def make_item(item_id: str, *, age_days: int | None = None, created_at: str | None = None) -> dict[str, str]:
    item: dict[str, str] = {"id": item_id, "name": f"item-{item_id}"}
    if created_at is not None:
        item["created_at"] = created_at
    elif age_days is not None:
        item["created_at"] = (datetime.now(timezone.utc) - timedelta(days=age_days)).isoformat()
    return item


@pytest.fixture(autouse=True)
def generic_api_key(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(cleaner, "DOWNLOADER_API_KEY", "test-key")


def test_process_queue_aggregates_all_queue_types(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]):
    now = datetime.now(timezone.utc)
    client = FakeClient(
        list_results={
            "torrent": [make_item("1", age_days=20)],
            "usenet": [make_item("2", age_days=15)],
            "webdl": [make_item("3", age_days=12)],
        }
    )

    monkeypatch.setattr(cleaner, "list_queued_downloads", lambda client_arg, queue_type: [
        cleaner.normalize_queued_download(item, queue_type) for item in client_arg.list_results[queue_type]
    ])
    monkeypatch.setattr(
        cleaner,
        "delete_queued_download",
        lambda client_arg, queued_download: client_arg.deleted.append(queued_download),
    )

    stats = cleaner.process_queue(client, now=now, max_age_days=10)

    assert stats.fetched == 3
    assert stats.deleted == 3
    assert stats.failed == 0
    assert {item.queue_type for item in client.deleted} == {"torrent", "usenet", "webdl"}
    assert "Summary: fetched=3 deleted=3" in capsys.readouterr().out


def test_process_queue_deletes_only_items_older_than_threshold(monkeypatch: pytest.MonkeyPatch) -> None:
    now = datetime.now(timezone.utc)
    client = FakeClient(
        list_results={
            "torrent": [make_item("old", age_days=11), make_item("new", age_days=2)],
            "usenet": [],
            "webdl": [],
        }
    )

    monkeypatch.setattr(cleaner, "list_queued_downloads", lambda client_arg, queue_type: [
        cleaner.normalize_queued_download(item, queue_type) for item in client_arg.list_results[queue_type]
    ])
    monkeypatch.setattr(
        cleaner,
        "delete_queued_download",
        lambda client_arg, queued_download: client_arg.deleted.append(queued_download),
    )

    stats = cleaner.process_queue(client, now=now, max_age_days=10)

    assert stats.deleted == 1
    assert stats.skipped_too_new == 1
    assert [item.item_id for item in client.deleted] == ["old"]


def test_process_queue_skips_items_with_missing_timestamps(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = datetime.now(timezone.utc)
    client = FakeClient(
        list_results={
            "torrent": [make_item("missing", created_at="")],
            "usenet": [],
            "webdl": [],
        }
    )

    monkeypatch.setattr(cleaner, "list_queued_downloads", lambda client_arg, queue_type: [
        cleaner.normalize_queued_download(item, queue_type) for item in client_arg.list_results[queue_type]
    ])
    monkeypatch.setattr(
        cleaner,
        "delete_queued_download",
        lambda client_arg, queued_download: client_arg.deleted.append(queued_download),
    )

    stats = cleaner.process_queue(client, now=now, max_age_days=10)

    assert stats.deleted == 0
    assert stats.skipped_missing_timestamp == 1
    assert client.deleted == []


def test_process_queue_continues_after_delete_failure(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    now = datetime.now(timezone.utc)
    client = FakeClient(
        list_results={
            "torrent": [make_item("bad", age_days=20), make_item("good", age_days=20)],
            "usenet": [],
            "webdl": [],
        },
        delete_errors={"bad"},
    )

    monkeypatch.setattr(cleaner, "list_queued_downloads", lambda client_arg, queue_type: [
        cleaner.normalize_queued_download(item, queue_type) for item in client_arg.list_results[queue_type]
    ])

    def fake_delete(client_arg: FakeClient, queued_download: cleaner.QueuedDownload) -> None:
        if queued_download.item_id in client_arg.delete_errors:
            raise RuntimeError("delete failed")
        client_arg.deleted.append(queued_download)

    monkeypatch.setattr(cleaner, "delete_queued_download", fake_delete)

    stats = cleaner.process_queue(client, now=now, max_age_days=10)

    assert stats.deleted == 1
    assert stats.failed == 1
    assert [item.item_id for item in client.deleted] == ["good"]
    assert "Failed to delete download item" in capsys.readouterr().out


def test_process_queue_continues_after_queue_type_fetch_failure(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    now = datetime.now(timezone.utc)
    client = FakeClient(
        list_results={
            "torrent": [make_item("1", age_days=20)],
            "webdl": [make_item("2", age_days=20)],
        }
    )

    def fake_list(client_arg: FakeClient, queue_type: str) -> list[cleaner.QueuedDownload]:
        if queue_type == "usenet":
            raise RuntimeError("fetch failed")
        return [cleaner.normalize_queued_download(item, queue_type) for item in client_arg.list_results[queue_type]]

    monkeypatch.setattr(cleaner, "list_queued_downloads", fake_list)
    monkeypatch.setattr(
        cleaner,
        "delete_queued_download",
        lambda client_arg, queued_download: client_arg.deleted.append(queued_download),
    )

    stats = cleaner.process_queue(client, now=now, max_age_days=10)

    assert stats.fetched == 2
    assert stats.deleted == 2
    assert stats.failed == 1
    assert "Failed to list downloads for type=usenet" in capsys.readouterr().out


def test_list_queued_downloads_raises_on_non_success_status(monkeypatch: pytest.MonkeyPatch) -> None:
    client = FakeClient()

    monkeypatch.setattr(
        cleaner,
        "send_list_request",
        lambda client_arg, queue_type: ({"message": "bad request"}, 500, "application/json"),
    )

    with pytest.raises(RuntimeError, match="status=500"):
        cleaner.list_queued_downloads(client, "torrent")


def test_delete_queued_download_uses_provider_specific_id_keys(monkeypatch: pytest.MonkeyPatch) -> None:
    captured_requests: list[tuple[str, dict[str, object]]] = []

    def fake_send_delete_request(
        client_arg: FakeClient,
        queue_type: str,
        request_body: dict[str, object],
    ) -> tuple[dict[str, object], int, str]:
        captured_requests.append((queue_type, request_body))
        return ({"success": True}, 200, "application/json")

    monkeypatch.setattr(cleaner, "send_delete_request", fake_send_delete_request)

    client = FakeClient()
    cleaner.delete_queued_download(client, cleaner.QueuedDownload("torrent", "1", "a", None))
    cleaner.delete_queued_download(client, cleaner.QueuedDownload("usenet", "2", "b", None))
    cleaner.delete_queued_download(client, cleaner.QueuedDownload("webdl", "3", "c", None))

    assert captured_requests == [
        ("torrent", {"torrent_id": 1, "operation": "delete", "all": False}),
        ("usenet", {"usenet_id": 2, "operation": "delete", "all": False}),
        ("webdl", {"webdl_id": 3, "operation": "delete", "all": False}),
    ]


def test_main_returns_non_zero_when_processing_reports_failures(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(cleaner, "run_clean_queue", lambda: cleaner.CleanQueueStats(failed=1))

    assert cleaner.main() == 1


def test_run_clean_queue_uses_process_queue_and_returns_stats(monkeypatch: pytest.MonkeyPatch) -> None:
    client = FakeClient()
    expected_stats = cleaner.CleanQueueStats(deleted=2)

    monkeypatch.setattr(cleaner, "create_downloader_client", lambda: client)
    monkeypatch.setattr(cleaner, "process_queue", lambda client_arg, emit=None: expected_stats)

    assert cleaner.run_clean_queue() is expected_stats


def test_main_smoke_path_returns_zero(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    monkeypatch.setattr(cleaner, "run_clean_queue", lambda: cleaner.CleanQueueStats())

    assert cleaner.main() == 0
    assert capsys.readouterr().out == ""
