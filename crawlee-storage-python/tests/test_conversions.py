"""Tests for the Python-side type conversions across the FFI boundary.

These cover the behaviors the .pyi stubs promise:
- Metadata datetime fields (`accessedAt`, `createdAt`, `modifiedAt`) come back
  as timezone-aware `datetime.datetime` instances anchored to UTC.
- `set_expected_request_processing_time` accepts `datetime.timedelta`.
"""

from __future__ import annotations

import datetime
from pathlib import Path

import pytest
from crawlee_storage import (
    FileSystemDatasetClient,
    FileSystemKeyValueStoreClient,
    FileSystemRequestQueueClient,
)


@pytest.fixture
def storage_dir(tmp_path: Path) -> str:
    return str(tmp_path / "storage")


def _assert_tz_aware_utc(value: object) -> None:
    """Assert `value` is a `datetime.datetime` in UTC (tz-aware)."""
    assert isinstance(value, datetime.datetime), f"expected datetime, got {type(value).__name__}"
    assert value.tzinfo is not None, "datetime must be timezone-aware"
    # `utcoffset` returns the offset from UTC; UTC itself is exactly zero.
    assert value.utcoffset() == datetime.timedelta(0), (
        f"expected UTC, got tz {value.tzinfo} with offset {value.utcoffset()}"
    )


async def test_dataset_metadata_datetimes_are_tz_aware_utc(storage_dir: str) -> None:
    client = await FileSystemDatasetClient.open(storage_dir=storage_dir)
    meta = await client.get_metadata()
    _assert_tz_aware_utc(meta["accessedAt"])
    _assert_tz_aware_utc(meta["createdAt"])
    _assert_tz_aware_utc(meta["modifiedAt"])


async def test_kvs_metadata_datetimes_are_tz_aware_utc(storage_dir: str) -> None:
    client = await FileSystemKeyValueStoreClient.open(storage_dir=storage_dir)
    meta = await client.get_metadata()
    _assert_tz_aware_utc(meta["accessedAt"])
    _assert_tz_aware_utc(meta["createdAt"])
    _assert_tz_aware_utc(meta["modifiedAt"])


async def test_rq_metadata_datetimes_are_tz_aware_utc(storage_dir: str) -> None:
    client = await FileSystemRequestQueueClient.open(storage_dir=storage_dir)
    meta = await client.get_metadata()
    _assert_tz_aware_utc(meta["accessedAt"])
    _assert_tz_aware_utc(meta["createdAt"])
    _assert_tz_aware_utc(meta["modifiedAt"])


async def test_metadata_datetimes_survive_reopen(storage_dir: str) -> None:
    """The on-disk format uses microsecond precision — confirm it round-trips."""
    client = await FileSystemDatasetClient.open(name="roundtrip", storage_dir=storage_dir)
    meta_before = await client.get_metadata()

    client2 = await FileSystemDatasetClient.open(name="roundtrip", storage_dir=storage_dir)
    meta_after = await client2.get_metadata()

    # `createdAt` is stable across reopen; `accessedAt`/`modifiedAt` may shift.
    assert meta_after["createdAt"] == meta_before["createdAt"]
    _assert_tz_aware_utc(meta_after["createdAt"])


async def test_set_expected_request_processing_time_accepts_timedelta(storage_dir: str) -> None:
    """The current API takes a `timedelta`; anything else is a TypeError."""
    client = await FileSystemRequestQueueClient.open(storage_dir=storage_dir)
    # No return value to check — we just want this to not raise.
    await client.set_expected_request_processing_time(datetime.timedelta(seconds=42))
    await client.set_expected_request_processing_time(datetime.timedelta(minutes=5))
    await client.set_expected_request_processing_time(datetime.timedelta(microseconds=500))


async def test_set_expected_request_processing_time_rejects_non_timedelta(storage_dir: str) -> None:
    """Passing a number (the old API) must now fail with a TypeError."""
    client = await FileSystemRequestQueueClient.open(storage_dir=storage_dir)
    with pytest.raises(TypeError):
        await client.set_expected_request_processing_time(60)  # type: ignore[arg-type]
    with pytest.raises(TypeError):
        await client.set_expected_request_processing_time(60.5)  # type: ignore[arg-type]


async def test_advance_clock_for_testing_accepts_timedelta(storage_dir: str) -> None:
    """Each client's `advance_clock_for_testing` now takes a `timedelta`."""
    for opener in (
        FileSystemDatasetClient.open,
        FileSystemKeyValueStoreClient.open,
        FileSystemRequestQueueClient.open,
    ):
        client = await opener(storage_dir=storage_dir, use_test_clock=True)
        client.advance_clock_for_testing(datetime.timedelta(seconds=1))
        client.advance_clock_for_testing(datetime.timedelta(minutes=5))


async def test_advance_clock_for_testing_rejects_non_timedelta(storage_dir: str) -> None:
    """Passing raw milliseconds (the old API) must now fail with a TypeError."""
    client = await FileSystemDatasetClient.open(storage_dir=storage_dir, use_test_clock=True)
    with pytest.raises(TypeError):
        client.advance_clock_for_testing(1000)  # type: ignore[arg-type]


async def test_iterate_keys_accepts_prefix(storage_dir: str) -> None:
    """`iterate_keys` accepts a `prefix` kwarg and filters on the decoded key."""
    client = await FileSystemKeyValueStoreClient.open(storage_dir=storage_dir)
    await client.set_value("foo:1", b"1", "text/plain")
    await client.set_value("foo:2", b"2", "text/plain")
    await client.set_value("bar:1", b"3", "text/plain")

    keys = [record["key"] async for record in client.iterate_keys(prefix="foo:")]
    assert keys == ["foo:1", "foo:2"]

    # No prefix still returns everything.
    all_keys = sorted([record["key"] async for record in client.iterate_keys()])
    assert all_keys == ["bar:1", "foo:1", "foo:2"]


async def test_resolve_value_falls_back_to_bare_file(storage_dir: str) -> None:
    """`resolve_value` accepts a list of `(extension, content_type)` tuples, probes
    bare files when the tracked record is absent, applies the declared content type,
    and re-keys the result to the requested key."""
    client = await FileSystemKeyValueStoreClient.open(storage_dir=storage_dir)

    fallbacks = [
        ("", ""),
        (".json", "application/json"),
        (".bin", ""),
    ]

    # Hand-place a bare INPUT.json (no sidecar), like a CLI/platform writer would.
    payload = b'{"foo":"bar"}'
    (Path(client.path_to_kvs) / "INPUT.json").write_bytes(payload)

    record = await client.resolve_value("INPUT", fallbacks)
    assert record is not None
    assert record["key"] == "INPUT"  # re-keyed, not "INPUT.json"
    assert record["contentType"] == "application/json"  # declared fallback type
    assert record["value"] == payload

    # A tracked record wins over the fallback (verbatim sidecar content type).
    await client.set_value("tracked", b"x", "text/plain")
    tracked = await client.resolve_value("tracked", fallbacks)
    assert tracked is not None
    assert tracked["contentType"] == "text/plain"

    # Nothing resolves -> None.
    assert await client.resolve_value("missing", fallbacks) is None


async def test_resolve_existing_key_returns_matched_key(storage_dir: str) -> None:
    """`resolve_existing_key` accepts a list of extension strings and returns the
    matched on-disk key (literal key or key + extension), or None."""
    client = await FileSystemKeyValueStoreClient.open(storage_dir=storage_dir)
    extensions = ["", ".json", ".txt", ".bin"]

    await client.set_value("tracked", b"x", "text/plain")
    assert await client.resolve_existing_key("tracked", extensions) == "tracked"

    (Path(client.path_to_kvs) / "INPUT.json").write_bytes(b"{}")
    assert await client.resolve_existing_key("INPUT", extensions) == "INPUT.json"

    assert await client.resolve_existing_key("nope", extensions) is None


async def test_iterate_keys_valid_cursor_paginates(storage_dir: str) -> None:
    """A valid, existing `exclusive_start_key` still paginates correctly."""
    client = await FileSystemKeyValueStoreClient.open(storage_dir=storage_dir)
    await client.set_value("alpha", b"1", "text/plain")
    await client.set_value("beta", b"2", "text/plain")
    await client.set_value("gamma", b"3", "text/plain")

    keys = [record["key"] async for record in client.iterate_keys(exclusive_start_key="beta")]
    assert keys == ["gamma"]


async def test_iterate_keys_unknown_cursor_raises(storage_dir: str) -> None:
    """A nonexistent `exclusive_start_key` raises ValueError with the crawlee
    contract message (so the consumer can drop its preflight existence guard)."""
    client = await FileSystemKeyValueStoreClient.open(storage_dir=storage_dir)
    await client.set_value("alpha", b"1", "text/plain")

    with pytest.raises(ValueError, match='exclusiveStartKey "nope" was not found in the key-value store'):
        # The error surfaces when the first page is fetched.
        _ = [record async for record in client.iterate_keys(exclusive_start_key="nope")]


async def test_get_public_url_is_existence_aware(storage_dir: str) -> None:
    """`get_public_url` returns a file:// URL for an existing key and None for a
    missing one (matching the crawlee `str | None` contract)."""
    client = await FileSystemKeyValueStoreClient.open(storage_dir=storage_dir)

    assert await client.get_public_url("missing") is None

    await client.set_value("my-key", b"v", "text/plain")
    url = await client.get_public_url("my-key")
    assert url is not None
    assert url.startswith("file://")
    assert "my-key" in url


async def test_rq_open_assumes_sole_owner_by_default(storage_dir: str) -> None:
    """The RQ `open` default is now `assume_sole_owner=True`: a request left
    in-progress (locked) at crash time is immediately re-fetchable on reopen,
    without waiting out the lock window."""
    client = await FileSystemRequestQueueClient.open(storage_dir=storage_dir, name="recover")
    await client.add_batch_of_requests(
        [{"uniqueKey": "k", "url": "https://example.com/k", "method": "GET"}],
        forefront=False,
    )
    # Lock it on disk (simulating in-flight work), then "crash" by dropping the handle.
    assert await client.fetch_next_request() is not None
    rq_id = (await client.get_metadata())["id"]

    # Reopen with the default. The previously-locked request must be fetchable again.
    reopened = await FileSystemRequestQueueClient.open(storage_dir=storage_dir, id=rq_id)
    refetched = await reopened.fetch_next_request()
    assert refetched is not None
    assert refetched["uniqueKey"] == "k"
