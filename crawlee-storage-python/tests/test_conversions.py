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
