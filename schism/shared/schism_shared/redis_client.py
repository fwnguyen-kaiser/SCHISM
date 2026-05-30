"""
Thin async wrapper for Redis Streams: publish (XADD) and consume (XREADGROUP + XACK).

Design choice: each stream entry carries a single JSON-encoded "data" field rather
than one Redis field per model attribute. This avoids manual byte-to-type coercion
on read and keeps the schema the single source of truth (Pydantic).
"""
from __future__ import annotations

import json
from collections.abc import AsyncGenerator
from typing import Any

import redis.asyncio as aioredis
from pydantic import BaseModel


class RedisStreamClient:
    def __init__(self, client: aioredis.Redis) -> None:
        self._r = client

    @classmethod
    def from_url(cls, url: str) -> RedisStreamClient:
        return cls(aioredis.from_url(url, decode_responses=True))

    async def publish(self, stream: str, event: BaseModel) -> str:
        """XADD event to stream. Returns the Redis entry ID."""
        entry_id: str = await self._r.xadd(stream, {"data": event.model_dump_json()})
        return entry_id

    async def ensure_consumer_group(self, stream: str, group: str) -> None:
        """
        XGROUP CREATE with id="$" so the group only sees messages arriving after
        its first creation. MKSTREAM creates the stream key if it doesn't exist yet.
        BUSYGROUP means the group already exists — safe to ignore.
        """
        try:
            await self._r.xgroup_create(stream, group, id="$", mkstream=True)
        except aioredis.ResponseError as exc:
            if "BUSYGROUP" not in str(exc):
                raise

    async def consume(
        self,
        stream: str,
        group: str,
        consumer: str,
        batch_size: int = 1,
        block_ms: int = 5_000,
    ) -> AsyncGenerator[tuple[str, dict[str, Any]], None]:
        """
        Yield (entry_id, raw_fields) for each new message from the consumer group.

        Uses ">" as the ID so only undelivered messages are returned (not PEL retries).
        Blocks for block_ms ms; yields nothing if the timeout elapses with no messages.
        Caller must call ack() after successful processing to remove from the PEL.
        """
        entries = await self._r.xreadgroup(
            groupname=group,
            consumername=consumer,
            streams={stream: ">"},
            count=batch_size,
            block=block_ms,
        )
        if not entries:
            return
        for _stream, messages in entries:
            for entry_id, fields in messages:
                yield entry_id, fields

    async def ack(self, stream: str, group: str, entry_id: str) -> None:
        """XACK: remove entry from the Pending Entries List after successful processing."""
        await self._r.xack(stream, group, entry_id)

    def decode(self, fields: dict[str, Any]) -> dict:
        """Deserialise the JSON payload from a raw Redis fields dict."""
        return json.loads(fields["data"])
