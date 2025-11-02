"""Redis-based message queue backend implementation for abstract backend.

This module provides a Redis Streams-based implementation of the QueueBackend
protocol, enabling the abstract backend to use Redis as its message queue backend.
"""

import asyncio
import json
import logging
import os
from collections.abc import AsyncIterator
from typing import Any

import redis.asyncio as aioredis
from redis.asyncio.client import Redis
from abe.backends.message_queue.base.protocol import MessageQueueBackend

__all__ = ["RedisMessageQueueBackend"]

logger = logging.getLogger(__name__)


class RedisMessageQueueBackend(MessageQueueBackend):
    """Redis Streams-based message queue backend implementation.

    This backend uses Redis Streams for reliable message queueing with support
    for consumer groups. It provides asynchronous publish/consume operations
    with automatic connection management and error handling.

    Environment Variables:
        REDIS_URL: Redis connection URL (default: redis://localhost:6379/0)
        REDIS_PASSWORD: Redis password (optional)
        REDIS_SSL: Enable SSL/TLS connection (default: false)
        REDIS_MAX_CONNECTIONS: Maximum connection pool size (default: 10)
        REDIS_STREAM_MAXLEN: Maximum stream length for trimming (default: 10000)

    Example:
        >>> import os
        >>> os.environ["REDIS_URL"] = "redis://localhost:6379/0"
        >>> backend = RedisMessageQueueBackend.from_env()
        >>> await backend.publish("events", {"type": "message", "text": "Hello"})
        >>> async for msg in backend.consume(group="workers"):
        ...     print(msg)
    """

    def __init__(
        self,
        redis_url: str,
        password: str | None = None,
        ssl: bool = False,
        max_connections: int = 10,
        stream_maxlen: int = 10000,
    ) -> None:
        """Initialize the Redis message queue backend.

        Args:
            redis_url: Redis connection URL (e.g., redis://localhost:6379/0)
            password: Optional Redis password for authentication
            ssl: Enable SSL/TLS for the connection
            max_connections: Maximum number of connections in the pool
            stream_maxlen: Maximum length for Redis streams (for MAXLEN trimming)
        """
        self._redis_url = redis_url
        self._password = password
        self._ssl = ssl
        self._max_connections = max_connections
        self._stream_maxlen = stream_maxlen
        self._client: Redis[bytes] | None = None
        self._connected = False
        logger.info(
            "Initialized RedisMessageQueueBackend with URL: %s, SSL: %s",
            redis_url,
            ssl,
        )

    @classmethod
    def from_env(cls) -> "RedisMessageQueueBackend":
        """Create a Redis backend instance from environment variables.

        Reads configuration from the following environment variables:
        - REDIS_URL: Connection URL (default: redis://localhost:6379/0)
        - REDIS_PASSWORD: Authentication password (optional)
        - REDIS_SSL: Enable SSL (default: false)
        - REDIS_MAX_CONNECTIONS: Pool size (default: 10)
        - REDIS_STREAM_MAXLEN: Stream max length (default: 10000)

        Returns:
            Configured RedisMessageQueueBackend instance

        Raises:
            ValueError: If REDIS_URL format is invalid
        """
        redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
        password = os.getenv("REDIS_PASSWORD")
        ssl = os.getenv("REDIS_SSL", "false").lower() == "true"
        max_connections = int(os.getenv("REDIS_MAX_CONNECTIONS", "10"))
        stream_maxlen = int(os.getenv("REDIS_STREAM_MAXLEN", "10000"))

        logger.info("Creating Redis backend from environment variables")
        return cls(
            redis_url=redis_url,
            password=password,
            ssl=ssl,
            max_connections=max_connections,
            stream_maxlen=stream_maxlen,
        )

    async def _ensure_connected(self) -> None:
        """Ensure connection to Redis server is established.

        Raises:
            ConnectionError: If unable to connect to Redis
        """
        if not self._connected or self._client is None:
            await self._connect()

    async def _connect(self) -> None:
        """Establish connection to Redis server.

        Creates a connection pool and validates the connection with a PING.

        Raises:
            ConnectionError: If connection to Redis fails
        """
        try:
            # Build connection kwargs
            connection_kwargs: dict[str, Any] = {
                "max_connections": self._max_connections,
                "decode_responses": False,
            }

            # Add password if provided
            if self._password:
                connection_kwargs["password"] = self._password

            # Add SSL if enabled (use ssl_context for SSL connections)
            if self._ssl:
                import ssl as ssl_module

                connection_kwargs["ssl"] = ssl_module.create_default_context()

            self._client = await aioredis.from_url(
                self._redis_url,
                **connection_kwargs,
            )
            # Validate connection
            await self._client.ping()
            self._connected = True
            logger.info("Successfully connected to Redis at %s", self._redis_url)
        except Exception as e:
            logger.error("Failed to connect to Redis: %s", e)
            raise ConnectionError(f"Unable to connect to Redis: {e}") from e

    async def publish(self, key: str, payload: dict[str, Any]) -> None:
        """Publish a message to a Redis stream.

        Publishes the payload as JSON to a Redis stream identified by the key.
        Automatically trims the stream to REDIS_STREAM_MAXLEN to prevent
        unbounded growth.

        Args:
            key: Redis stream name/key
            payload: Message data as a dictionary (must be JSON-serializable)

        Raises:
            ConnectionError: If connection to Redis is lost
            ValueError: If payload cannot be serialized to JSON
            RuntimeError: If publishing fails
        """
        await self._ensure_connected()

        if self._client is None:
            raise RuntimeError("Redis client is not initialized")

        try:
            message_json = json.dumps(payload)
            # Use XADD to add message to stream with MAXLEN trimming
            await self._client.xadd(
                name=key,
                fields={"data": message_json},
                maxlen=self._stream_maxlen,
                approximate=True,
            )
            logger.debug("Published message to stream '%s': %s", key, payload)
        except (TypeError, ValueError) as e:
            # TypeError or ValueError from json.dumps for non-serializable objects
            logger.error("Failed to serialize payload to JSON: %s", e)
            raise ValueError(f"Payload is not JSON-serializable: {e}") from e
        except Exception as e:
            logger.error("Failed to publish message to stream '%s': %s", key, e)
            raise RuntimeError(f"Unable to publish message: {e}") from e

    async def consume(
        self,
        *,
        group: str | None = None,
    ) -> AsyncIterator[dict[str, Any]]:
        """Consume messages from Redis streams.

        Continuously consumes messages from all streams matching the pattern
        'slack:*'. Supports consumer groups for distributed consumption.

        When a consumer group is specified, messages are distributed among
        consumers in the group using Redis Streams consumer groups.

        Args:
            group: Optional consumer group name for distributed consumption

        Yields:
            Message payloads as dictionaries

        Raises:
            ConnectionError: If connection to Redis is lost
            RuntimeError: If consumption fails
        """
        await self._ensure_connected()

        if self._client is None:
            raise RuntimeError("Redis client is not initialized")

        # Stream pattern for all Abstract Backend events
        stream_pattern = "slack:*"
        consumer_name = f"consumer-{id(self)}"

        try:
            if group:
                # Use consumer groups for distributed consumption
                async for message in self._consume_with_group(stream_pattern, group, consumer_name):
                    yield message
            else:
                # Simple consumption without groups
                async for message in self._consume_simple(stream_pattern):
                    yield message
        except asyncio.CancelledError:
            logger.info("Consumer cancelled, shutting down gracefully")
            raise
        except Exception as e:
            logger.error("Error during message consumption: %s", e)
            raise RuntimeError(f"Unable to consume messages: {e}") from e

    async def _consume_simple(self, pattern: str) -> AsyncIterator[dict[str, Any]]:
        """Consume messages without consumer groups.

        Args:
            pattern: Stream key pattern to consume from

        Yields:
            Message payloads as dictionaries
        """
        if self._client is None:
            raise RuntimeError("Redis client is not initialized")

        stream_ids: dict[bytes, bytes] = {}
        refresh_counter = 0

        while True:
            try:
                # Refresh stream list every iteration when no streams, otherwise every 2 iterations
                should_refresh = (not stream_ids) or (refresh_counter % 2 == 0)
                if should_refresh:
                    streams = await self._get_streams_by_pattern(pattern)
                    # Add new streams with $ (new messages only)
                    for stream in streams:
                        if stream not in stream_ids:
                            stream_ids[stream] = b"$"
                            logger.info(f"Added stream {stream.decode('utf-8')} for consumption")
                refresh_counter += 1

                # Skip if no streams to read from
                if not stream_ids:
                    await asyncio.sleep(0.05)  # Shorter sleep when waiting for streams
                    continue

                # Read from multiple streams
                result = await self._client.xread(
                    streams=stream_ids,
                    count=10,
                    block=1000,  # Block for 1 second
                )

                logger.debug(f"xread result: {len(result) if result else 0} streams with messages")

                if result:
                    for stream_name, messages in result:
                        for message_id, fields in messages:
                            # Update last seen ID
                            stream_ids[stream_name] = message_id

                            # Parse message data
                            data = fields.get(b"data", b"{}")
                            try:
                                payload = json.loads(data.decode("utf-8"))
                                yield payload
                            except json.JSONDecodeError as e:
                                logger.warning(
                                    "Invalid JSON in message from %s: %s",
                                    stream_name.decode("utf-8"),
                                    e,
                                )
                                continue
                else:
                    # No messages, yield control
                    await asyncio.sleep(0.1)

            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error("Error during simple consumption: %s", e)
                await asyncio.sleep(1)

    async def _consume_with_group(self, pattern: str, group: str, consumer_name: str) -> AsyncIterator[dict[str, Any]]:
        """Consume messages using consumer groups.

        Args:
            pattern: Stream key pattern to consume from
            group: Consumer group name
            consumer_name: Unique consumer identifier

        Yields:
            Message payloads as dictionaries
        """
        if self._client is None:
            raise RuntimeError("Redis client is not initialized")

        stream_ids: dict[bytes, bytes] = {}
        refresh_counter = 0

        while True:
            try:
                # Refresh stream list every iteration when no streams, otherwise every 5 iterations
                should_refresh = (not stream_ids) or (refresh_counter % 5 == 0)
                if should_refresh:
                    streams = await self._get_streams_by_pattern(pattern)
                    # Create consumer groups and add new streams
                    for stream in streams:
                        if stream not in stream_ids:
                            await self._ensure_consumer_group(stream, group)
                            stream_ids[stream] = b">"  # Read new messages for this consumer
                            logger.debug(f"Added stream {stream.decode('utf-8')} to consumer group '{group}'")
                refresh_counter += 1

                # Skip if no streams to read from
                if not stream_ids:
                    await asyncio.sleep(0.1)
                    continue

                # Read from consumer group
                result = await self._client.xreadgroup(
                    groupname=group,
                    consumername=consumer_name,
                    streams=stream_ids,
                    count=10,
                    block=1000,
                )

                if result:
                    for stream_name, messages in result:
                        for message_id, fields in messages:
                            # Parse message data
                            data = fields.get(b"data", b"{}")
                            try:
                                payload = json.loads(data.decode("utf-8"))

                                # Acknowledge message
                                await self._client.xack(stream_name, group, message_id)

                                yield payload
                            except json.JSONDecodeError as e:
                                logger.warning(
                                    "Invalid JSON in message from %s: %s",
                                    stream_name.decode("utf-8"),
                                    e,
                                )
                                # Still acknowledge to prevent reprocessing
                                await self._client.xack(stream_name, group, message_id)
                                continue
                else:
                    # No messages, yield control
                    await asyncio.sleep(0.1)

            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error("Error reading from consumer group: %s", e)
                await asyncio.sleep(1)

    async def _ensure_consumer_group(self, stream: bytes, group: str) -> None:
        """Ensure a consumer group exists for the given stream.

        Args:
            stream: Stream key
            group: Consumer group name
        """
        if self._client is None:
            raise RuntimeError("Redis client is not initialized")

        try:
            # Try to create the consumer group (read from beginning with ID "0")
            await self._client.xgroup_create(
                name=stream,
                groupname=group,
                id=b"0",  # Start from beginning
                mkstream=True,  # Create stream if it doesn't exist
            )
            logger.info(f"Created consumer group '{group}' for stream '{stream.decode('utf-8')}'")
        except Exception as e:
            # Group might already exist, which is fine
            error_msg = str(e).lower()
            if "busygroup" not in error_msg and "exists" not in error_msg:
                logger.warning(f"Error creating consumer group '{group}' for stream '{stream.decode('utf-8')}': {e}")

    async def _get_streams_by_pattern(self, pattern: str) -> list[bytes]:
        """Get all Redis keys matching the given pattern.

        Args:
            pattern: Key pattern (e.g., 'slack:*')

        Returns:
            List of matching stream keys
        """
        if self._client is None:
            raise RuntimeError("Redis client is not initialized")

        keys = await self._client.keys(pattern)
        return keys if keys else []

    async def close(self) -> None:
        """Close the Redis connection and cleanup resources."""
        if self._client and self._connected:
            await self._client.aclose()
            self._connected = False
            logger.info("Closed Redis connection")
