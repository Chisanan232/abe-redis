"""Unit tests for Redis message queue backend implementation.

This module contains comprehensive unit tests for the RedisMessageQueueBackend
class, including connection handling, publish/consume operations, and error cases.
"""

import asyncio
import json
import os
from unittest.mock import AsyncMock, patch

import pytest
from redis.asyncio.client import Redis

from abe_plugin.backends.message_queue.service.redis import RedisMessageQueueBackend


class TestRedisMessageQueueBackendInit:
    """Test cases for RedisMessageQueueBackend initialization."""

    def test_init_with_defaults(self) -> None:
        """Test initialization with default parameters."""
        backend = RedisMessageQueueBackend(redis_url="redis://localhost:6379/0")

        assert backend._redis_url == "redis://localhost:6379/0"
        assert backend._password is None
        assert backend._ssl is False
        assert backend._max_connections == 10
        assert backend._stream_maxlen == 10000
        assert backend._client is None
        assert backend._connected is False

    def test_init_with_custom_params(self) -> None:
        """Test initialization with custom parameters."""
        backend = RedisMessageQueueBackend(
            redis_url="redis://remote:6379/1",
            password="secret",
            ssl=True,
            max_connections=20,
            stream_maxlen=5000,
        )

        assert backend._redis_url == "redis://remote:6379/1"
        assert backend._password == "secret"
        assert backend._ssl is True
        assert backend._max_connections == 20
        assert backend._stream_maxlen == 5000

    @patch.dict(
        os.environ,
        {
            "REDIS_URL": "redis://env-host:6379/0",
            "REDIS_PASSWORD": "env-password",
            "REDIS_SSL": "true",
            "REDIS_MAX_CONNECTIONS": "15",
            "REDIS_STREAM_MAXLEN": "8000",
        },
    )
    def test_from_env_with_all_vars(self) -> None:
        """Test from_env with all environment variables set."""
        backend = RedisMessageQueueBackend.from_env()

        assert backend._redis_url == "redis://env-host:6379/0"
        assert backend._password == "env-password"
        assert backend._ssl is True
        assert backend._max_connections == 15
        assert backend._stream_maxlen == 8000

    @patch.dict(os.environ, {}, clear=True)
    def test_from_env_with_defaults(self) -> None:
        """Test from_env with default values when env vars not set."""
        backend = RedisMessageQueueBackend.from_env()

        assert backend._redis_url == "redis://localhost:6379/0"
        assert backend._password is None
        assert backend._ssl is False
        assert backend._max_connections == 10
        assert backend._stream_maxlen == 10000


class TestRedisMessageQueueBackendConnection:
    """Test cases for Redis connection management."""

    @pytest.mark.asyncio
    async def test_connect_success(self) -> None:
        """Test successful connection to Redis."""
        backend = RedisMessageQueueBackend(redis_url="redis://localhost:6379/0")

        with patch("abe_plugin.backends.queue.service.redis.aioredis.from_url", new_callable=AsyncMock) as mock_from_url:
            mock_client = AsyncMock(spec=Redis)
            mock_client.ping = AsyncMock()
            mock_from_url.return_value = mock_client

            await backend._connect()

            assert backend._connected is True
            assert backend._client is mock_client
            mock_client.ping.assert_called_once()
            # Only max_connections and decode_responses are passed when password and ssl are not set
            mock_from_url.assert_called_once_with(
                "redis://localhost:6379/0",
                max_connections=10,
                decode_responses=False,
            )

    @pytest.mark.asyncio
    async def test_connect_failure(self) -> None:
        """Test connection failure handling."""
        backend = RedisMessageQueueBackend(redis_url="redis://invalid:6379/0")

        with patch("abe_plugin.backends.queue.service.redis.aioredis.from_url", new_callable=AsyncMock) as mock_from_url:
            mock_from_url.side_effect = ConnectionError("Connection refused")

            with pytest.raises(ConnectionError, match="Unable to connect to Redis"):
                await backend._connect()

            assert backend._connected is False

    @pytest.mark.asyncio
    async def test_ensure_connected_when_not_connected(self) -> None:
        """Test _ensure_connected calls _connect when not connected."""
        backend = RedisMessageQueueBackend(redis_url="redis://localhost:6379/0")

        with patch.object(backend, "_connect", new_callable=AsyncMock) as mock_connect:
            await backend._ensure_connected()
            mock_connect.assert_called_once()

    @pytest.mark.asyncio
    async def test_ensure_connected_when_already_connected(self) -> None:
        """Test _ensure_connected skips connection when already connected."""
        backend = RedisMessageQueueBackend(redis_url="redis://localhost:6379/0")
        backend._connected = True
        backend._client = AsyncMock(spec=Redis)

        with patch.object(backend, "_connect", new_callable=AsyncMock) as mock_connect:
            await backend._ensure_connected()
            mock_connect.assert_not_called()


class TestRedisMessageQueueBackendPublish:
    """Test cases for message publishing."""

    @pytest.mark.asyncio
    async def test_publish_success(self) -> None:
        """Test successful message publishing."""
        backend = RedisMessageQueueBackend(redis_url="redis://localhost:6379/0")

        mock_client = AsyncMock(spec=Redis)
        mock_client.xadd = AsyncMock(return_value=b"1234567890-0")
        backend._client = mock_client
        backend._connected = True

        test_payload = {"type": "message", "text": "Hello, World!"}

        await backend.publish("slack:events", test_payload)

        mock_client.xadd.assert_called_once()
        call_args = mock_client.xadd.call_args
        assert call_args.kwargs["name"] == "slack:events"
        assert call_args.kwargs["fields"]["data"] == json.dumps(test_payload)
        assert call_args.kwargs["maxlen"] == 10000
        assert call_args.kwargs["approximate"] is True

    @pytest.mark.asyncio
    async def test_publish_with_custom_maxlen(self) -> None:
        """Test publishing with custom stream maxlen."""
        backend = RedisMessageQueueBackend(
            redis_url="redis://localhost:6379/0",
            stream_maxlen=5000,
        )

        mock_client = AsyncMock(spec=Redis)
        mock_client.xadd = AsyncMock(return_value=b"1234567890-0")
        backend._client = mock_client
        backend._connected = True

        await backend.publish("slack:events", {"data": "test"})

        call_args = mock_client.xadd.call_args
        assert call_args.kwargs["maxlen"] == 5000

    @pytest.mark.asyncio
    async def test_publish_invalid_json(self) -> None:
        """Test publishing non-serializable payload raises ValueError."""
        backend = RedisMessageQueueBackend(redis_url="redis://localhost:6379/0")
        mock_client = AsyncMock(spec=Redis)
        mock_client.xadd = AsyncMock()
        backend._client = mock_client
        backend._connected = True

        # Create a non-serializable object
        class NotSerializable:
            pass

        invalid_payload = {"obj": NotSerializable()}

        with pytest.raises(ValueError, match="Payload is not JSON-serializable"):
            await backend.publish("slack:events", invalid_payload)

    @pytest.mark.asyncio
    async def test_publish_client_not_initialized(self) -> None:
        """Test publishing when client is not initialized raises RuntimeError."""
        backend = RedisMessageQueueBackend(redis_url="redis://localhost:6379/0")

        # Mock _ensure_connected to do nothing, then set client to None
        # This simulates a scenario where connection was lost
        with patch.object(backend, "_ensure_connected", new_callable=AsyncMock):
            backend._connected = True
            backend._client = None

            with pytest.raises(RuntimeError, match="Redis client is not initialized"):
                await backend.publish("slack:events", {"data": "test"})

    @pytest.mark.asyncio
    async def test_publish_connection_error(self) -> None:
        """Test publishing handles connection errors."""
        backend = RedisMessageQueueBackend(redis_url="redis://localhost:6379/0")

        mock_client = AsyncMock(spec=Redis)
        mock_client.xadd = AsyncMock(side_effect=ConnectionError("Connection lost"))
        backend._client = mock_client
        backend._connected = True

        with pytest.raises(RuntimeError, match="Unable to publish message"):
            await backend.publish("slack:events", {"data": "test"})


class TestRedisMessageQueueBackendConsume:
    """Test cases for message consumption."""

    @pytest.mark.asyncio
    async def test_consume_simple_success(self) -> None:
        """Test simple message consumption without consumer groups."""
        backend = RedisMessageQueueBackend(redis_url="redis://localhost:6379/0")

        mock_client = AsyncMock(spec=Redis)
        backend._client = mock_client
        backend._connected = True

        # Mock stream keys
        mock_client.keys = AsyncMock(return_value=[b"slack:events"])

        # Mock XREAD to return one message then empty
        test_message = {"type": "message", "text": "Test"}
        mock_client.xread = AsyncMock(
            side_effect=[
                [
                    (
                        b"slack:events",
                        [
                            (
                                b"1234567890-0",
                                {b"data": json.dumps(test_message).encode()},
                            )
                        ],
                    )
                ],
                [],  # Empty result
                asyncio.CancelledError(),  # Force exit after empty result
            ]
        )

        messages = []
        consumer = backend.consume()

        try:
            async for msg in consumer:
                messages.append(msg)
                if len(messages) >= 1:
                    break  # Exit after first message
        except asyncio.CancelledError:
            pass

        assert len(messages) == 1
        assert messages[0] == test_message

    @pytest.mark.asyncio
    async def test_consume_with_group(self) -> None:
        """Test consumption with consumer groups."""
        backend = RedisMessageQueueBackend(redis_url="redis://localhost:6379/0")

        mock_client = AsyncMock(spec=Redis)
        backend._client = mock_client
        backend._connected = True

        # Mock stream keys
        mock_client.keys = AsyncMock(return_value=[b"slack:events"])
        mock_client.xgroup_create = AsyncMock()

        # Mock XREADGROUP to return one message then raise to exit
        test_message = {"type": "message", "text": "Test"}
        mock_client.xreadgroup = AsyncMock(
            side_effect=[
                [
                    (
                        b"slack:events",
                        [
                            (
                                b"1234567890-0",
                                {b"data": json.dumps(test_message).encode()},
                            )
                        ],
                    )
                ],
                [],  # Empty result
                asyncio.CancelledError(),  # Force exit
            ]
        )
        mock_client.xack = AsyncMock()

        messages = []
        consumer = backend.consume(group="test-group")

        # Get first message then stop
        try:
            async for msg in consumer:
                messages.append(msg)
                if len(messages) >= 1:
                    break
        except asyncio.CancelledError:
            pass

        assert len(messages) == 1
        assert messages[0] == test_message
        mock_client.xack.assert_called_once()

    @pytest.mark.asyncio
    async def test_consume_invalid_json_skipped(self) -> None:
        """Test that invalid JSON messages are skipped."""
        backend = RedisMessageQueueBackend(redis_url="redis://localhost:6379/0")

        mock_client = AsyncMock(spec=Redis)
        backend._client = mock_client
        backend._connected = True

        mock_client.keys = AsyncMock(return_value=[b"slack:events"])

        # Mock XREAD with invalid JSON followed by valid message
        valid_message = {"type": "test", "valid": True}
        mock_client.xread = AsyncMock(
            side_effect=[
                [
                    (
                        b"slack:events",
                        [
                            (b"1234567890-0", {b"data": b"invalid json{"}),
                        ],
                    )
                ],
                [
                    (
                        b"slack:events",
                        [
                            (b"1234567890-1", {b"data": json.dumps(valid_message).encode()}),
                        ],
                    )
                ],
                asyncio.CancelledError(),
            ]
        )

        consumer = backend.consume()

        # Should skip invalid message and get valid one
        messages = []
        try:
            async for msg in consumer:
                messages.append(msg)
                if len(messages) >= 1:
                    break
        except asyncio.CancelledError:
            pass

        # Should only get the valid message
        assert len(messages) == 1
        assert messages[0] == valid_message

    @pytest.mark.asyncio
    async def test_get_streams_by_pattern(self) -> None:
        """Test getting streams by pattern."""
        backend = RedisMessageQueueBackend(redis_url="redis://localhost:6379/0")

        mock_client = AsyncMock(spec=Redis)
        backend._client = mock_client

        expected_keys = [b"slack:events", b"slack:commands"]
        mock_client.keys = AsyncMock(return_value=expected_keys)

        result = await backend._get_streams_by_pattern("slack:*")

        assert result == expected_keys
        mock_client.keys.assert_called_once_with("slack:*")

    @pytest.mark.asyncio
    async def test_get_streams_empty(self) -> None:
        """Test getting streams returns empty list when no matches."""
        backend = RedisMessageQueueBackend(redis_url="redis://localhost:6379/0")

        mock_client = AsyncMock(spec=Redis)
        backend._client = mock_client
        mock_client.keys = AsyncMock(return_value=[])

        result = await backend._get_streams_by_pattern("nonexistent:*")

        assert result == []


class TestRedisMessageQueueBackendClose:
    """Test cases for connection cleanup."""

    @pytest.mark.asyncio
    async def test_close_when_connected(self) -> None:
        """Test closing connection when connected."""
        backend = RedisMessageQueueBackend(redis_url="redis://localhost:6379/0")

        mock_client = AsyncMock(spec=Redis)
        mock_client.aclose = AsyncMock()
        backend._client = mock_client
        backend._connected = True

        await backend.close()

        mock_client.aclose.assert_called_once()
        assert backend._connected is False

    @pytest.mark.asyncio
    async def test_close_when_not_connected(self) -> None:
        """Test closing when not connected does nothing."""
        backend = RedisMessageQueueBackend(redis_url="redis://localhost:6379/0")
        backend._connected = False
        backend._client = None

        # Should not raise any errors
        await backend.close()

        assert backend._connected is False

    @pytest.mark.asyncio
    async def test_close_when_client_none(self) -> None:
        """Test closing when client is None."""
        backend = RedisMessageQueueBackend(redis_url="redis://localhost:6379/0")
        backend._connected = True
        backend._client = None

        await backend.close()

        # Should handle gracefully
        assert backend._connected is True  # State unchanged


class TestRedisMessageQueueBackendEdgeCases:
    """Test edge cases and error conditions."""

    @pytest.mark.asyncio
    async def test_multiple_publishes(self) -> None:
        """Test publishing multiple messages in sequence."""
        backend = RedisMessageQueueBackend(redis_url="redis://localhost:6379/0")

        mock_client = AsyncMock(spec=Redis)
        mock_client.xadd = AsyncMock(return_value=b"1234567890-0")
        backend._client = mock_client
        backend._connected = True

        messages = [
            {"id": 1, "text": "First"},
            {"id": 2, "text": "Second"},
            {"id": 3, "text": "Third"},
        ]

        for msg in messages:
            await backend.publish("slack:events", msg)

        assert mock_client.xadd.call_count == 3

    @pytest.mark.asyncio
    async def test_consume_handles_cancellation(self) -> None:
        """Test that consume handles cancellation gracefully."""
        backend = RedisMessageQueueBackend(redis_url="redis://localhost:6379/0")

        mock_client = AsyncMock(spec=Redis)
        backend._client = mock_client
        backend._connected = True

        mock_client.keys = AsyncMock(return_value=[b"slack:events"])
        # Make xread raise CancelledError to simulate cancellation
        mock_client.xread = AsyncMock(side_effect=asyncio.CancelledError())

        consumer = backend.consume()

        # Should handle cancellation gracefully
        with pytest.raises(asyncio.CancelledError):
            await consumer.__anext__()

    @pytest.mark.asyncio
    async def test_empty_payload(self) -> None:
        """Test publishing empty payload."""
        backend = RedisMessageQueueBackend(redis_url="redis://localhost:6379/0")

        mock_client = AsyncMock(spec=Redis)
        mock_client.xadd = AsyncMock(return_value=b"1234567890-0")
        backend._client = mock_client
        backend._connected = True

        await backend.publish("slack:events", {})

        mock_client.xadd.assert_called_once()
        call_args = mock_client.xadd.call_args
        assert call_args.kwargs["fields"]["data"] == "{}"
