"""Integration tests for Redis message queue backend.

These tests use Testcontainers to automatically spin up a Redis instance
for testing, ensuring isolated and reproducible test environments.
"""

import asyncio
import os
from typing import Any, AsyncGenerator

import pytest
import pytest_asyncio

from abe_plugin.backends.message_queue.service.redis import RedisMessageQueueBackend

# Skip all tests if integration tests are explicitly disabled
pytestmark = pytest.mark.skipif(
    os.getenv("SKIP_INTEGRATION_TESTS", "false").lower() == "true",
    reason="Integration tests disabled (set SKIP_INTEGRATION_TESTS=false to enable)",
)


@pytest_asyncio.fixture
async def redis_backend(redis_connection_url: str) -> AsyncGenerator[RedisMessageQueueBackend, None]:
    """Provide a Redis backend instance for testing.

    Uses Testcontainers Redis instance for isolated testing.
    Cleans up streams after tests.

    Args:
        redis_connection_url: Redis connection URL from Testcontainers (session-scoped)

    Yields:
        Configured RedisMessageQueueBackend instance
    """
    backend = RedisMessageQueueBackend(redis_url=redis_connection_url, stream_maxlen=100)

    # Ensure connection
    await backend._connect()

    yield backend

    # Cleanup: delete all test streams
    if backend._client:
        try:
            keys = await backend._client.keys("slack:*")
            if keys:
                await backend._client.delete(*keys)
        except Exception:
            pass  # Ignore cleanup errors

    await backend.close()


@pytest_asyncio.fixture
async def clean_redis_backend(redis_connection_url: str) -> AsyncGenerator[RedisMessageQueueBackend, None]:
    """Provide a Redis backend with cleanup after test.

    Uses Testcontainers Redis instance and deletes all streams after the test completes.
    Does NOT clean before test to allow tests to set up their own state.

    Args:
        redis_connection_url: Redis connection URL from Testcontainers (session-scoped)

    Yields:
        RedisMessageQueueBackend instance
    """
    backend = RedisMessageQueueBackend(redis_url=redis_connection_url, stream_maxlen=100)

    await backend._connect()

    yield backend

    # Clean after test
    if backend._client:
        try:
            keys = await backend._client.keys("slack:*")
            if keys:
                await backend._client.delete(*keys)
        except Exception:
            pass

    await backend.close()


class TestRedisIntegrationConnection:
    """Integration tests for Redis connection management."""

    @pytest.mark.asyncio
    async def test_connection_lifecycle(self, redis_connection_url: str) -> None:
        """Test complete connection lifecycle: connect -> use -> close."""
        backend = RedisMessageQueueBackend(redis_url=redis_connection_url)

        # Initially not connected
        assert not backend._connected

        # Connect
        await backend._connect()
        assert backend._connected
        assert backend._client is not None

        # Can ping
        if backend._client:
            result = await backend._client.ping()
            assert result is True

        # Close
        await backend.close()
        assert not backend._connected

    @pytest.mark.asyncio
    async def test_from_env_integration(self, redis_connection_url: str) -> None:
        """Test creating backend from environment variables."""
        # Set test environment with Testcontainers URL
        os.environ["REDIS_URL"] = redis_connection_url
        os.environ["REDIS_STREAM_MAXLEN"] = "50"

        backend = RedisMessageQueueBackend.from_env()

        await backend._connect()
        assert backend._connected
        assert backend._stream_maxlen == 50

        await backend.close()


class TestRedisIntegrationPublish:
    """Integration tests for message publishing."""

    @pytest.mark.asyncio
    async def test_publish_single_message(self, redis_backend: RedisMessageQueueBackend) -> None:
        """Test publishing a single message to Redis."""
        test_payload = {
            "type": "slack_event",
            "event": {"type": "message", "text": "Hello Integration!"},
        }

        await redis_backend.publish("slack:events", test_payload)

        # Verify message exists in stream
        if redis_backend._client:
            messages = await redis_backend._client.xread(
                streams={"slack:events": "0-0"},
                count=1,
            )
            assert len(messages) == 1
            assert messages[0][0] == b"slack:events"

    @pytest.mark.asyncio
    async def test_publish_multiple_messages(self, redis_backend: RedisMessageQueueBackend) -> None:
        """Test publishing multiple messages."""
        messages_to_send = [
            {"id": 1, "text": "First message"},
            {"id": 2, "text": "Second message"},
            {"id": 3, "text": "Third message"},
        ]

        for msg in messages_to_send:
            await redis_backend.publish("slack:events", msg)

        # Verify all messages are in stream
        if redis_backend._client:
            result = await redis_backend._client.xread(
                streams={"slack:events": "0-0"},
                count=10,
            )
            assert len(result) == 1
            assert len(result[0][1]) == 3

    @pytest.mark.asyncio
    async def test_publish_to_different_streams(self, redis_backend: RedisMessageQueueBackend) -> None:
        """Test publishing to multiple different streams."""
        await redis_backend.publish("slack:events", {"type": "event"})
        await redis_backend.publish("slack:commands", {"type": "command"})
        await redis_backend.publish("slack:actions", {"type": "action"})

        # Verify all streams exist
        if redis_backend._client:
            keys = await redis_backend._client.keys("slack:*")
            assert len(keys) == 3
            assert b"slack:events" in keys
            assert b"slack:commands" in keys
            assert b"slack:actions" in keys

    @pytest.mark.asyncio
    async def test_publish_with_maxlen_trimming(self, redis_connection_url: str) -> None:
        """Test that stream maxlen trimming works."""
        backend = RedisMessageQueueBackend(
            redis_url=redis_connection_url,
            stream_maxlen=50,  # Set maxlen for testing
        )

        await backend._connect()

        # Publish many more messages than maxlen
        for i in range(200):
            await backend.publish("slack:test", {"id": i})

        # Verify stream is trimmed (approximately)
        if backend._client:
            info = await backend._client.xlen("slack:test")
            # With approximate trimming, Redis can be very lenient
            # Just verify it's significantly less than total published
            assert info < 200, f"Expected stream to be trimmed, but got {info} (published 200)"
            # And verify it's at least somewhat close to maxlen
            assert info <= 150, f"Expected stream length <= 150, got {info} (maxlen=50)"

        await backend.close()


class TestRedisIntegrationConsume:
    """Integration tests for message consumption."""

    @pytest.mark.asyncio
    async def test_consume_simple_mode(self, clean_redis_backend: RedisMessageQueueBackend) -> None:
        """Test consuming messages without consumer groups."""
        # First, create the stream by publishing a dummy message
        # This ensures the stream exists before the consumer starts
        await clean_redis_backend.publish("slack:events", {"id": 0, "dummy": True})

        # Consume messages with timeout protection
        consumed_messages: list[dict[str, Any]] = []

        async def consume_messages() -> None:
            consumer = clean_redis_backend.consume()
            try:
                async for msg in consumer:
                    # Skip the dummy message
                    if msg.get("dummy"):
                        continue
                    consumed_messages.append(msg)
                    if len(consumed_messages) >= 2:
                        return  # Exit cleanly
            except asyncio.CancelledError:
                pass

        # Start consumer (it will discover the existing stream)
        task = asyncio.create_task(consume_messages())

        # Give consumer time to start and discover streams
        await asyncio.sleep(0.5)

        # Now publish test messages (consumer will receive them)
        test_messages = [
            {"id": 1, "text": "Message 1"},
            {"id": 2, "text": "Message 2"},
        ]

        for msg in test_messages:
            await clean_redis_backend.publish("slack:events", msg)
            await asyncio.sleep(0.1)  # Small delay between messages

        # Wait for consumption with timeout
        try:
            await asyncio.wait_for(task, timeout=5.0)
        except asyncio.TimeoutError:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
            pytest.fail(f"Timeout waiting for messages. Got {len(consumed_messages)}/2")

        # Verify consumed messages
        assert len(consumed_messages) == 2
        assert consumed_messages[0]["id"] == 1
        assert consumed_messages[1]["id"] == 2

    @pytest.mark.asyncio
    async def test_consume_with_consumer_group(self, clean_redis_backend: RedisMessageQueueBackend) -> None:
        """Test consuming messages with consumer groups."""
        # Publish test message
        test_message = {"id": 1, "text": "Group test"}
        await clean_redis_backend.publish("slack:events", test_message)

        # Consume with consumer group
        consumed_messages: list[dict[str, Any]] = []

        async def consume_with_group() -> None:
            consumer = clean_redis_backend.consume(group="test-group")
            try:
                async for msg in consumer:
                    consumed_messages.append(msg)
                    if len(consumed_messages) >= 1:
                        return  # Exit cleanly
            except asyncio.CancelledError:
                pass

        task = asyncio.create_task(consume_with_group())
        try:
            await asyncio.wait_for(task, timeout=5.0)
        except asyncio.TimeoutError:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
            pytest.fail(f"Timeout waiting for messages. Got {len(consumed_messages)}/1")

        assert len(consumed_messages) == 1
        assert consumed_messages[0]["id"] == 1

        # Verify consumer group was created
        if clean_redis_backend._client:
            groups = await clean_redis_backend._client.xinfo_groups("slack:events")
            # xinfo_groups returns a list of dicts with keys like 'name', 'consumers', etc.
            assert len(groups) >= 1, f"Expected at least 1 group, got {groups}"
            # Extract group names from the response
            group_names = []
            for g in groups:
                if isinstance(g, dict):
                    name = g.get("name") or g.get(b"name")
                    if name:
                        group_names.append(name if isinstance(name, bytes) else name.encode())
            assert b"test-group" in group_names, f"Expected b'test-group' in {group_names}, groups={groups}"

    @pytest.mark.asyncio
    async def test_multiple_consumers_same_group(
        self, clean_redis_backend: RedisMessageQueueBackend, redis_connection_url: str
    ) -> None:
        """Test multiple consumers in the same group distribute work."""
        # Create second backend for second consumer
        backend2 = RedisMessageQueueBackend(redis_url=redis_connection_url)
        await backend2._connect()

        consumed1: list[dict[str, Any]] = []
        consumed2: list[dict[str, Any]] = []

        async def consume1() -> None:
            consumer = clean_redis_backend.consume(group="shared-group")
            try:
                async for msg in consumer:
                    consumed1.append(msg)
            except asyncio.CancelledError:
                pass

        async def consume2() -> None:
            consumer = backend2.consume(group="shared-group")
            try:
                async for msg in consumer:
                    consumed2.append(msg)
            except asyncio.CancelledError:
                pass

        # Start both consumers first
        tasks = [asyncio.create_task(consume1()), asyncio.create_task(consume2())]

        # Give consumers time to start
        await asyncio.sleep(0.5)

        # Now publish messages (consumers will distribute them)
        for i in range(6):
            await clean_redis_backend.publish("slack:events", {"id": i})
            await asyncio.sleep(0.1)  # Small delay between messages

        # Wait a bit for consumption
        await asyncio.sleep(2.0)

        # Cancel consumers
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

        await backend2.close()

        # Verify messages were distributed
        total_consumed = len(consumed1) + len(consumed2)
        assert total_consumed == 6, f"Expected 6 messages, got {total_consumed}"
        # Both consumers should have received at least one message (load distribution)
        assert len(consumed1) > 0, "Consumer 1 received no messages"
        assert len(consumed2) > 0, "Consumer 2 received no messages"

    @pytest.mark.asyncio
    async def test_consume_new_messages_only(self, clean_redis_backend: RedisMessageQueueBackend) -> None:
        """Test that simple consume only gets new messages."""
        # Publish old message
        await clean_redis_backend.publish("slack:events", {"id": 0, "old": True})

        # Start consumer (should read from $ = new messages only)
        consumed: list[dict[str, Any]] = []

        async def consume_task() -> None:
            consumer = clean_redis_backend.consume()
            try:
                async for msg in consumer:
                    consumed.append(msg)
                    if len(consumed) >= 1:
                        return
            except asyncio.CancelledError:
                pass

        task = asyncio.create_task(consume_task())

        # Wait a bit for consumer to start
        await asyncio.sleep(0.5)

        # Publish new message
        await clean_redis_backend.publish("slack:events", {"id": 1, "new": True})

        # Wait for consumption with timeout
        try:
            await asyncio.wait_for(task, timeout=5.0)
        except asyncio.TimeoutError:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
            pytest.fail(f"Timeout waiting for new message. Consumed: {len(consumed)}")

        # Should only get the new message
        assert len(consumed) == 1
        assert consumed[0]["id"] == 1
        assert consumed[0].get("new") is True


class TestRedisIntegrationErrorHandling:
    """Integration tests for error handling scenarios."""

    @pytest.mark.asyncio
    async def test_connection_to_invalid_url(self) -> None:
        """Test connection error with invalid Redis URL."""
        backend = RedisMessageQueueBackend(redis_url="redis://invalid-host:9999/0")

        with pytest.raises(ConnectionError):
            await backend._connect()

    @pytest.mark.asyncio
    async def test_publish_without_connection(self, redis_connection_url: str) -> None:
        """Test publishing without establishing connection."""
        backend = RedisMessageQueueBackend(redis_url=redis_connection_url)

        # Don't connect, but publish should auto-connect
        await backend.publish("slack:test", {"id": 1})

        # Should be connected now
        assert backend._connected

        await backend.close()

    @pytest.mark.asyncio
    async def test_consume_handles_stream_deletion(self, redis_backend: RedisMessageQueueBackend) -> None:
        """Test that consume handles stream deletion gracefully."""
        await redis_backend.publish("slack:events", {"id": 1})

        consumed: list[dict[str, Any]] = []

        async def consume_task() -> None:
            consumer = redis_backend.consume()
            try:
                async for msg in consumer:
                    consumed.append(msg)
                    if len(consumed) >= 2:
                        return
            except asyncio.CancelledError:
                pass

        task = asyncio.create_task(consume_task())
        await asyncio.sleep(0.5)

        # Delete stream while consuming
        if redis_backend._client:
            await redis_backend._client.delete("slack:events")

        # Publish new message (recreates stream)
        await redis_backend.publish("slack:events", {"id": 2})

        try:
            await asyncio.wait_for(task, timeout=5.0)
        except asyncio.TimeoutError:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        # Should handle gracefully
        assert len(consumed) >= 1


class TestRedisIntegrationEndToEnd:
    """End-to-end integration tests."""

    @pytest.mark.asyncio
    async def test_full_publish_consume_workflow(self, clean_redis_backend: RedisMessageQueueBackend) -> None:
        """Test complete workflow: publish -> consume -> acknowledge."""
        # Simulate real Abstract Backend event
        slack_event = {
            "token": "test-token",
            "team_id": "T123456",
            "event": {
                "type": "message",
                "user": "U123456",
                "text": "Hello from integration test!",
                "ts": "1234567890.123456",
                "channel": "C123456",
            },
            "type": "event_callback",
            "event_id": "Ev123456",
        }

        # Publish event
        await clean_redis_backend.publish("slack:events", slack_event)

        # Consume with consumer group
        consumed: list[dict[str, Any]] = []

        async def worker() -> None:
            consumer = clean_redis_backend.consume(group="workers")
            try:
                async for msg in consumer:
                    consumed.append(msg)
                    if len(consumed) >= 1:
                        return
            except asyncio.CancelledError:
                pass

        task = asyncio.create_task(worker())
        try:
            await asyncio.wait_for(task, timeout=5.0)
        except asyncio.TimeoutError:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
            pytest.fail("Failed to consume message")

        # Verify complete event structure
        assert len(consumed) == 1
        received_event = consumed[0]
        assert received_event["team_id"] == "T123456"
        assert received_event["event"]["type"] == "message"
        assert received_event["event"]["text"] == "Hello from integration test!"

    @pytest.mark.asyncio
    async def test_concurrent_publishers_and_consumers(self, redis_connection_url: str) -> None:
        """Test system under concurrent load."""
        backend = RedisMessageQueueBackend(redis_url=redis_connection_url)
        await backend._connect()

        # Clean up first
        if backend._client:
            keys = await backend._client.keys("slack:*")
            if keys:
                await backend._client.delete(*keys)

        published_count = 0
        consumed_messages: list[dict[str, Any]] = []

        async def publisher(publisher_id: int) -> None:
            nonlocal published_count
            for i in range(5):
                await backend.publish(
                    "slack:events",
                    {"publisher": publisher_id, "message": i},
                )
                published_count += 1
                await asyncio.sleep(0.01)

        async def consumer() -> None:
            consumer_gen = backend.consume(group="load-test")
            try:
                async for msg in consumer_gen:
                    consumed_messages.append(msg)
                    if len(consumed_messages) >= 15:  # 3 publishers * 5 messages
                        return
            except asyncio.CancelledError:
                pass

        # Run 3 concurrent publishers and 1 consumer
        tasks = [
            asyncio.create_task(publisher(1)),
            asyncio.create_task(publisher(2)),
            asyncio.create_task(publisher(3)),
            asyncio.create_task(consumer()),
        ]

        try:
            await asyncio.wait_for(
                asyncio.gather(*tasks),
                timeout=10.0,
            )
        except asyncio.TimeoutError:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            pytest.fail(f"Timeout - published: {published_count}, consumed: {len(consumed_messages)}")
        finally:
            await backend.close()

        # Verify all messages were consumed
        assert published_count == 15
        assert len(consumed_messages) == 15

        # Verify messages from all publishers
        publishers = {msg["publisher"] for msg in consumed_messages}
        assert publishers == {1, 2, 3}
