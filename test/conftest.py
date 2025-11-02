"""Pytest configuration and shared fixtures for all tests.

This module provides common fixtures and configuration for both unit
and integration tests.
"""

import asyncio
import logging
import os
from typing import Generator

import pytest
from testcontainers.redis import RedisContainer

# Configure logging for tests
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)


@pytest.fixture(scope="session")
def event_loop_policy() -> asyncio.AbstractEventLoopPolicy:
    """Use the default event loop policy for all tests."""
    return asyncio.get_event_loop_policy()


@pytest.fixture(scope="function")
def event_loop(event_loop_policy: asyncio.AbstractEventLoopPolicy) -> Generator:
    """Create an event loop for each test function."""
    loop = event_loop_policy.new_event_loop()
    yield loop
    loop.close()


@pytest.fixture(autouse=True)
def reset_env_vars() -> Generator:
    """Reset environment variables before each test.

    This ensures tests don't interfere with each other through
    environment variable side effects.
    """
    # Save current environment
    original_env = dict(os.environ)

    yield

    # Restore original environment
    os.environ.clear()
    os.environ.update(original_env)


@pytest.fixture
def redis_url() -> str:
    """Provide Redis URL for tests."""
    return os.getenv("REDIS_URL", "redis://localhost:6379/1")


@pytest.fixture
def mock_slack_event() -> dict:
    """Provide a sample Abstract Backend event for testing."""
    return {
        "token": "test-token",
        "team_id": "T123456",
        "api_app_id": "A123456",
        "event": {
            "type": "message",
            "user": "U123456",
            "text": "Hello, World!",
            "ts": "1234567890.123456",
            "channel": "C123456",
            "event_ts": "1234567890.123456",
        },
        "type": "event_callback",
        "event_id": "Ev123456",
        "event_time": 1234567890,
    }


@pytest.fixture
def mock_slack_command() -> dict:
    """Provide a sample Abstract Backend command for testing."""
    return {
        "token": "test-token",
        "team_id": "T123456",
        "team_domain": "test-team",
        "channel_id": "C123456",
        "channel_name": "general",
        "user_id": "U123456",
        "user_name": "testuser",
        "command": "/test",
        "text": "hello",
        "response_url": "https://hooks.slack.com/commands/123/456",
        "trigger_id": "123.456.789",
    }


# ============================================================================
# Integration Test Fixtures (Testcontainers)
# ============================================================================


@pytest.fixture(scope="session")
def redis_container() -> Generator[RedisContainer, None, None]:
    """Provide a Redis container for integration tests (session-scoped).

    This fixture starts a Redis container once per test session and reuses it
    across all integration tests for better performance. The container is
    automatically cleaned up after all tests complete.
    Yields:
        RedisContainer: Running Redis container instance

    Example:
        >>> def test_something(redis_container):
        ...     host = redis_container.get_container_host_ip()
        ...     port = redis_container.get_exposed_port(6379)
        ...     redis_url = f"redis://{host}:{port}/0"
    """
    # Start Redis container with Redis 7
    container = RedisContainer(image="redis:7-alpine")
    container.start()

    try:
        # Wait for Redis to be ready
        client = container.get_client()
        client.ping()

        host = container.get_container_host_ip()
        port = container.get_exposed_port(6379)
        logging.info("Redis container started: redis://%s:%s/0", host, port)

        yield container
    finally:
        # Cleanup: stop and remove container
        container.stop()
        logging.info("Redis container stopped")


@pytest.fixture(scope="session")
def redis_connection_url(redis_container: RedisContainer) -> str:
    """Provide Redis connection URL from the container (session-scoped).

    Args:
        redis_container: Redis container fixture

    Returns:
        Redis connection URL string (format: redis://host:port/0)

    Example:
        >>> def test_connection(redis_connection_url):
        ...     backend = RedisMessageQueueBackend(redis_url=redis_connection_url)
        ...     await backend._connect()
    """
    host = redis_container.get_container_host_ip()
    port = redis_container.get_exposed_port(6379)
    return f"redis://{host}:{port}/0"
