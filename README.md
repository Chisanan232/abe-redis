# Redis Message Queue Backend for Abstract Backend

A production-ready Redis Streams-based message queue backend implementation for the [Abstract Backend](https://github.com/Chisanan232/abstract-backend) project. This backend extends message queue capabilities to support Redis, enabling reliable, scalable, and high-performance Abstract Backend event processing.

## Status & Quality

### CI/CD & Testing
[![CI](https://github.com/Chisanan232/abe-redis/actions/workflows/ci.yaml/badge.svg)](https://github.com/Chisanan232/abe-redis/actions/workflows/ci.yaml)
[![Documentation](https://github.com/Chisanan232/abe-redis/actions/workflows/documentation.yaml/badge.svg)](https://github.com/Chisanan232/abe-redis/actions/workflows/documentation.yaml)
[![Documentation Build Check](https://github.com/Chisanan232/abe-redis/actions/workflows/docs-build-check.yaml/badge.svg)](https://github.com/Chisanan232/abe-redis/actions/workflows/docs-build-check.yaml)

### Code Coverage & Quality
[![codecov](https://codecov.io/gh/Chisanan232/abe-redis/branch/master/graph/badge.svg)](https://codecov.io/gh/Chisanan232/abe-redis)
[![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=Chisanan232_abe-redis&metric=alert_status)](https://sonarcloud.io/summary/new_code?id=Chisanan232_abe-redis)
[![Reliability Rating](https://sonarcloud.io/api/project_badges/measure?project=Chisanan232_abe-redis&metric=reliability_rating)](https://sonarcloud.io/summary/new_code?id=Chisanan232_abe-redis)
[![Security Rating](https://sonarcloud.io/api/project_badges/measure?project=Chisanan232_abe-redis&metric=security_rating)](https://sonarcloud.io/summary/new_code?id=Chisanan232_abe-redis)
[![Maintainability Rating](https://sonarcloud.io/api/project_badges/measure?project=Chisanan232_abe-redis&metric=sqale_rating)](https://sonarcloud.io/summary/new_code?id=Chisanan232_abe-redis)

### Code Style & Standards
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![linting: pylint](https://img.shields.io/badge/linting-pylint-yellowgreen)](https://github.com/pylint-dev/pylint)
[![Imports: isort](https://img.shields.io/badge/%20imports-isort-%231674b1?style=flat&labelColor=ef8336)](https://pycqa.github.io/isort/)
[![Type Checked: mypy](https://img.shields.io/badge/type%20checked-mypy-blue.svg)](http://mypy-lang.org/)

### Package Info
[![PyPI version](https://badge.fury.io/py/abe-redis.svg)](https://badge.fury.io/py/abe-redis)
[![Supported Versions](https://img.shields.io/pypi/pyversions/abe-redis.svg?logo=python&logoColor=FBE072)](https://pypi.org/project/abe-redis)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

### Downloads
[![Downloads](https://pepy.tech/badge/abe-redis)](https://pepy.tech/project/abe-redis)
[![Downloads/Month](https://pepy.tech/badge/abe-redis/month)](https://pepy.tech/project/abe-redis)
[![Downloads/Week](https://pepy.tech/badge/abe-redis/week)](https://pepy.tech/project/abe-redis)

---

## Overview

**abe-redis** is a Redis Streams-based message queue backend that integrates seamlessly with the Abstract Backend's component loading mechanism. It provides a reliable, scalable solution for handling Abstract Backend events using Redis as the message queue infrastructure.

### Key Features

- 🔌 **Plug-and-Play**: Install via pip and configure with environment variables
- ⚡ **Redis Streams**: Modern stream-based message processing with consumer groups
- 🚀 **Production Ready**: Connection pooling, error handling, and retry logic built-in
- 🔄 **Async-First**: Built for modern Python async/await patterns
- 📦 **Universal Compatibility**: Works with any project using the same component loading mechanism
- 🧪 **Well Tested**: Comprehensive unit and integration tests with high coverage
- 📚 **Fully Documented**: Complete API reference and usage examples

## Python Version Support

Python 3.12, 3.13


## Quick Start

### Installation

Install the package via pip:

```bash
pip install abe-redis
```

### Configuration

Configure your environment to use Redis as the message queue backend:

```bash
export QUEUE_BACKEND=redis
export REDIS_URL=redis://localhost:6379/0
```

### Start Redis

Using Docker (recommended):

```bash
docker run -d --name test-redis -p 6379:6379 redis:7-alpine
```

### Basic Usage

```python
import asyncio
from abe_plugin.backends.queue import RedisMessageQueueBackend

async def main():
    # Create backend from environment variables
    backend = RedisMessageQueueBackend.from_env()

    # Publish a message
    await backend.publish("slack:events", {"type": "message", "text": "Hello Redis!"})

    # Consume messages
    async for message in backend.consume():
        print(f"Received: {message}")
        break

    await backend.close()

asyncio.run(main())
```

## Architecture

The Redis backend uses **Redis Streams** for reliable message queueing:

- **Persistent Storage**: Messages stored in Redis Streams with configurable retention
- **Consumer Groups**: Distributed consumption across multiple workers
- **Automatic Acknowledgment**: Messages acknowledged after successful processing
- **Stream Pattern Matching**: Automatically discovers and consumes from `slack:*` streams
- **Connection Pooling**: Efficient connection management with configurable pool size

## Configuration Options

| Environment Variable | Required | Default | Description |
|---------------------|----------|---------|-------------|
| `QUEUE_BACKEND` | Yes | - | Must be set to `redis` |
| `REDIS_URL` | Yes | `redis://localhost:6379/0` | Redis connection URL |
| `REDIS_PASSWORD` | No | None | Redis authentication password |
| `REDIS_SSL` | No | `false` | Enable SSL/TLS connection |
| `REDIS_MAX_CONNECTIONS` | No | `10` | Maximum connection pool size |
| `REDIS_STREAM_MAXLEN` | No | `10000` | Maximum stream length for trimming |

## Documentation

For comprehensive documentation, including API references, examples, and development guides:

📚 **[Full Documentation](https://chisanan232.github.io/abe-redis/)**

### Quick Links

- [Introduction](https://chisanan232.github.io/abe-redis/docs/introduction)
- [Quick Start Guide](https://chisanan232.github.io/abe-redis/docs/quick-start/quick-start)
- [Installation Guide](https://chisanan232.github.io/abe-redis/docs/quick-start/installation)
- [API Reference](https://chisanan232.github.io/abe-redis/docs/api-references/api-references)
- [Development Guide](https://chisanan232.github.io/abe-redis/dev/development)
- [CI/CD Documentation](https://chisanan232.github.io/abe-redis/dev/ci-cd/)

## Development

### Prerequisites

- Python 3.12 or 3.13
- Redis 6.0+ (7.0+ recommended)
- uv or pip package manager

### Setup Development Environment

```bash
# Clone the repository
git clone https://github.com/Chisanan232/abe-redis.git
cd abe-redis

# Install dependencies
uv sync

# Start Redis for testing
docker run -d --name redis-dev -p 6379:6379 redis:7-alpine

# Run tests
uv run pytest

# Run tests with coverage
uv run pytest --cov=abe_plugin --cov-report=html
```

### Code Quality

The project follows strict code quality standards:

- **Code Formatting**: [black](https://github.com/psf/black)
- **Linting**: [pylint](https://github.com/pylint-dev/pylint)
- **Import Sorting**: [isort](https://pycqa.github.io/isort/)
- **Type Checking**: [mypy](http://mypy-lang.org/)

Run quality checks:

```bash
# Format code
uv run black abe_plugin/ test/

# Lint code
uv run pylint abe_plugin/

# Sort imports
uv run isort abe_plugin/ test/

# Type check
uv run mypy abe_plugin/
```

## Contributing

We welcome contributions! Please see our [Contributing Guide](https://chisanan232.github.io/abe-redis/docs/contribute/contribute) for details.

### How to Contribute

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Make your changes
4. Run tests and quality checks
5. Commit your changes (`git commit -m 'feat: add amazing feature'`)
6. Push to the branch (`git push origin feature/amazing-feature`)
7. Open a Pull Request

## License

This project is licensed under the MIT License - see the [LICENSE](./LICENSE) file for details.

## Links

- **Documentation**: https://chisanan232.github.io/abe-redis/
- **PyPI Package**: https://pypi.org/project/abe-redis/
- **Source Code**: https://github.com/Chisanan232/abe-redis
- **Issue Tracker**: https://github.com/Chisanan232/abe-redis/issues
- **Abstract Backend**: https://github.com/Chisanan232/abstract-backend

## Support

If you encounter any issues or have questions:

- 📖 Check the [Documentation](https://chisanan232.github.io/abe-redis/)
- 🐛 Report bugs via [GitHub Issues](https://github.com/Chisanan232/abe-redis/issues)
- 💬 Ask questions in [GitHub Discussions](https://github.com/Chisanan232/abe-redis/discussions)

---

**Made with ❤️ for the Abstract Backend ecosystem**
