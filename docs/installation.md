# Installation Guide

This guide covers installing Ethereal and setting up SurrealDB for development.

## Table of Contents

- [Requirements](#requirements)
- [Installing Ethereal](#installing-ethereal)
- [Installing SurrealDB](#installing-surrealdb)
- [Configuration](#configuration)
- [Verification](#verification)
- [Next Steps](#next-steps)

## Requirements

### Python

- Python 3.12 or higher
- pip or [uv](https://github.com/astral-sh/uv) package manager

Check your Python version:

```shell
python --version
# or
python3 --version
```

### SurrealDB

- SurrealDB 1.0 or higher

## Installing Ethereal

### Using pip

```shell
pip install ethereal
```

### Using uv (Recommended)

[uv](https://github.com/astral-sh/uv) is a fast Python package installer and resolver.

```shell
# Install uv if you haven't already
pip install uv

# Add ethereal to your project
uv add ethereal
```

### From Source

For development or to use the latest features:

```shell
# Clone the repository
git clone https://github.com/yourusername/ethereal.git
cd ethereal

# Install with uv
uv sync

# Or with pip
pip install -e .
```

## Installing SurrealDB

### macOS

Using Homebrew:

```shell
brew install surrealdb/tap/surreal
```

### Linux

```shell
curl -sSf https://install.surrealdb.com | sh
```

### Windows

Using PowerShell:

```powershell
iwr https://windows.surrealdb.com -useb | iex
```

Using WSL or Git Bash:

```shell
curl -sSf https://install.surrealdb.com | sh
```

### Docker

```shell
docker pull surrealdb/surrealdb:latest
```

### Verify SurrealDB Installation

```shell
surreal version
```

You should see output similar to:

```shell
surreal 1.0.0 for linux on x86_64
```

## Running SurrealDB

### Local Development

Start SurrealDB in memory mode (data is not persisted):

```shell
surreal start --log trace --user root --pass root memory
```

Start with file storage:

```shell
surreal start --log trace --user root --pass root file://mydb.db
```

Start with RocksDB (recommended for production):

```shell
surreal start --log trace --user root --pass root rocksdb://mydb
```

### Using Docker

```shell
docker run --rm --pull always -p 8000:8000 \
  surrealdb/surrealdb:latest \
  start --user root --pass root memory
```

With persistent storage:

```shell
docker run --rm --pull always -p 8000:8000 \
  -v $(pwd)/mydb:/mydb \
  surrealdb/surrealdb:latest \
  start --user root --pass root file://mydb/db.surreal
```

### Using Docker Compose

Create a `docker-compose.yml` file:

```yaml
version: '3.8'

services:
  surrealdb:
    image: surrealdb/surrealdb:latest
    ports:
      - "8000:8000"
    command: start --user root --pass root memory
    # For persistent storage:
    # volumes:
    #   - ./data:/data
    # command: start --user root --pass root file://data/db.surreal
```

Start the service:

```shell
docker-compose up -d
```

## Configuration

### Environment Variables

Create a `.env` file in your project root:

```env
# Database Connection
SURREAL_URL=ws://localhost:8000/rpc
SURREAL_NAMESPACE=test
SURREAL_DATABASE=test
SURREAL_USERNAME=root
SURREAL_PASSWORD=root

# Connection Pool
SURREAL_MAX_CONNECTIONS=10

# Retry Configuration
SURREAL_RETRY_MAX_ATTEMPTS=3
SURREAL_RETRY_MIN_WAIT=1.0
SURREAL_RETRY_MAX_WAIT=10.0
SURREAL_RETRY_MULTIPLIER=2.0

# Logging
LOG_LEVEL=INFO
```

### Python Configuration

Create a configuration file in your project:

```python
from src.connection.config import ConnectionConfig

config = ConnectionConfig(
  url='ws://localhost:8000/rpc',
  namespace='test',
  database='test',
  username='root',
  password='root',
  max_connections=10,
)
```

### Loading from Environment

Ethereal automatically loads configuration from environment variables:

```python
from src.settings import get_db_config

# Loads from environment variables
config = get_db_config()
```

## Verification

### Verify Ethereal Installation

```shell
ethereal --help
```

You should see the CLI help output:

```shell
Usage: ethereal [OPTIONS] COMMAND [ARGS]...

  Ethereal - Code-first database toolkit for SurrealDB.

Options:
  --verbose, -v  Enable verbose logging
  --help         Show this message and exit.

Commands:
  db       Database management commands
  migrate  Database migration commands
  schema   Schema inspection commands
  version  Show ethereal version information
```

### Test Database Connection

Create a test script `test_connection.py`:

```python
import asyncio
from src.connection.client import get_client
from src.connection.config import ConnectionConfig

async def test_connection():
  config = ConnectionConfig(
    url='ws://localhost:8000/rpc',
    namespace='test',
    database='test',
    username='root',
    password='root',
  )

  async with get_client(config) as client:
    result = await client.execute('SELECT * FROM $session')
    print('Connected successfully!')
    print(f'Session info: {result}')

if __name__ == '__main__':
  asyncio.run(test_connection())
```

Run the test:

```shell
python test_connection.py
```

### Using the CLI

```shell
# Check database connection
ethereal db ping

# Show database info
ethereal db info
```

## Project Setup

### Initialize Migration Directory

Create a migrations directory in your project:

```shell
mkdir migrations
```

### Create First Migration

```shell
ethereal migrate create "Initial setup"
```

This creates a migration file in `migrations/` with the current timestamp.

### Project Structure

Your project should look like this:

```shell
my-project/
├── .env                 # Environment variables
├── migrations/          # Migration files
│   └── 20260102_120000_initial_setup.py
├── schemas/             # Schema definitions (optional)
│   └── user.py
└── main.py             # Your application
```

## Troubleshooting

### Connection Refused

If you get a connection refused error:

1. Ensure SurrealDB is running:

    ```shell
    surreal version
    ```

2. Check the URL matches your SurrealDB instance:

    ```shell
    # Default is ws://localhost:8000/rpc
    ```

3. Verify port 8000 is not blocked by firewall

### Import Errors

If you get import errors:

```shell
# Reinstall ethereal
pip install --force-reinstall ethereal

# Or with uv
uv sync --reinstall
```

### Permission Errors

If you get permission errors on Linux/macOS:

```shell
# Run with sudo (not recommended for pip)
sudo pip install ethereal

# Or use virtual environment (recommended)
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install ethereal
```

### SurrealDB Not Found

If `surreal` command is not found after installation:

1. Check if the binary is in your PATH:

   ```shell
   which surreal  # Linux/macOS
   where surreal  # Windows
   ```

2. Add to PATH or use full path to binary

3. On Windows, restart your terminal after installation

## Next Steps

Now that you have Ethereal installed and configured:

1. Follow the [Quick Start Tutorial](quickstart.md) to create your first schema and migration
2. Read the [Schema Definition Guide](schema.md) to learn about schema features
3. Explore the [Query Builder & ORM Guide](queries.md) for data operations
4. Check out the [Examples](examples/) for working code samples

## Additional Resources

- [SurrealDB Documentation](https://surrealdb.com/docs)
- [SurrealDB Installation](https://surrealdb.com/install)
- [Ethereal GitHub Repository](https://github.com/yourusername/ethereal)
- [Python Virtual Environments](https://docs.python.org/3/tutorial/venv.html)
