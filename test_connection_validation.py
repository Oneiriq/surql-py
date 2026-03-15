"""Standalone script to validate database connection with provided credentials.

This script tests the database connection by:
1. Loading configuration from .env file
2. Creating a DatabaseClient instance
3. Attempting to connect to the database
4. Verifying authentication succeeds
5. Verifying namespace and database selection works
6. Performing a simple INFO FOR DB query
7. Reporting results and handling errors
8. Closing the connection cleanly
"""

import asyncio
import sys
from typing import Any

from surql.connection.client import ConnectionError, DatabaseClient, QueryError
from surql.settings import get_db_config


def print_header(text: str) -> None:
    """Print a formatted header."""
    print(f"\n{'=' * 60}")
    print(f"  {text}")
    print(f"{'=' * 60}\n")


def print_success(text: str) -> None:
    """Print a success message."""
    print(f"[OK] {text}")


def print_error(text: str) -> None:
    """Print an error message."""
    print(f"[ERROR] {text}")


def print_info(text: str) -> None:
    """Print an info message."""
    print(f"  {text}")


def format_query_result(result: Any) -> str:
    """Format query result for display."""
    if result is None:
        return "None"
    if isinstance(result, (list, tuple)) and len(result) > 0:
        return f"{type(result).__name__} with {len(result)} item(s)"
    return repr(result)


async def validate_connection() -> bool:
    """Validate database connection with loaded credentials.

    Returns:
        bool: True if validation succeeds, False otherwise
    """
    print_header("Database Connection Validation")

    # Step 1: Load configuration
    print_info("Step 1: Loading configuration from .env file...")
    try:
        config = get_db_config()
        print_success("Configuration loaded successfully")
        print_info(f"  URL: {config.url}")
        print_info(f"  Namespace: {config.namespace}")
        print_info(f"  Database: {config.database}")
        print_info(f"  Username: {config.username or '(not set)'}")
        print_info(f"  Password: {'*' * len(config.password) if config.password else '(not set)'}")
        print_info(f"  Max Connections: {config.max_connections}")
        print_info(f"  Timeout: {config.timeout}s")
        print_info(f"  Retry Attempts: {config.retry_max_attempts}")
    except Exception as e:
        print_error(f"Failed to load configuration: {e}")
        return False

    # Step 2: Create DatabaseClient instance
    print_info("\nStep 2: Creating DatabaseClient instance...")
    try:
        client = DatabaseClient(config)
        print_success("DatabaseClient instance created")
    except Exception as e:
        print_error(f"Failed to create DatabaseClient: {e}")
        return False

    # Step 3-5: Connect to database (includes authentication and namespace/db selection)
    print_info("\nStep 3-5: Connecting to database...")
    print_info("  (This includes authentication and namespace/database selection)")
    try:
        await client.connect()
        print_success("Connected to database successfully")
        print_success("Authentication succeeded")
        print_success(f"Namespace '{config.namespace}' and database '{config.database}' selected")
    except ConnectionError as e:
        print_error(f"Connection failed: {e}")
        return False
    except Exception as e:
        print_error(f"Unexpected error during connection: {e}")
        return False

    # Step 6: Verify connection with INFO FOR DB query
    print_info("\nStep 6: Verifying connection with INFO FOR DB query...")
    try:
        result = await client.execute("INFO FOR DB")
        print_success("Query executed successfully")
        print_info(f"  Result type: {format_query_result(result)}")

        # Try to extract useful info from the result
        if result and isinstance(result, list) and len(result) > 0:
            db_info = result[0]
            if isinstance(db_info, dict):
                print_info("\n  Database Information:")
                if 'result' in db_info:
                    info_result = db_info['result']
                    if isinstance(info_result, dict):
                        for key in ['dl', 'dt', 'fc', 'pa', 'sc', 'tb']:
                            if key in info_result:
                                print_info(f"    {key}: {info_result[key]}")
    except QueryError as e:
        print_error(f"Query execution failed: {e}")
        await client.disconnect()
        return False
    except Exception as e:
        print_error(f"Unexpected error during query: {e}")
        await client.disconnect()
        return False

    # Step 7: Test basic SELECT operation
    print_info("\nStep 7: Testing basic SELECT operation...")
    try:
        # Try to select from a system table or just verify the operation works
        result = await client.execute("SELECT * FROM $auth LIMIT 1")
        print_success("SELECT query executed successfully")
        print_info(f"  Result: {format_query_result(result)}")
    except QueryError as e:
        # This might fail if there's no data, which is okay
        print_info(f"  SELECT query completed (might be empty): {e}")
    except Exception as e:
        print_error(f"Unexpected error during SELECT: {e}")

    # Step 8: Disconnect cleanly
    print_info("\nStep 8: Disconnecting from database...")
    try:
        await client.disconnect()
        print_success("Disconnected cleanly")
    except Exception as e:
        print_error(f"Disconnect failed: {e}")
        return False

    return True


async def main() -> int:
    """Main entry point for the validation script.

    Returns:
        int: Exit code (0 for success, 1 for failure)
    """
    try:
        success = await validate_connection()

        print_header("Validation Result")
        if success:
            print_success("All connection validation steps passed!")
            print_info("\nThe database connection is properly configured and working.")
            return 0
        else:
            print_error("Connection validation failed!")
            print_info("\nPlease check the error messages above and verify your .env file.")
            return 1

    except KeyboardInterrupt:
        print_error("\nValidation interrupted by user")
        return 130
    except Exception as e:
        print_error(f"Unexpected error during validation: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)
