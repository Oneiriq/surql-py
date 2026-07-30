"""Bucket / file object-storage CLI commands.

Provides a ``bucket`` command group mirroring the ``schema`` group: bucket DDL
(``define`` / ``list`` / ``rm``) plus file operations (``put`` / ``get`` /
``delete`` / ``exists`` / ``files``).

These commands require the SurrealDB server to be started with the experimental
``files`` capability enabled (``surreal start --allow-experimental files``).
"""

import asyncio
from pathlib import Path
from typing import Annotated, Any

import structlog
import typer

from surql.cli.common import (
  OutputFormat,
  display_error,
  display_info,
  display_success,
  format_output,
  handle_error,
  spinner,
  verbose_option,
)
from surql.connection.client import ConnectionError as DBConnectionError
from surql.connection.client import get_client
from surql.schema.bucket import bucket_schema, file_bucket, memory_bucket
from surql.schema.parser import parse_db_buckets
from surql.schema.sql import generate_bucket_sql, generate_remove_bucket_sql
from surql.settings import get_db_config

logger = structlog.get_logger(__name__)

app = typer.Typer(
  name='bucket',
  help='Bucket and file object-storage commands',
  no_args_is_help=True,
)


@app.command('define')
def define_bucket(
  name: Annotated[str, typer.Argument(help='Bucket name')],
  backend: Annotated[
    str,
    typer.Option('--backend', '-b', help="Backend: 'memory', a path, or 's3://...'"),
  ] = 'memory',
  path: Annotated[
    str | None,
    typer.Option('--path', help='Local folder path (builds a file:<path> backend)'),
  ] = None,
  readonly: Annotated[bool, typer.Option('--readonly', help='Define as read-only')] = False,
  comment: Annotated[str | None, typer.Option('--comment', help='Bucket comment')] = None,
  if_not_exists: Annotated[bool, typer.Option('--if-not-exists', help='Add IF NOT EXISTS')] = False,
  verbose: Annotated[bool, verbose_option] = False,
) -> None:
  """Define a bucket (DEFINE BUCKET).

  Examples:
    Define a memory-backed bucket:
    $ surql bucket define avatars

    Define a file-backed bucket:
    $ surql bucket define archive --path /srv/data/archive --readonly
  """
  try:
    if path is not None:
      definition = file_bucket(name, path, readonly=readonly, comment=comment)
    elif backend == 'memory':
      definition = memory_bucket(name, readonly=readonly, comment=comment)
    else:
      definition = bucket_schema(name, backend=backend, readonly=readonly, comment=comment)

    statement = generate_bucket_sql(definition, if_not_exists=if_not_exists)[0]
    asyncio.run(_run_statement(statement, verbose))
    display_success(f'Bucket {name!r} defined')
  except DBConnectionError as e:
    display_error(f'Connection failed: {e}')
    raise typer.Exit(1) from e
  except Exception as e:
    handle_error(e, verbose)
    raise typer.Exit(1) from e


@app.command('list')
def list_buckets(
  output_format: Annotated[OutputFormat, typer.Option('--format', '-f')] = OutputFormat.TABLE,
  verbose: Annotated[bool, verbose_option] = False,
) -> None:
  """List buckets defined in the database.

  Examples:
    List buckets:
    $ surql bucket list
  """
  try:
    asyncio.run(_list_buckets_async(output_format, verbose))
  except DBConnectionError as e:
    display_error(f'Connection failed: {e}')
    raise typer.Exit(1) from e
  except Exception as e:
    handle_error(e, verbose)
    raise typer.Exit(1) from e


@app.command('rm')
def remove_bucket(
  name: Annotated[str, typer.Argument(help='Bucket name')],
  if_exists: Annotated[bool, typer.Option('--if-exists', help='Add IF EXISTS')] = False,
  verbose: Annotated[bool, verbose_option] = False,
) -> None:
  """Remove a bucket (REMOVE BUCKET).

  Examples:
    Remove a bucket:
    $ surql bucket rm avatars
  """
  try:
    statement = generate_remove_bucket_sql(name, if_exists=if_exists)[0]
    asyncio.run(_run_statement(statement, verbose))
    display_success(f'Bucket {name!r} removed')
  except DBConnectionError as e:
    display_error(f'Connection failed: {e}')
    raise typer.Exit(1) from e
  except Exception as e:
    handle_error(e, verbose)
    raise typer.Exit(1) from e


@app.command('put')
def put_file(
  bucket: Annotated[str, typer.Argument(help='Bucket name')],
  key: Annotated[str, typer.Argument(help='File key within the bucket')],
  file: Annotated[
    Path | None,
    typer.Option('--file', '-F', help='Read content from this local file (binary)'),
  ] = None,
  text: Annotated[
    str | None,
    typer.Option('--text', '-t', help='Use this literal string as content'),
  ] = None,
  verbose: Annotated[bool, verbose_option] = False,
) -> None:
  """Write a file into a bucket (type::file($b,$k).put($data)).

  Provide content via --file (binary) or --text (literal string).

  Examples:
    $ surql bucket put avatars alice.png --file ./alice.png
    $ surql bucket put notes hello.txt --text "hello world"
  """
  try:
    if file is not None and text is not None:
      display_error('Provide only one of --file or --text')
      raise typer.Exit(1)
    if file is None and text is None:
      display_error('Provide --file or --text for the content')
      raise typer.Exit(1)

    data: str | bytes = file.read_bytes() if file is not None else (text or '')
    asyncio.run(_put_file_async(bucket, key, data, verbose))
    display_success(f'Wrote {bucket}:/{key}')
  except typer.Exit:
    raise
  except DBConnectionError as e:
    display_error(f'Connection failed: {e}')
    raise typer.Exit(1) from e
  except Exception as e:
    handle_error(e, verbose)
    raise typer.Exit(1) from e


@app.command('get')
def get_file(
  bucket: Annotated[str, typer.Argument(help='Bucket name')],
  key: Annotated[str, typer.Argument(help='File key within the bucket')],
  output: Annotated[
    Path | None,
    typer.Option('--output', '-o', help='Write bytes to this file (default: print as text)'),
  ] = None,
  verbose: Annotated[bool, verbose_option] = False,
) -> None:
  """Read a file from a bucket (type::file($b,$k).get()).

  Without --output the content is printed as text; with --output the raw bytes
  are written to the given path.

  Examples:
    $ surql bucket get notes hello.txt
    $ surql bucket get avatars alice.png --output ./alice.png
  """
  try:
    data = asyncio.run(_get_file_async(bucket, key, verbose))
    if data is None:
      display_error(f'File not found: {bucket}:/{key}')
      raise typer.Exit(1)
    if output is not None:
      output.write_bytes(data)
      display_success(f'Wrote {len(data)} bytes to {output}')
    else:
      typer.echo(data.decode('utf-8', errors='replace'))
  except typer.Exit:
    raise
  except DBConnectionError as e:
    display_error(f'Connection failed: {e}')
    raise typer.Exit(1) from e
  except Exception as e:
    handle_error(e, verbose)
    raise typer.Exit(1) from e


@app.command('delete')
def delete_file(
  bucket: Annotated[str, typer.Argument(help='Bucket name')],
  key: Annotated[str, typer.Argument(help='File key within the bucket')],
  verbose: Annotated[bool, verbose_option] = False,
) -> None:
  """Delete a file from a bucket (type::file($b,$k).delete()).

  Examples:
    $ surql bucket delete notes hello.txt
  """
  try:
    asyncio.run(_delete_file_async(bucket, key, verbose))
    display_success(f'Deleted {bucket}:/{key}')
  except DBConnectionError as e:
    display_error(f'Connection failed: {e}')
    raise typer.Exit(1) from e
  except Exception as e:
    handle_error(e, verbose)
    raise typer.Exit(1) from e


@app.command('exists')
def file_exists(
  bucket: Annotated[str, typer.Argument(help='Bucket name')],
  key: Annotated[str, typer.Argument(help='File key within the bucket')],
  verbose: Annotated[bool, verbose_option] = False,
) -> None:
  """Check whether a file exists (type::file($b,$k).exists()).

  Exits 0 if the file exists, 1 if it does not.

  Examples:
    $ surql bucket exists notes hello.txt
  """
  try:
    present = asyncio.run(_file_exists_async(bucket, key, verbose))
    if present:
      display_info(f'{bucket}:/{key} exists')
    else:
      display_info(f'{bucket}:/{key} does not exist')
      raise typer.Exit(1)
  except typer.Exit:
    raise
  except DBConnectionError as e:
    display_error(f'Connection failed: {e}')
    raise typer.Exit(1) from e
  except Exception as e:
    handle_error(e, verbose)
    raise typer.Exit(1) from e


@app.command('files')
def list_files(
  bucket: Annotated[str, typer.Argument(help='Bucket name')],
  output_format: Annotated[OutputFormat, typer.Option('--format', '-f')] = OutputFormat.TABLE,
  verbose: Annotated[bool, verbose_option] = False,
) -> None:
  """List the files in a bucket (file::list($bucket)).

  Examples:
    $ surql bucket files avatars
  """
  try:
    rows = asyncio.run(_list_files_async(bucket, verbose))
    if rows:
      format_output(rows, output_format, title=f'Files in {bucket}')
    else:
      display_info(f'No files in bucket {bucket!r}')
  except DBConnectionError as e:
    display_error(f'Connection failed: {e}')
    raise typer.Exit(1) from e
  except Exception as e:
    handle_error(e, verbose)
    raise typer.Exit(1) from e


# Async implementations


async def _run_statement(statement: str, verbose: bool) -> None:
  """Connect and execute a single DDL statement."""
  config = get_db_config()
  async with get_client(config) as client:
    if verbose:
      display_info(f'Executing: {statement}')
    with spinner() as progress:
      task = progress.add_task('Executing...', total=None)
      await client.execute(statement)
      progress.update(task, completed=True)


async def _list_buckets_async(output_format: OutputFormat, verbose: bool) -> None:
  """List defined buckets via INFO FOR DB."""
  config = get_db_config()
  async with get_client(config) as client:
    result = await client.execute('INFO FOR DB;')
    db_info: dict[str, Any] = {}
    if (
      isinstance(result, list) and result and isinstance(result[0], dict) and 'result' in result[0]
    ):
      db_info = result[0]['result']
    elif isinstance(result, dict):
      db_info = result

    buckets = parse_db_buckets(db_info)
    if not buckets:
      display_info('No buckets defined')
      return

    rows = [
      {
        'name': b.name,
        'backend': b.backend,
        'readonly': b.readonly,
        'comment': b.comment or '',
      }
      for b in buckets.values()
    ]
    if verbose:
      display_info(f'Found {len(rows)} bucket(s)')
    format_output(rows, output_format, title='Buckets')


async def _put_file_async(bucket: str, key: str, data: str | bytes, verbose: bool) -> None:
  config = get_db_config()
  async with get_client(config) as client:
    if verbose:
      display_info(f'Putting {bucket}:/{key} ({len(data)} bytes)')
    await client.bucket(bucket).put(key, data)


async def _get_file_async(bucket: str, key: str, verbose: bool) -> bytes | None:
  config = get_db_config()
  async with get_client(config) as client:
    if verbose:
      display_info(f'Getting {bucket}:/{key}')
    return await client.bucket(bucket).get(key)


async def _delete_file_async(bucket: str, key: str, verbose: bool) -> None:
  config = get_db_config()
  async with get_client(config) as client:
    if verbose:
      display_info(f'Deleting {bucket}:/{key}')
    await client.bucket(bucket).delete(key)


async def _file_exists_async(bucket: str, key: str, verbose: bool) -> bool:
  config = get_db_config()
  async with get_client(config) as client:
    if verbose:
      display_info(f'Checking {bucket}:/{key}')
    return await client.bucket(bucket).exists(key)


async def _list_files_async(bucket: str, verbose: bool) -> list[dict[str, Any]]:
  config = get_db_config()
  async with get_client(config) as client:
    if verbose:
      display_info(f'Listing files in {bucket}')
    return await client.bucket(bucket).list()
