#Requires -Version 7.0

<#
.SYNOPSIS
  Surql PowerShell Development Helper Functions
.DESCRIPTION
  This script provides convenient wrapper functions for common Surql development tasks.
  Simply dot-source this file in your PowerShell session to load all functions.

  Usage: . .\surql.ps1

  All functions follow PowerShell best practices with proper Verb-Noun naming,
  CmdletBinding, parameter validation, and comprehensive help documentation.
.NOTES
  Author: Shon Thomas
  Project: Surql
  Python: 3.12 (Windows)
  Package Manager: uv
#>

#region Helper Functions

function Write-SurqlMessage {
    <#
  .SYNOPSIS
    Writes a formatted message to the console with color coding.
  .DESCRIPTION
    Displays a message prefixed with its type (Info, Success, Warning, Error)
    in a color corresponding to the message type for better visibility.
  .PARAMETER Message
    The message text to display.
  .PARAMETER Type
    The type of message: Info, Success, Warning, or Error. Defaults to Info.
  .EXAMPLE
    Write-SurqlMessage -Message 'Starting database...' -Type 'Info'
    Displays an informational message.
  .EXAMPLE
    Write-SurqlMessage -Message 'Database started successfully' -Type 'Success'
    Displays a success message.
  .EXAMPLE
    Write-SurqlMessage -Message 'Linting found issues' -Type 'Warning'
    Displays a warning message.
  .EXAMPLE
    Write-SurqlMessage -Message 'Command failed' -Type 'Error'
    Displays an error message.
  #>
    [CmdletBinding()]
  param(
    [Parameter(Mandatory)]
    [string]$Message,

    [Parameter()]
    [ValidateSet('Info', 'Success', 'Warning', 'Error')]
    [string]$Type = 'Info'
  )

  $colors = @{
    'Info'    = 'Cyan'
    'Success' = 'Green'
    'Warning' = 'Yellow'
    'Error'   = 'Red'
  }

  $prefix = switch ($Type) {
    'Info' { '[INFO]' }
    'Success' { '[SUCCESS]' }
    'Warning' { '[WARNING]' }
    'Error' { '[ERROR]' }
  }

  Write-Host "$prefix $Message" -ForegroundColor $colors[$Type]
}
Set-Alias -Name sqms -Value Write-SurqlMessage


function Invoke-SurqlCommand {
  <#
  .SYNOPSIS
    Executes a shell command with error handling.
  .DESCRIPTION
    Runs the specified command using Invoke-Expression, captures its output,
    and checks the exit code. If the command fails (non-zero exit code),
    an error message is displayed and an exception is thrown unless PassThru
    is specified.
  .PARAMETER Command
    The shell command to execute.
  .PARAMETER Description
    An optional description of the command being executed.
  .PARAMETER PassThru
    If specified, the function will not throw an exception on command failure.
  .EXAMPLE
    Invoke-SurqlCommand -Command 'uv run ruff check .' -Description 'Running linter'
    Executes the ruff linter command with a description.
  .EXAMPLE
    Invoke-SurqlCommand -Command 'uv run pytest' -PassThru
    Executes the pytest command and does not throw on failure.
  #>
  [CmdletBinding()]
  param(
    [Parameter(Mandatory)]
    [string]$Command,

    [Parameter()]
    [string]$Description,

    [Parameter()]
    [switch]$PassThru
  )

  if ($Description) {
    Write-SurqlMessage -Message $Description -Type 'Info'
  }

  Write-Verbose "Executing: $Command"

  $result = Invoke-Expression $Command
  $exitCode = $LASTEXITCODE

  if ($exitCode -ne 0 -and $null -ne $exitCode) {
    Write-SurqlMessage -Message "Command failed with exit code $exitCode" -Type 'Error'
    if (-not $PassThru) {
      throw "Command failed: $Command"
    }
  }

  return $result
}
Set-Alias -Name sqcmd -Value Invoke-SurqlCommand

#endregion

#region Project Management

function Remove-SurqlArtifacts {
  <#
  .SYNOPSIS
    Removes Surql project build artifacts and caches.
  .DESCRIPTION
    Removes Python cache files, build artifacts, test caches, and other temporary files
    generated during development and testing.
  .EXAMPLE
    Remove-SurqlArtifacts
    Removes all build artifacts and cache files from the project.
  .EXAMPLE
    sqrm
    Uses the alias to quickly clean the project.
  #>
  [CmdletBinding(SupportsShouldProcess)]
  param()

  if ($PSCmdlet.ShouldProcess('Surql project', 'Remove build artifacts and caches')) {
    Write-SurqlMessage -Message 'Removing Surql project artifacts...' -Type 'Info'

    $itemsToRemove = @(
      '__pycache__',
      '*.pyc',
      '*.pyo',
      '*.pyd',
      '.pytest_cache',
      '.mypy_cache',
      '.ruff_cache',
      '*.egg-info',
      'dist',
      'build',
      '.coverage',
      'htmlcov',
      '.tox'
    )

    $totalItems = $itemsToRemove.Count
    $currentItem = 0

    foreach ($item in $itemsToRemove) {
      $currentItem++
      $percentComplete = ($currentItem / $totalItems) * 100
      Write-Progress -Activity 'Removing Surql Artifacts' -Status "Removing $item" -PercentComplete $percentComplete

      Get-ChildItem -Path . -Filter $item -Recurse -Force -ErrorAction SilentlyContinue |
      ForEach-Object {
        Write-Verbose "Removing: $($_.FullName)"
        Remove-Item -Path $_.FullName -Recurse -Force -ErrorAction SilentlyContinue
      }
    }

    Write-Progress -Activity 'Removing Surql Artifacts' -Completed
    Write-SurqlMessage -Message 'Project cleaned successfully' -Type 'Success'
  }
}
Set-Alias -Name sqrm -Value Remove-SurqlArtifacts

function Install-SurqlDependencies {
  <#
  .SYNOPSIS
    Installs Surql project dependencies using uv.
  .DESCRIPTION
    Synchronizes and installs all project dependencies and development dependencies
    using the uv package manager. This ensures the virtual environment matches
    the project's dependency specifications.
  .PARAMETER DevOnly
    Install only development dependencies.
  .EXAMPLE
    Install-SurqlDependencies
    Installs all project dependencies.
  .EXAMPLE
    sqinst
    Uses the alias to quickly install dependencies.
  #>
  [CmdletBinding()]
  param(
    [Parameter()]
    [switch]$DevOnly
  )

  Write-SurqlMessage -Message 'Installing Surql project dependencies...' -Type 'Info'

  if ($DevOnly) {
    Invoke-SurqlCommand -Command 'uv sync --only-dev' -Description 'Syncing development dependencies only'
  }
  else {
    Invoke-SurqlCommand -Command 'uv sync' -Description 'Syncing all dependencies'
  }

  Write-SurqlMessage -Message 'Dependencies installed successfully' -Type 'Success'
}
Set-Alias -Name sqinst -Value Install-SurqlDependencies

#endregion

#region Database Management

function Initialize-SurqlDatabase {
  <#
  .SYNOPSIS
      Initializes the Surql SurrealDB database.
  .DESCRIPTION
      Runs the database initialization script to set up the SurrealDB schema
      and prepare the database for use. Requires SurrealDB to be running.
  .PARAMETER Force
      Force re-initialization even if database already exists.
  .EXAMPLE
      Initialize-SurqlDatabase
      Initializes the database with default schema.
  .EXAMPLE
      sqinit
      Uses the alias to quickly initialize the database.
  #>
  [CmdletBinding(SupportsShouldProcess)]
  param(
    [Parameter()]
    [switch]$Force
  )

  if ($PSCmdlet.ShouldProcess('SurrealDB database', 'Initialize schema')) {
    Write-SurqlMessage -Message 'Initializing Surql database...' -Type 'Info'

    $initScript = Join-Path $PSScriptRoot 'scripts' 'init_db.py'

    if (-not (Test-Path $initScript)) {
      Write-SurqlMessage -Message "Init script not found: $initScript" -Type 'Error'
      throw "Database initialization script not found"
    }

    $command = "uv run python `"$initScript`""
    Invoke-SurqlCommand -Command $command -Description 'Running database initialization script'

    Write-SurqlMessage -Message 'Database initialized successfully' -Type 'Success'
  }
}
Set-Alias -Name sqinit -Value Initialize-SurqlDatabase

function Invoke-SurqlMigration {
  <#
  .SYNOPSIS
    Runs Surql database migrations.
  .DESCRIPTION
    Applies pending database schema migrations to SurrealDB. This ensures
    the database schema is up to date with the latest application requirements.
  .EXAMPLE
    Invoke-SurqlMigration
    Runs all pending migrations.
  .EXAMPLE
    sqmig
    Uses the alias to quickly run migrations.
  .NOTES
    Migration scripts are expected to be in the scripts/ directory.
    Currently, schema is maintained in schemas.surql and applied via init_db.py.
  #>
  [CmdletBinding(SupportsShouldProcess)]
  param()

  if ($PSCmdlet.ShouldProcess('SurrealDB database', 'Apply migrations')) {
    Write-SurqlMessage -Message 'Running database migrations...' -Type 'Info'

    # For now, migrations are handled through schema re-initialization
    # In the future, dedicated migration scripts would go here
    Initialize-SurqlDatabase -Force

    Write-SurqlMessage -Message 'Migrations completed successfully' -Type 'Success'
  }
}
Set-Alias -Name sqmig -Value Invoke-SurqlMigration

function Start-SurqlDatabase {
  <#
  .SYNOPSIS
    Starts the Surql SurrealDB database via Docker Compose.
  .DESCRIPTION
    Starts the SurrealDB database container using docker-compose.
    This is required before running the application or tests.
  .PARAMETER Detached
    Run containers in detached mode (background).
  .EXAMPLE
    Start-SurqlDatabase
    Starts the database in the foreground.
  .EXAMPLE
    Start-SurqlDatabase -Detached
    Starts the database in the background.
  .EXAMPLE
    sqstart -Detached
    Uses the alias to start the database in the background.
  #>
  [CmdletBinding()]
  param(
    [Parameter()]
    [switch]$Detached
  )

  Write-SurqlMessage -Message 'Starting Surql database...' -Type 'Info'

  $composeFile = Join-Path $PSScriptRoot 'docker-compose.yml'

  if (-not (Test-Path $composeFile)) {
    Write-SurqlMessage -Message "Docker Compose file not found: $composeFile" -Type 'Error'
    throw "docker-compose.yml not found"
  }

  $command = if ($Detached) {
    "docker-compose up -d"
  }
  else {
    "docker-compose up"
  }

  Invoke-SurqlCommand -Command $command -Description 'Starting database containers'

  if ($Detached) {
    Write-SurqlMessage -Message 'Database started in background' -Type 'Success'
  }
}
Set-Alias -Name sqstart -Value Start-SurqlDatabase

function Stop-SurqlDatabase {
  <#
  .SYNOPSIS
    Stops the Surql SurrealDB database containers.
  .DESCRIPTION
    Stops and removes the SurrealDB database containers started via Docker Compose.
  .EXAMPLE
    Stop-SurqlDatabase
    Stops the database containers.
  .EXAMPLE
    sqstop
    Uses the alias to quickly stop the database.
  #>
  [CmdletBinding(SupportsShouldProcess)]
  param()

  if ($PSCmdlet.ShouldProcess('Database containers', 'Stop')) {
    Write-SurqlMessage -Message 'Stopping Surql database...' -Type 'Info'

    Invoke-SurqlCommand -Command 'docker-compose down' -Description 'Stopping database containers'

    Write-SurqlMessage -Message 'Database stopped successfully' -Type 'Success'
  }
}
Set-Alias -Name sqstop -Value Stop-SurqlDatabase

#endregion

#region CLI Commands

function Test-SurqlConnection {
  <#
  .SYNOPSIS
    Tests the database connection using surql CLI.
  .DESCRIPTION
    Executes 'surql db ping' to verify database connectivity.
    This is the first step in UAT to confirm SurrealDB is accessible.
  .EXAMPLE
    Test-SurqlConnection
    Tests the database connection.
  .EXAMPLE
    sqping
    Uses the alias to quickly test connection.
  #>
  [CmdletBinding()]
  param()

  Write-SurqlMessage -Message 'Testing database connection...' -Type 'Info'

  Invoke-SurqlCommand -Command 'uv run surql db ping' -PassThru

  if ($LASTEXITCODE -eq 0) {
    Write-SurqlMessage -Message 'Database connection successful' -Type 'Success'
  }
  else {
    Write-SurqlMessage -Message 'Database connection failed' -Type 'Error'
  }
}
Set-Alias -Name sqping -Value Test-SurqlConnection

function Get-SurqlDatabaseInfo {
  <#
  .SYNOPSIS
    Shows database information using surql CLI.
  .DESCRIPTION
    Executes 'surql db info' to display database details including
    connection info, tables, and migration count.
  .EXAMPLE
    Get-SurqlDatabaseInfo
    Shows database information.
  .EXAMPLE
    sqdbinfo
    Uses the alias to quickly show database info.
  #>
  [CmdletBinding()]
  param()

  Write-SurqlMessage -Message 'Fetching database information...' -Type 'Info'

  Invoke-SurqlCommand -Command 'uv run surql db info' -PassThru
}
Set-Alias -Name sqdbinfo -Value Get-SurqlDatabaseInfo

function New-SurqlMigration {
  <#
  .SYNOPSIS
    Creates a new migration file using surql CLI.
  .DESCRIPTION
    Executes 'surql migrate create' to generate a new migration file
    with the specified description. The file is created in the migrations
    directory with a timestamp-based filename.
  .PARAMETER Description
    The description of the migration (e.g., "Create user table").
  .PARAMETER Directory
    The migrations directory path. Defaults to 'migrations'.
  .EXAMPLE
    New-SurqlMigration -Description "Create user table"
    Creates a new migration file for creating the user table.
  .EXAMPLE
    sqmigcreate "Add post table" -Directory uat_migrations
    Uses the alias with a custom directory.
  #>
  [CmdletBinding()]
  param(
    [Parameter(Mandatory, Position = 0)]
    [string]$Description,

    [Parameter()]
    [string]$Directory = 'migrations'
  )

  Write-SurqlMessage -Message "Creating migration: $Description" -Type 'Info'

  $command = "uv run surql migrate create `"$Description`" --directory `"$Directory`""
  Invoke-SurqlCommand -Command $command -PassThru

  if ($LASTEXITCODE -eq 0) {
    Write-SurqlMessage -Message 'Migration file created successfully' -Type 'Success'
  }
  else {
    Write-SurqlMessage -Message 'Failed to create migration file' -Type 'Error'
  }
}
Set-Alias -Name sqmigcreate -Value New-SurqlMigration

function Get-SurqlMigrationStatus {
  <#
  .SYNOPSIS
    Shows migration status using surql CLI.
  .DESCRIPTION
    Executes 'surql migrate status' to display which migrations have
    been applied and which are pending.
  .PARAMETER Directory
    The migrations directory path. Defaults to 'migrations'.
  .PARAMETER Format
    Output format: table, json, or yaml. Defaults to 'table'.
  .EXAMPLE
    Get-SurqlMigrationStatus
    Shows migration status in table format.
  .EXAMPLE
    sqmigstatus -Format json
    Shows migration status in JSON format.
  .EXAMPLE
    sqmigstatus -Directory uat_migrations
    Shows status for migrations in custom directory.
  #>
  [CmdletBinding()]
  param(
    [Parameter()]
    [string]$Directory = 'migrations',

    [Parameter()]
    [ValidateSet('table', 'json', 'yaml')]
    [string]$Format = 'table'
  )

  Write-SurqlMessage -Message 'Checking migration status...' -Type 'Info'

  $command = "uv run surql migrate status --directory `"$Directory`" --format $Format"
  Invoke-SurqlCommand -Command $command -PassThru
}
Set-Alias -Name sqmigstatus -Value Get-SurqlMigrationStatus

function Invoke-SurqlMigrationUp {
  <#
  .SYNOPSIS
    Applies pending migrations using surql CLI.
  .DESCRIPTION
    Executes 'surql migrate up' to apply pending migrations to the database.
    Can optionally perform a dry run to preview changes without applying them.
  .PARAMETER Directory
    The migrations directory path. Defaults to 'migrations'.
  .PARAMETER Steps
    Number of migrations to apply. If not specified, applies all pending.
  .PARAMETER DryRun
    Preview changes without applying them.
  .EXAMPLE
    Invoke-SurqlMigrationUp
    Applies all pending migrations.
  .EXAMPLE
    sqmigup -DryRun
    Previews migrations without applying.
  .EXAMPLE
    sqmigup -Steps 1 -Directory uat_migrations
    Applies only the next pending migration from custom directory.
  #>
  [CmdletBinding(SupportsShouldProcess)]
  param(
    [Parameter()]
    [string]$Directory = 'migrations',

    [Parameter()]
    [int]$Steps,

    [Parameter()]
    [switch]$DryRun
  )

  $action = if ($DryRun) { 'Preview migrations (dry run)' } else { 'Apply migrations' }

  if ($DryRun -or $PSCmdlet.ShouldProcess('Database', $action)) {
    $message = if ($DryRun) { 'Previewing migrations (dry run)...' } else { 'Applying migrations...' }
    Write-SurqlMessage -Message $message -Type 'Info'

    $command = "uv run surql migrate up --directory `"$Directory`""

    if ($Steps) {
      $command += " --steps $Steps"
    }

    if ($DryRun) {
      $command += ' --dry-run'
    }

    Invoke-SurqlCommand -Command $command -PassThru

    if ($LASTEXITCODE -eq 0) {
      $successMsg = if ($DryRun) { 'Dry run completed' } else { 'Migrations applied successfully' }
      Write-SurqlMessage -Message $successMsg -Type 'Success'
    }
    else {
      Write-SurqlMessage -Message 'Migration failed' -Type 'Error'
    }
  }
}
Set-Alias -Name sqmigup -Value Invoke-SurqlMigrationUp

function Invoke-SurqlMigrationDown {
  <#
  .SYNOPSIS
    Rolls back migrations using surql CLI.
  .DESCRIPTION
    Executes 'surql migrate down' to rollback the last applied migration(s).
    Can optionally perform a dry run to preview the rollback without executing.
  .PARAMETER Directory
    The migrations directory path. Defaults to 'migrations'.
  .PARAMETER Steps
    Number of migrations to rollback. Defaults to 1.
  .PARAMETER DryRun
    Preview rollback without executing.
  .PARAMETER Yes
    Skip confirmation prompt.
  .EXAMPLE
    Invoke-SurqlMigrationDown
    Rolls back the last migration.
  .EXAMPLE
    sqmigdown -Steps 3
    Rolls back the last 3 migrations.
  .EXAMPLE
    sqmigdown -DryRun
    Previews rollback without executing.
  .EXAMPLE
    sqmigdown -Yes
    Rolls back without confirmation prompt.
  #>
  [CmdletBinding(SupportsShouldProcess)]
  param(
    [Parameter()]
    [string]$Directory = 'migrations',

    [Parameter()]
    [int]$Steps = 1,

    [Parameter()]
    [switch]$DryRun,

    [Parameter()]
    [Alias('y')]
    [switch]$Yes
  )

  $action = if ($DryRun) { 'Preview rollback (dry run)' } else { "Rollback $Steps migration(s)" }

  if ($DryRun -or $Yes -or $PSCmdlet.ShouldProcess('Database', $action)) {
    $message = if ($DryRun) { 'Previewing rollback (dry run)...' } else { "Rolling back $Steps migration(s)..." }
    Write-SurqlMessage -Message $message -Type 'Info'

    $command = "uv run surql migrate down --directory `"$Directory`" --steps $Steps"

    if ($DryRun) {
      $command += ' --dry-run'
    }

    if ($Yes) {
      $command += ' --yes'
    }

    Invoke-SurqlCommand -Command $command -PassThru

    if ($LASTEXITCODE -eq 0) {
      $successMsg = if ($DryRun) { 'Dry run completed' } else { 'Rollback completed successfully' }
      Write-SurqlMessage -Message $successMsg -Type 'Success'
    }
    else {
      Write-SurqlMessage -Message 'Rollback failed' -Type 'Error'
    }
  }
}
Set-Alias -Name sqmigdown -Value Invoke-SurqlMigrationDown

function Get-SurqlMigrationHistory {
  <#
  .SYNOPSIS
    Shows migration history using surql CLI.
  .DESCRIPTION
    Executes 'surql migrate history' to display the history of applied
    migrations including timestamps and execution times.
  .PARAMETER Directory
    The migrations directory path. Defaults to 'migrations'.
  .PARAMETER Format
    Output format: table, json, or yaml. Defaults to 'table'.
  .EXAMPLE
    Get-SurqlMigrationHistory
    Shows migration history in table format.
  .EXAMPLE
    sqmighist -Format json
    Shows migration history in JSON format.
  #>
  [CmdletBinding()]
  param(
    [Parameter()]
    [string]$Directory = 'migrations',

    [Parameter()]
    [ValidateSet('table', 'json', 'yaml')]
    [string]$Format = 'table'
  )

  Write-SurqlMessage -Message 'Fetching migration history...' -Type 'Info'

  $command = "uv run surql migrate history --directory `"$Directory`" --format $Format"
  Invoke-SurqlCommand -Command $command -PassThru
}
Set-Alias -Name sqmighist -Value Get-SurqlMigrationHistory

function Get-SurqlSchema {
  <#
  .SYNOPSIS
    Shows database or table schema using surql CLI.
  .DESCRIPTION
    Executes 'surql schema show' to display the database schema or
    a specific table's schema including fields, indexes, and constraints.
  .PARAMETER Table
    Optional table name to show specific table schema.
    If not specified, shows the entire database schema.
  .EXAMPLE
    Get-SurqlSchema
    Shows the entire database schema.
  .EXAMPLE
    sqschema user
    Shows the schema for the user table.
  .EXAMPLE
    Get-SurqlSchema -Table post
    Shows the schema for the post table.
  #>
  [CmdletBinding()]
  param(
    [Parameter(Position = 0)]
    [string]$Table
  )

  $target = if ($Table) { "table '$Table'" } else { 'database' }
  Write-SurqlMessage -Message "Fetching schema for $target..." -Type 'Info'

  $command = 'uv run surql schema show'

  if ($Table) {
    $command += " $Table"
  }

  Invoke-SurqlCommand -Command $command -PassThru
}
Set-Alias -Name sqschema -Value Get-SurqlSchema

function Test-SurqlMigration {
  <#
  .SYNOPSIS
    Validates migration files using surql CLI.
  .DESCRIPTION
    Executes 'surql migrate validate' to check migration files for errors
    including syntax, missing functions, and metadata issues.
  .PARAMETER Directory
    The migrations directory path. Defaults to 'migrations'.
  .EXAMPLE
    Test-SurqlMigration
    Validates all migration files.
  .EXAMPLE
    sqmigvalid -Directory uat_migrations
    Validates migrations in custom directory.
  #>
  [CmdletBinding()]
  param(
    [Parameter()]
    [string]$Directory = 'migrations'
  )

  Write-SurqlMessage -Message 'Validating migration files...' -Type 'Info'

  $command = "uv run surql migrate validate --directory `"$Directory`""
  Invoke-SurqlCommand -Command $command -PassThru

  if ($LASTEXITCODE -eq 0) {
    Write-SurqlMessage -Message 'All migrations are valid' -Type 'Success'
  }
  else {
    Write-SurqlMessage -Message 'Migration validation failed' -Type 'Error'
  }
}
Set-Alias -Name sqmigvalid -Value Test-SurqlMigration

#endregion

#region Code Quality & Testing

function Invoke-SurqlLint {
  <#
  .SYNOPSIS
    Runs ruff linter on the Surql codebase.
  .DESCRIPTION
    Executes ruff check to identify code quality issues, style violations,
    and potential bugs. This is part of the project's critical quality checks.
  .PARAMETER Fix
    Automatically fix issues where possible.
  .EXAMPLE
    Invoke-SurqlLint
    Runs the linter and reports issues.
  .EXAMPLE
    Invoke-SurqlLint -Fix
    Runs the linter and automatically fixes issues.
  .EXAMPLE
    sqlint
    Uses the alias to quickly run the linter.
  #>
  [CmdletBinding()]
  param(
    [Parameter()]
    [switch]$Fix
  )

  Write-SurqlMessage -Message 'Running ruff linter...' -Type 'Info'

  $command = if ($Fix) {
    'uv run ruff check src tests --fix'
  }
  else {
    'uv run ruff check src tests'
  }

  Invoke-SurqlCommand -Command $command -PassThru

  if ($LASTEXITCODE -eq 0) {
    Write-SurqlMessage -Message 'Linting passed' -Type 'Success'
  }
  else {
    Write-SurqlMessage -Message 'Linting found issues' -Type 'Warning'
  }
}
Set-Alias -Name sqlint -Value Invoke-SurqlLint

function Invoke-SurqlFormat {
  <#
  .SYNOPSIS
    Formats Surql code using ruff formatter.
  .DESCRIPTION
    Runs ruff format to automatically format the codebase according to
    project style guidelines (black-compatible, 2 spaces, single quotes).
  .PARAMETER Check
    Checks if files are formatted without making changes.
  .EXAMPLE
    Invoke-SurqlFormat
    Formats all Python files in the project.
  .EXAMPLE
    Invoke-SurqlFormat -Check
    Checks if files are formatted without modifying them.
  .EXAMPLE
    sqfmt
    Uses the alias to quickly format code.
  #>
  [CmdletBinding(SupportsShouldProcess)]
  param(
    [Parameter()]
    [switch]$Check
  )

  if ($Check -or $PSCmdlet.ShouldProcess('Surql codebase', 'Format code')) {
    Write-SurqlMessage -Message 'Running ruff formatter...' -Type 'Info'

    $command = if ($Check) {
      'uv run ruff format src tests --check'
    }
    else {
      'uv run ruff format src tests'
    }

    Invoke-SurqlCommand -Command $command -PassThru

    if ($LASTEXITCODE -eq 0) {
      if ($Check) {
        Write-SurqlMessage -Message 'All files are properly formatted' -Type 'Success'
      }
      else {
        Write-SurqlMessage -Message 'Code formatted successfully' -Type 'Success'
      }
    }
    else {
      Write-SurqlMessage -Message 'Formatting issues found' -Type 'Warning'
    }
  }
}
Set-Alias -Name sqfmt -Value Invoke-SurqlFormat

function Invoke-SurqlTypeCheck {
  <#
  .SYNOPSIS
    Runs mypy type checker on Surql codebase.
  .DESCRIPTION
    Executes mypy in strict mode to verify type annotations and catch
    type-related errors. All code must pass mypy strict checking.
  .EXAMPLE
    Invoke-SurqlTypeCheck
    Runs type checking on the entire codebase.
  .EXAMPLE
    sqtype
    Uses the alias to quickly run type checking.
  #>
  [CmdletBinding()]
  param()

  Write-SurqlMessage -Message 'Running mypy type checker...' -Type 'Info'

  Invoke-SurqlCommand -Command 'uv run python -m mypy --strict src' -PassThru

  if ($LASTEXITCODE -eq 0) {
    Write-SurqlMessage -Message 'Type checking passed' -Type 'Success'
  }
  else {
    Write-SurqlMessage -Message 'Type checking found issues' -Type 'Warning'
  }
}
Set-Alias -Name sqtype -Value Invoke-SurqlTypeCheck

function Test-Surql {
  <#
  .SYNOPSIS
    Runs Surql test suite.
  .DESCRIPTION
    Executes pytest to run all tests in the project. Tests must pass
    as part of the project's critical quality checks.
  .PARAMETER Verbose
    Runs tests with verbose output.
  .PARAMETER FailFast
    Stops on first test failure.
  .PARAMETER Pattern
    Runs only tests matching the pattern.
  .EXAMPLE
    Test-Surql
    Runs all tests.
  .EXAMPLE
    Test-Surql -Verbose
    Runs all tests with verbose output.
  .EXAMPLE
    Test-Surql -Pattern 'test_storage*'
    # Runs only storage-related tests.
  .EXAMPLE
    sqtest
    Uses the alias to quickly run tests.
  #>
  [CmdletBinding()]
  param(
    [Parameter()]
    [switch]$FailFast,

    [Parameter()]
    [string]$Pattern
  )

  Write-SurqlMessage -Message 'Running test suite...' -Type 'Info'

  $pytestArgs = @('uv', 'run', 'python', '-m', 'pytest')

  if ($PSBoundParameters.ContainsKey('Verbose')) {
    $pytestArgs += '-v'
  }

  if ($FailFast) {
    $pytestArgs += '-x'
  }

  if ($Pattern) {
    $pytestArgs += '-k'
    $pytestArgs += $Pattern
  }

  $command = $pytestArgs -join ' '
  Invoke-SurqlCommand -Command $command -PassThru

  if ($LASTEXITCODE -eq 0) {
    Write-SurqlMessage -Message 'All tests passed' -Type 'Success'
  }
  else {
    Write-SurqlMessage -Message 'Some tests failed' -Type 'Warning'
  }
}
Set-Alias -Name sqtest -Value Test-Surql

function Test-SurqlCoverage {
  <#
  .SYNOPSIS
    Runs Surql tests with coverage analysis.
  .DESCRIPTION
    Executes pytest with coverage reporting to measure test coverage.
    The project requires ≥80% coverage.
  .PARAMETER Html
    Generate HTML coverage report.
  .EXAMPLE
    Test-SurqlCoverage
    Runs tests and displays coverage report.
  .EXAMPLE
    Test-SurqlCoverage -Html
    Runs tests and generates HTML coverage report.
  .EXAMPLE
    sqcov
    Uses the alias to quickly run coverage analysis.
  #>
  [CmdletBinding()]
  param(
    [Parameter()]
    [switch]$Html
  )

  Write-SurqlMessage -Message 'Running tests with coverage analysis...' -Type 'Info'

  $command = 'uv run python -m pytest --cov=src --cov-report=term-missing'

  if ($Html) {
    $command += ' --cov-report=html'
  } else {
    $command += ' --cov-report=term'
  }

  Invoke-SurqlCommand -Command $command -PassThru

  if ($LASTEXITCODE -eq 0) {
    Write-SurqlMessage -Message 'Coverage analysis completed' -Type 'Success'
    if ($Html) {
      Write-SurqlMessage -Message 'HTML report generated in htmlcov/' -Type 'Info'
    }
  }
  else {
    Write-SurqlMessage -Message 'Coverage analysis failed' -Type 'Warning'
  }
}
Set-Alias -Name sqcov -Value Test-SurqlCoverage

function Invoke-SurqlCheck {
  <#
  .SYNOPSIS
    Runs all Surql quality checks.
  .DESCRIPTION
    Executes the complete suite of critical quality checks required by the project:
      - ruff check (linting)
      - ruff format --check (formatting)
      - mypy --strict (type checking)
      - pytest (tests)
      - pytest --cov (coverage)
  .PARAMETER SkipTests
    Skip running tests & bypasses test-related checks.
  .EXAMPLE
    Invoke-SurqlCheck
    Runs all quality checks.
  .EXAMPLE
    sqcheck
    Uses the alias to quickly run all checks.
  #>
  [CmdletBinding()]
  param(
    [Parameter()]
    [switch]$SkipTests
  )

  Write-SurqlMessage -Message 'Running all Surql quality checks...' -Type 'Info'

  $failed = $false
  $totalChecks = if ($SkipTests) { 3 } else { 5 }

  # Check 1: Ruff lint
  Write-Progress -Activity 'Running Surql Quality Checks' -Status 'Running ruff check' -PercentComplete (1 / $totalChecks * 100)
  Invoke-SurqlLint
  if ($LASTEXITCODE -ne 0) { $failed = $true }

  # Check 2: Ruff format
  Write-Progress -Activity 'Running Surql Quality Checks' -Status 'Running ruff format --check' -PercentComplete (2 / $totalChecks * 100)
  Invoke-SurqlFormat -Check
  if ($LASTEXITCODE -ne 0) { $failed = $true }

  # Check 3: Mypy type checking
  Write-Progress -Activity 'Running Surql Quality Checks' -Status 'Running mypy --strict' -PercentComplete (3 / $totalChecks * 100)
  Invoke-SurqlTypeCheck
  if ($LASTEXITCODE -ne 0) { $failed = $true }

  if (-not $SkipTests) {
    # Check 4: Tests
    Write-Progress -Activity 'Running Surql Quality Checks' -Status 'Running pytest' -PercentComplete 80
    Test-Surql
    if ($LASTEXITCODE -ne 0) { $failed = $true }

    # Check 5: Coverage
    Write-Progress -Activity 'Running Surql Quality Checks' -Status 'Running pytest --cov' -PercentComplete 100
    Test-SurqlCoverage
    if ($LASTEXITCODE -ne 0) { $failed = $true }
  }

  # Clear progress bar
  Write-Progress -Activity 'Running Surql Quality Checks' -Completed

  # Summary
  if ($failed) {
    Write-SurqlMessage -Message 'Some checks failed. Please fix issues before committing.' -Type 'Error'
    throw "Quality checks failed"
  }
  else {
    Write-SurqlMessage -Message 'All quality checks passed successfully!' -Type 'Success'
  }
}
Set-Alias -Name sqcheck -Value Invoke-SurqlCheck

#endregion

#region Application

function Start-Surql {
  <#
  .SYNOPSIS
      Starts the Surql CLI application.
  .DESCRIPTION
      Executes the Surql CLI using uv run. Pass any additional arguments
      directly to the CLI.
  .PARAMETER Arguments
      Arguments to pass to the Surql CLI.
  .EXAMPLE
      Start-Surql
      Starts the Surql CLI.
  .EXAMPLE
      Start-Surql --help
      Shows Surql CLI help.
  .EXAMPLE
      sqrun <app_command> <args>
      Uses the alias to quickly start the Surql CLI with commands.
  #>
  [CmdletBinding()]
  param(
    [Parameter(ValueFromRemainingArguments)]
    [string[]]$Arguments
  )

  $command = "uv run python -m src $($Arguments -join ' ')"

  Write-SurqlMessage -Message 'Starting Surql CLI...' -Type 'Info'
  Invoke-SurqlCommand -Command $command
}
Set-Alias -Name sqrun -Value Start-Surql

#endregion

#region Convenience Functions

function Show-SurqlHelp {
  <#
  .SYNOPSIS
      Shows all available Surql PowerShell commands.
  .DESCRIPTION
      Displays a summary of all available Surql development helper functions
      with their aliases and descriptions.
  .EXAMPLE
      Show-SurqlHelp
      Displays all available commands.
  .EXAMPLE
      sqhelp
      Uses the alias to quickly show help.
  #>
  [CmdletBinding()]
  param()

  Write-Host ''
  Write-Host '+----------------------------------------------------------------------+' -ForegroundColor Cyan
  Write-Host '|            Surql PowerShell Development Commands                  |' -ForegroundColor Cyan
  Write-Host '+----------------------------------------------------------------------+' -ForegroundColor Cyan
  Write-Host ''

  # Helper function to print command row
  $printCmd = {
    param($c)
    $p1 = [Math]::Max(1, 35 - $c.Name.Length)
    $p2 = [Math]::Max(1, 16 - "($($c.Alias))".Length)
    Write-Host "  $($c.Name)" -ForegroundColor Magenta -NoNewline
    Write-Host (' ' * $p1) -NoNewline
    Write-Host "($($c.Alias))" -ForegroundColor Yellow -NoNewline
    Write-Host (' ' * $p2) -NoNewline
    Write-Host $c.Description -ForegroundColor Gray
  }

  Write-Host 'PROJECT MANAGEMENT:' -ForegroundColor Cyan
  @(
    @{ Name = 'Initialize-SurqlProject'; Alias = 'sqinitproj'; Description = 'Set up a new Surql project' }
    @{ Name = 'Remove-SurqlArtifacts'; Alias = 'sqrm'; Description = 'Remove build artifacts and caches' }
    @{ Name = 'Install-SurqlDependencies'; Alias = 'sqinst'; Description = 'Install project dependencies' }
  ) | ForEach-Object { & $printCmd $_ }
  Write-Host ''

  Write-Host 'DATABASE CONTAINERS:' -ForegroundColor Cyan
  @(
    @{ Name = 'Initialize-SurqlDatabase'; Alias = 'sqinit'; Description = 'Initialize SurrealDB database' }
    @{ Name = 'Start-SurqlDatabase'; Alias = 'sqstart'; Description = 'Start database containers' }
    @{ Name = 'Stop-SurqlDatabase'; Alias = 'sqstop'; Description = 'Stop database containers' }
  ) | ForEach-Object { & $printCmd $_ }
  Write-Host ''

  Write-Host 'CLI - DATABASE:' -ForegroundColor Cyan
  @(
    @{ Name = 'Test-SurqlConnection'; Alias = 'sqping'; Description = 'Test database connection' }
    @{ Name = 'Get-SurqlDatabaseInfo'; Alias = 'sqdbinfo'; Description = 'Show database information' }
    @{ Name = 'Get-SurqlSchema'; Alias = 'sqschema'; Description = 'Show database/table schema' }
  ) | ForEach-Object { & $printCmd $_ }
  Write-Host ''

  Write-Host 'CLI - MIGRATIONS:' -ForegroundColor Cyan
  @(
    @{ Name = 'Invoke-SurqlMigration'; Alias = 'sqmig'; Description = 'Run all pending migrations' }
    @{ Name = 'Get-SurqlMigrationStatus'; Alias = 'sqmigstatus'; Description = 'Show migration status' }
    @{ Name = 'New-SurqlMigration'; Alias = 'sqmigcreate'; Description = 'Create new migration file' }
    @{ Name = 'Invoke-SurqlMigrationUp'; Alias = 'sqmigup'; Description = 'Apply pending migrations' }
    @{ Name = 'Invoke-SurqlMigrationDown'; Alias = 'sqmigdown'; Description = 'Rollback migrations' }
    @{ Name = 'Get-SurqlMigrationHistory'; Alias = 'sqmighist'; Description = 'Show migration history' }
    @{ Name = 'Test-SurqlMigration'; Alias = 'sqmigvalid'; Description = 'Validate migration files' }
  ) | ForEach-Object { & $printCmd $_ }
  Write-Host ''

  Write-Host 'CODE QUALITY & TESTING:' -ForegroundColor Cyan
  @(
    @{ Name = 'Invoke-SurqlLint'; Alias = 'sqlint'; Description = 'Run ruff linter' }
    @{ Name = 'Invoke-SurqlFormat'; Alias = 'sqfmt'; Description = 'Format code with ruff' }
    @{ Name = 'Invoke-SurqlTypeCheck'; Alias = 'sqtype'; Description = 'Run mypy type checker' }
    @{ Name = 'Test-Surql'; Alias = 'sqtest'; Description = 'Run test suite' }
    @{ Name = 'Test-SurqlCoverage'; Alias = 'sqcov'; Description = 'Run tests with coverage' }
    @{ Name = 'Invoke-SurqlCheck'; Alias = 'sqcheck'; Description = 'Run all quality checks' }
  ) | ForEach-Object { & $printCmd $_ }
  Write-Host ''

  Write-Host 'APPLICATION:' -ForegroundColor Cyan
  @(
    @{ Name = 'Start-Surql'; Alias = 'sqrun'; Description = 'Start Surql CLI' }
  ) | ForEach-Object { & $printCmd $_ }
  Write-Host ''

  Write-Host 'HELP:' -ForegroundColor Cyan
  @(
    @{ Name = 'Show-SurqlHelp'; Alias = 'sqhelp'; Description = 'Show this help message' }
  ) | ForEach-Object { & $printCmd $_ }

  Write-Host 'TIP: Use "Get-Help <CommandName> -Detailed" for more information' -ForegroundColor DarkGray
  Write-Host ''
}
Set-Alias -Name sqhelp -Value Show-SurqlHelp

#endregion

# Display welcome message
Write-Host ''
Write-Host '+----------------------------------------------------------------------+' -ForegroundColor Green
Write-Host '|                    ' -ForegroundColor Green -NoNewline
Write-Host 'Surql Commands Loaded' -ForegroundColor Blue -NoNewline
Write-Host '                           |' -ForegroundColor Green
Write-Host '+----------------------------------------------------------------------+' -ForegroundColor Green
Write-Host ''
Write-Host 'Surql development helper functions loaded successfully!' -ForegroundColor Cyan
Write-Host "Type 'sqhelp' or 'Show-SurqlHelp' to see all available commands." -ForegroundColor Gray
Write-Host ''
