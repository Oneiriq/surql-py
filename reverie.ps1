#Requires -Version 7.0

<#
.SYNOPSIS
  Reverie PowerShell Development Helper Functions
.DESCRIPTION
  This script provides convenient wrapper functions for common Reverie development tasks.
  Simply dot-source this file in your PowerShell session to load all functions.

  Usage: . .\reverie.ps1

  All functions follow PowerShell best practices with proper Verb-Noun naming,
  CmdletBinding, parameter validation, and comprehensive help documentation.
.NOTES
  Author: Shon Thomas
  Project: Reverie
  Python: 3.12 (Windows)
  Package Manager: uv
#>

#region Helper Functions

function Write-ReverieMessage {
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
    Write-ReverieMessage -Message 'Starting database...' -Type 'Info'
    Displays an informational message.
  .EXAMPLE
    Write-ReverieMessage -Message 'Database started successfully' -Type 'Success'
    Displays a success message.
  .EXAMPLE
    Write-ReverieMessage -Message 'Linting found issues' -Type 'Warning'
    Displays a warning message.
  .EXAMPLE
    Write-ReverieMessage -Message 'Command failed' -Type 'Error'
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
Set-Alias -Name rvms -Value Write-ReverieMessage


function Invoke-ReverieCommand {
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
    Invoke-ReverieCommand -Command 'uv run ruff check .' -Description 'Running linter'
    Executes the ruff linter command with a description.
  .EXAMPLE
    Invoke-ReverieCommand -Command 'uv run pytest' -PassThru
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
    Write-ReverieMessage -Message $Description -Type 'Info'
  }

  Write-Verbose "Executing: $Command"

  $result = Invoke-Expression $Command
  $exitCode = $LASTEXITCODE

  if ($exitCode -ne 0 -and $null -ne $exitCode) {
    Write-ReverieMessage -Message "Command failed with exit code $exitCode" -Type 'Error'
    if (-not $PassThru) {
      throw "Command failed: $Command"
    }
  }

  return $result
}
Set-Alias -Name rvcmd -Value Invoke-ReverieCommand

#endregion

#region Project Management

function Remove-ReverieArtifacts {
  <#
  .SYNOPSIS
    Removes Reverie project build artifacts and caches.
  .DESCRIPTION
    Removes Python cache files, build artifacts, test caches, and other temporary files
    generated during development and testing.
  .EXAMPLE
    Remove-ReverieArtifacts
    Removes all build artifacts and cache files from the project.
  .EXAMPLE
    rvrm
    Uses the alias to quickly clean the project.
  #>
  [CmdletBinding(SupportsShouldProcess)]
  param()

  if ($PSCmdlet.ShouldProcess('Reverie project', 'Remove build artifacts and caches')) {
    Write-ReverieMessage -Message 'Removing Reverie project artifacts...' -Type 'Info'

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
      Write-Progress -Activity 'Removing Reverie Artifacts' -Status "Removing $item" -PercentComplete $percentComplete

      Get-ChildItem -Path . -Filter $item -Recurse -Force -ErrorAction SilentlyContinue |
      ForEach-Object {
        Write-Verbose "Removing: $($_.FullName)"
        Remove-Item -Path $_.FullName -Recurse -Force -ErrorAction SilentlyContinue
      }
    }

    Write-Progress -Activity 'Removing Reverie Artifacts' -Completed
    Write-ReverieMessage -Message 'Project cleaned successfully' -Type 'Success'
  }
}
Set-Alias -Name rvrm -Value Remove-ReverieArtifacts

function Install-ReverieDependencies {
  <#
  .SYNOPSIS
    Installs Reverie project dependencies using uv.
  .DESCRIPTION
    Synchronizes and installs all project dependencies and development dependencies
    using the uv package manager. This ensures the virtual environment matches
    the project's dependency specifications.
  .PARAMETER DevOnly
    Install only development dependencies.
  .EXAMPLE
    Install-ReverieDependencies
    Installs all project dependencies.
  .EXAMPLE
    rvinst
    Uses the alias to quickly install dependencies.
  #>
  [CmdletBinding()]
  param(
    [Parameter()]
    [switch]$DevOnly
  )

  Write-ReverieMessage -Message 'Installing Reverie project dependencies...' -Type 'Info'

  if ($DevOnly) {
    Invoke-ReverieCommand -Command 'uv sync --only-dev' -Description 'Syncing development dependencies only'
  }
  else {
    Invoke-ReverieCommand -Command 'uv sync' -Description 'Syncing all dependencies'
  }

  Write-ReverieMessage -Message 'Dependencies installed successfully' -Type 'Success'
}
Set-Alias -Name rvinst -Value Install-ReverieDependencies

#endregion

#region Database Management

function Initialize-ReverieDatabase {
  <#
  .SYNOPSIS
      Initializes the Reverie SurrealDB database.
  .DESCRIPTION
      Runs the database initialization script to set up the SurrealDB schema
      and prepare the database for use. Requires SurrealDB to be running.
  .PARAMETER Force
      Force re-initialization even if database already exists.
  .EXAMPLE
      Initialize-ReverieDatabase
      Initializes the database with default schema.
  .EXAMPLE
      rvinit
      Uses the alias to quickly initialize the database.
  #>
  [CmdletBinding(SupportsShouldProcess)]
  param(
    [Parameter()]
    [switch]$Force
  )

  if ($PSCmdlet.ShouldProcess('SurrealDB database', 'Initialize schema')) {
    Write-ReverieMessage -Message 'Initializing Reverie database...' -Type 'Info'

    $initScript = Join-Path $PSScriptRoot 'scripts' 'init_db.py'

    if (-not (Test-Path $initScript)) {
      Write-ReverieMessage -Message "Init script not found: $initScript" -Type 'Error'
      throw "Database initialization script not found"
    }

    $command = "uv run python `"$initScript`""
    Invoke-ReverieCommand -Command $command -Description 'Running database initialization script'

    Write-ReverieMessage -Message 'Database initialized successfully' -Type 'Success'
  }
}
Set-Alias -Name rvinit -Value Initialize-ReverieDatabase

function Invoke-ReverieMigration {
  <#
  .SYNOPSIS
    Runs Reverie database migrations.
  .DESCRIPTION
    Applies pending database schema migrations to SurrealDB. This ensures
    the database schema is up to date with the latest application requirements.
  .EXAMPLE
    Invoke-ReverieMigration
    Runs all pending migrations.
  .EXAMPLE
    rvmig
    Uses the alias to quickly run migrations.
  .NOTES
    Migration scripts are expected to be in the scripts/ directory.
    Currently, schema is maintained in schemas.surql and applied via init_db.py.
  #>
  [CmdletBinding(SupportsShouldProcess)]
  param()

  if ($PSCmdlet.ShouldProcess('SurrealDB database', 'Apply migrations')) {
    Write-ReverieMessage -Message 'Running database migrations...' -Type 'Info'

    # For now, migrations are handled through schema re-initialization
    # In the future, dedicated migration scripts would go here
    Initialize-ReverieDatabase -Force

    Write-ReverieMessage -Message 'Migrations completed successfully' -Type 'Success'
  }
}
Set-Alias -Name rvmig -Value Invoke-ReverieMigration

function Start-ReverieDatabase {
  <#
  .SYNOPSIS
    Starts the Reverie SurrealDB database via Docker Compose.
  .DESCRIPTION
    Starts the SurrealDB database container using docker-compose.
    This is required before running the application or tests.
  .PARAMETER Detached
    Run containers in detached mode (background).
  .EXAMPLE
    Start-ReverieDatabase
    Starts the database in the foreground.
  .EXAMPLE
    Start-ReverieDatabase -Detached
    Starts the database in the background.
  .EXAMPLE
    rvstart -Detached
    Uses the alias to start the database in the background.
  #>
  [CmdletBinding()]
  param(
    [Parameter()]
    [switch]$Detached
  )

  Write-ReverieMessage -Message 'Starting Reverie database...' -Type 'Info'

  $composeFile = Join-Path $PSScriptRoot 'docker-compose.yml'

  if (-not (Test-Path $composeFile)) {
    Write-ReverieMessage -Message "Docker Compose file not found: $composeFile" -Type 'Error'
    throw "docker-compose.yml not found"
  }

  $command = if ($Detached) {
    "docker-compose up -d"
  }
  else {
    "docker-compose up"
  }

  Invoke-ReverieCommand -Command $command -Description 'Starting database containers'

  if ($Detached) {
    Write-ReverieMessage -Message 'Database started in background' -Type 'Success'
  }
}
Set-Alias -Name rvstart -Value Start-ReverieDatabase

function Stop-ReverieDatabase {
  <#
  .SYNOPSIS
    Stops the Reverie SurrealDB database containers.
  .DESCRIPTION
    Stops and removes the SurrealDB database containers started via Docker Compose.
  .EXAMPLE
    Stop-ReverieDatabase
    Stops the database containers.
  .EXAMPLE
    rvstop
    Uses the alias to quickly stop the database.
  #>
  [CmdletBinding(SupportsShouldProcess)]
  param()

  if ($PSCmdlet.ShouldProcess('Database containers', 'Stop')) {
    Write-ReverieMessage -Message 'Stopping Reverie database...' -Type 'Info'

    Invoke-ReverieCommand -Command 'docker-compose down' -Description 'Stopping database containers'

    Write-ReverieMessage -Message 'Database stopped successfully' -Type 'Success'
  }
}
Set-Alias -Name rvstop -Value Stop-ReverieDatabase

#endregion

#region Code Quality & Testing

function Invoke-ReverieLint {
  <#
  .SYNOPSIS
    Runs ruff linter on the Reverie codebase.
  .DESCRIPTION
    Executes ruff check to identify code quality issues, style violations,
    and potential bugs. This is part of the project's critical quality checks.
  .PARAMETER Fix
    Automatically fix issues where possible.
  .EXAMPLE
    Invoke-ReverieLint
    Runs the linter and reports issues.
  .EXAMPLE
    Invoke-ReverieLint -Fix
    Runs the linter and automatically fixes issues.
  .EXAMPLE
    rvlint
    Uses the alias to quickly run the linter.
  #>
  [CmdletBinding()]
  param(
    [Parameter()]
    [switch]$Fix
  )

  Write-ReverieMessage -Message 'Running ruff linter...' -Type 'Info'

  $command = if ($Fix) {
    'uv run ruff check src tests --fix'
  }
  else {
    'uv run ruff check src tests'
  }

  Invoke-ReverieCommand -Command $command -PassThru

  if ($LASTEXITCODE -eq 0) {
    Write-ReverieMessage -Message 'Linting passed' -Type 'Success'
  }
  else {
    Write-ReverieMessage -Message 'Linting found issues' -Type 'Warning'
  }
}
Set-Alias -Name rvlint -Value Invoke-ReverieLint

function Invoke-ReverieFormat {
  <#
  .SYNOPSIS
    Formats Reverie code using ruff formatter.
  .DESCRIPTION
    Runs ruff format to automatically format the codebase according to
    project style guidelines (black-compatible, 2 spaces, single quotes).
  .PARAMETER Check
    Checks if files are formatted without making changes.
  .EXAMPLE
    Invoke-ReverieFormat
    Formats all Python files in the project.
  .EXAMPLE
    Invoke-ReverieFormat -Check
    Checks if files are formatted without modifying them.
  .EXAMPLE
    rvfmt
    Uses the alias to quickly format code.
  #>
  [CmdletBinding(SupportsShouldProcess)]
  param(
    [Parameter()]
    [switch]$Check
  )

  if ($Check -or $PSCmdlet.ShouldProcess('Reverie codebase', 'Format code')) {
    Write-ReverieMessage -Message 'Running ruff formatter...' -Type 'Info'

    $command = if ($Check) {
      'uv run ruff format src tests --check'
    }
    else {
      'uv run ruff format src tests'
    }

    Invoke-ReverieCommand -Command $command -PassThru

    if ($LASTEXITCODE -eq 0) {
      if ($Check) {
        Write-ReverieMessage -Message 'All files are properly formatted' -Type 'Success'
      }
      else {
        Write-ReverieMessage -Message 'Code formatted successfully' -Type 'Success'
      }
    }
    else {
      Write-ReverieMessage -Message 'Formatting issues found' -Type 'Warning'
    }
  }
}
Set-Alias -Name rvfmt -Value Invoke-ReverieFormat

function Invoke-ReverieTypeCheck {
  <#
  .SYNOPSIS
    Runs mypy type checker on Reverie codebase.
  .DESCRIPTION
    Executes mypy in strict mode to verify type annotations and catch
    type-related errors. All code must pass mypy strict checking.
  .EXAMPLE
    Invoke-ReverieTypeCheck
    Runs type checking on the entire codebase.
  .EXAMPLE
    rvtype
    Uses the alias to quickly run type checking.
  #>
  [CmdletBinding()]
  param()

  Write-ReverieMessage -Message 'Running mypy type checker...' -Type 'Info'

  Invoke-ReverieCommand -Command 'uv run python -m mypy --strict src' -PassThru

  if ($LASTEXITCODE -eq 0) {
    Write-ReverieMessage -Message 'Type checking passed' -Type 'Success'
  }
  else {
    Write-ReverieMessage -Message 'Type checking found issues' -Type 'Warning'
  }
}
Set-Alias -Name rvtype -Value Invoke-ReverieTypeCheck

function Test-Reverie {
  <#
  .SYNOPSIS
    Runs Reverie test suite.
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
    Test-Reverie
    Runs all tests.
  .EXAMPLE
    Test-Reverie -Verbose
    Runs all tests with verbose output.
  .EXAMPLE
    Test-Reverie -Pattern 'test_storage*'
    # Runs only storage-related tests.
  .EXAMPLE
    rvtest
    Uses the alias to quickly run tests.
  #>
  [CmdletBinding()]
  param(
    [Parameter()]
    [switch]$FailFast,

    [Parameter()]
    [string]$Pattern
  )

  Write-ReverieMessage -Message 'Running test suite...' -Type 'Info'

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
  Invoke-ReverieCommand -Command $command -PassThru

  if ($LASTEXITCODE -eq 0) {
    Write-ReverieMessage -Message 'All tests passed' -Type 'Success'
  }
  else {
    Write-ReverieMessage -Message 'Some tests failed' -Type 'Warning'
  }
}
Set-Alias -Name rvtest -Value Test-Reverie

function Test-ReverieCoverage {
  <#
  .SYNOPSIS
    Runs Reverie tests with coverage analysis.
  .DESCRIPTION
    Executes pytest with coverage reporting to measure test coverage.
    The project requires ≥80% coverage.
  .PARAMETER Html
    Generate HTML coverage report.
  .EXAMPLE
    Test-ReverieCoverage
    Runs tests and displays coverage report.
  .EXAMPLE
    Test-ReverieCoverage -Html
    Runs tests and generates HTML coverage report.
  .EXAMPLE
    rvcov
    Uses the alias to quickly run coverage analysis.
  #>
  [CmdletBinding()]
  param(
    [Parameter()]
    [switch]$Html
  )

  Write-ReverieMessage -Message 'Running tests with coverage analysis...' -Type 'Info'

  $command = 'uv run python -m pytest --cov=src --cov-report=term-missing'

  if ($Html) {
    $command += ' --cov-report=html'
  } else {
    $command += ' --cov-report=term'
  }

  Invoke-ReverieCommand -Command $command -PassThru

  if ($LASTEXITCODE -eq 0) {
    Write-ReverieMessage -Message 'Coverage analysis completed' -Type 'Success'
    if ($Html) {
      Write-ReverieMessage -Message 'HTML report generated in htmlcov/' -Type 'Info'
    }
  }
  else {
    Write-ReverieMessage -Message 'Coverage analysis failed' -Type 'Warning'
  }
}
Set-Alias -Name rvcov -Value Test-ReverieCoverage

function Invoke-ReverieCheck {
  <#
  .SYNOPSIS
    Runs all Reverie quality checks.
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
    Invoke-ReverieCheck
    Runs all quality checks.
  .EXAMPLE
    rvcheck
    Uses the alias to quickly run all checks.
  #>
  [CmdletBinding()]
  param(
    [Parameter()]
    [switch]$SkipTests
  )

  Write-ReverieMessage -Message 'Running all Reverie quality checks...' -Type 'Info'

  $failed = $false
  $totalChecks = if ($SkipTests) { 3 } else { 5 }

  # Check 1: Ruff lint
  Write-Progress -Activity 'Running Reverie Quality Checks' -Status 'Running ruff check' -PercentComplete (1 / $totalChecks * 100)
  Invoke-ReverieLint
  if ($LASTEXITCODE -ne 0) { $failed = $true }

  # Check 2: Ruff format
  Write-Progress -Activity 'Running Reverie Quality Checks' -Status 'Running ruff format --check' -PercentComplete (2 / $totalChecks * 100)
  Invoke-ReverieFormat -Check
  if ($LASTEXITCODE -ne 0) { $failed = $true }

  # Check 3: Mypy type checking
  Write-Progress -Activity 'Running Reverie Quality Checks' -Status 'Running mypy --strict' -PercentComplete (3 / $totalChecks * 100)
  Invoke-ReverieTypeCheck
  if ($LASTEXITCODE -ne 0) { $failed = $true }

  if (-not $SkipTests) {
    # Check 4: Tests
    Write-Progress -Activity 'Running Reverie Quality Checks' -Status 'Running pytest' -PercentComplete 80
    Test-Reverie
    if ($LASTEXITCODE -ne 0) { $failed = $true }

    # Check 5: Coverage
    Write-Progress -Activity 'Running Reverie Quality Checks' -Status 'Running pytest --cov' -PercentComplete 100
    Test-ReverieCoverage
    if ($LASTEXITCODE -ne 0) { $failed = $true }
  }

  # Clear progress bar
  Write-Progress -Activity 'Running Reverie Quality Checks' -Completed

  # Summary
  if ($failed) {
    Write-ReverieMessage -Message 'Some checks failed. Please fix issues before committing.' -Type 'Error'
    throw "Quality checks failed"
  }
  else {
    Write-ReverieMessage -Message 'All quality checks passed successfully!' -Type 'Success'
  }
}
Set-Alias -Name rvcheck -Value Invoke-ReverieCheck

#endregion

#region Application

function Start-Reverie {
  <#
  .SYNOPSIS
      Starts the Reverie CLI application.
  .DESCRIPTION
      Executes the Reverie CLI using uv run. Pass any additional arguments
      directly to the CLI.
  .PARAMETER Arguments
      Arguments to pass to the Reverie CLI.
  .EXAMPLE
      Start-Reverie
      Starts the Reverie CLI.
  .EXAMPLE
      Start-Reverie --help
      Shows Reverie CLI help.
  .EXAMPLE
      rvrun <app_command> <args>
      Uses the alias to quickly start the Reverie CLI with commands.
  #>
  [CmdletBinding()]
  param(
    [Parameter(ValueFromRemainingArguments)]
    [string[]]$Arguments
  )

  $command = "uv run python -m src $($Arguments -join ' ')"

  Write-ReverieMessage -Message 'Starting Reverie CLI...' -Type 'Info'
  Invoke-ReverieCommand -Command $command
}
Set-Alias -Name rvrun -Value Start-Reverie

#endregion

#region Convenience Functions

function Show-ReverieHelp {
  <#
  .SYNOPSIS
      Shows all available Reverie PowerShell commands.
  .DESCRIPTION
      Displays a summary of all available Reverie development helper functions
      with their aliases and descriptions.
  .EXAMPLE
      Show-ReverieHelp
      Displays all available commands.
  .EXAMPLE
      rvhelp
      Uses the alias to quickly show help.
  #>
  [CmdletBinding()]
  param()

  Write-Host ''
  Write-Host '╔══════════════════════════════════════════════════════════════════════╗' -ForegroundColor Cyan
  Write-Host '║            Reverie PowerShell Development Commands                  ║' -ForegroundColor Cyan
  Write-Host '╚══════════════════════════════════════════════════════════════════════╝' -ForegroundColor Cyan
  Write-Host ''

  $commands = @(
    @{Name = 'Remove-ReverieArtifacts'; Alias = 'rvrm'; Description = 'Remove build artifacts and caches' }
    @{Name = 'Install-ReverieDependencies'; Alias = 'rvinst'; Description = 'Install project dependencies' }
    @{Name = 'Initialize-ReverieDatabase'; Alias = 'rvinit'; Description = 'Initialize SurrealDB database' }
    @{Name = 'Invoke-ReverieMigration'; Alias = 'rvmig'; Description = 'Run database migrations' }
    @{Name = 'Start-ReverieDatabase'; Alias = 'rvstart'; Description = 'Start database containers' }
    @{Name = 'Stop-ReverieDatabase'; Alias = 'rvstop'; Description = 'Stop database containers' }
    @{Name = 'Invoke-ReverieLint'; Alias = 'rvlint'; Description = 'Run ruff linter' }
    @{Name = 'Invoke-ReverieFormat'; Alias = 'rvfmt'; Description = 'Format code with ruff' }
    @{Name = 'Invoke-ReverieTypeCheck'; Alias = 'rvtype'; Description = 'Run mypy type checker' }
    @{Name = 'Test-Reverie'; Alias = 'rvtest'; Description = 'Run test suite' }
    @{Name = 'Test-ReverieCoverage'; Alias = 'rvcov'; Description = 'Run tests with coverage' }
    @{Name = 'Invoke-ReverieCheck'; Alias = 'rvcheck'; Description = 'Run all quality checks' }
    @{Name = 'Start-Reverie'; Alias = 'rvrun'; Description = 'Start Reverie CLI' }
    @{Name = 'Show-ReverieHelp'; Alias = 'rvhelp'; Description = 'Show this help message' }
  )

  Write-Host 'PROJECT MANAGEMENT:' -ForegroundColor Cyan
  $commands[0..1] | ForEach-Object {
    Write-Host '  ' -NoNewline
    Write-Host $_.Name -ForegroundColor Magenta -NoNewline
    Write-Host (' ' * (35 - $_.Name.Length)) -NoNewline
    Write-Host "($($_.Alias))" -ForegroundColor Yellow -NoNewline
    Write-Host (' ' * (12 - "($($_.Alias))".Length)) -NoNewline
    Write-Host $_.Description -ForegroundColor Gray
  }
  Write-Host ''

  Write-Host 'DATABASE:' -ForegroundColor Cyan
  $commands[2..5] | ForEach-Object {
    Write-Host '  ' -NoNewline
    Write-Host $_.Name -ForegroundColor Magenta -NoNewline
    Write-Host (' ' * (35 - $_.Name.Length)) -NoNewline
    Write-Host "($($_.Alias))" -ForegroundColor Yellow -NoNewline
    Write-Host (' ' * (12 - "($($_.Alias))".Length)) -NoNewline
    Write-Host $_.Description -ForegroundColor Gray
  }
  Write-Host ''

  Write-Host 'CODE QUALITY & TESTING:' -ForegroundColor Cyan
  $commands[6..11] | ForEach-Object {
    Write-Host '  ' -NoNewline
    Write-Host $_.Name -ForegroundColor Magenta -NoNewline
    Write-Host (' ' * (35 - $_.Name.Length)) -NoNewline
    Write-Host "($($_.Alias))" -ForegroundColor Yellow -NoNewline
    Write-Host (' ' * (12 - "($($_.Alias))".Length)) -NoNewline
    Write-Host $_.Description -ForegroundColor Gray
  }
  Write-Host ''

  Write-Host 'APPLICATION:' -ForegroundColor Cyan
  $commands[12] | ForEach-Object {
    Write-Host '  ' -NoNewline
    Write-Host $_.Name -ForegroundColor Magenta -NoNewline
    Write-Host (' ' * (35 - $_.Name.Length)) -NoNewline
    Write-Host "($($_.Alias))" -ForegroundColor Yellow -NoNewline
    Write-Host (' ' * (12 - "($($_.Alias))".Length)) -NoNewline
    Write-Host $_.Description -ForegroundColor Gray
  }
  Write-Host ''

  Write-Host 'HELP:' -ForegroundColor Cyan
  $commands[13] | ForEach-Object {
    Write-Host '  ' -NoNewline
    Write-Host $_.Name -ForegroundColor Magenta -NoNewline
    Write-Host (' ' * (35 - $_.Name.Length)) -NoNewline
    Write-Host "($($_.Alias))" -ForegroundColor Yellow -NoNewline
    Write-Host (' ' * (12 - "($($_.Alias))".Length)) -NoNewline
    Write-Host $_.Description -ForegroundColor Gray
  }
  Write-Host ''

  Write-Host 'TIP: Use "Get-Help <CommandName> -Detailed" for more information' -ForegroundColor DarkGray
  Write-Host ''
}
Set-Alias -Name rvhelp -Value Show-ReverieHelp

#endregion

# Display welcome message
Write-Host ''
Write-Host '╔══════════════════════════════════════════════════════════════════════╗' -ForegroundColor Green
Write-Host '║                    ' -ForegroundColor Green -NoNewline
Write-Host 'Reverie Commands Loaded' -ForegroundColor Blue -NoNewline
Write-Host '                           ║' -ForegroundColor Green
Write-Host '╚══════════════════════════════════════════════════════════════════════╝' -ForegroundColor Green
Write-Host ''
Write-Host 'Reverie development helper functions loaded successfully!' -ForegroundColor Cyan
Write-Host "Type 'rvhelp' or 'Show-ReverieHelp' to see all available commands." -ForegroundColor Gray
Write-Host ''
