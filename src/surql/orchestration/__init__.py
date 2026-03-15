"""Multi-database migration orchestration.

This module provides tools for managing migrations across multiple
database instances and environments with various deployment strategies.
"""

from surql.orchestration.config import (
  EnvironmentConfig,
  EnvironmentRegistry,
  configure_environments,
  get_registry,
  register_environment,
  set_registry,
)
from surql.orchestration.coordinator import (
  DeploymentPlan,
  MigrationCoordinator,
  OrchestrationError,
  deploy_to_environments,
)
from surql.orchestration.health import (
  HealthCheck,
  HealthStatus,
  check_environment_health,
  verify_connectivity,
)
from surql.orchestration.strategy import (
  CanaryStrategy,
  DeploymentResult,
  DeploymentStatus,
  DeploymentStrategy,
  ParallelStrategy,
  RollingStrategy,
  SequentialStrategy,
)

__all__ = [
  # Config
  'EnvironmentConfig',
  'EnvironmentRegistry',
  'configure_environments',
  'get_registry',
  'register_environment',
  'set_registry',
  # Coordinator
  'DeploymentPlan',
  'MigrationCoordinator',
  'OrchestrationError',
  'deploy_to_environments',
  # Health
  'HealthCheck',
  'HealthStatus',
  'check_environment_health',
  'verify_connectivity',
  # Strategy
  'CanaryStrategy',
  'DeploymentResult',
  'DeploymentStatus',
  'DeploymentStrategy',
  'ParallelStrategy',
  'RollingStrategy',
  'SequentialStrategy',
]
