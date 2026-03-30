"""
Production-Ready Logging Configuration for Skillab Turkey.

This module provides a centralized, structured logging system using structlog.
Supports both development (colored console) and production (JSON) modes.

Author: Skillab Turkey Team
Project: EU Horizon Skill Intelligence Hub
"""

import logging
import os
import sys
from typing import Any, Dict, List

import structlog
from structlog.types import FilteringBoundLogger


# ============================================================================
# Configuration Constants
# ============================================================================

LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO").upper()
ENVIRONMENT: str = os.getenv("ENVIRONMENT", "development").lower()

# Map string log levels to logging constants
LOG_LEVELS: Dict[str, int] = {
    "DEBUG": logging.DEBUG,
    "INFO": logging.INFO,
    "WARNING": logging.WARNING,
    "ERROR": logging.ERROR,
    "CRITICAL": logging.CRITICAL,
}


# ============================================================================
# Processor Configuration
# ============================================================================

def _get_shared_processors() -> List[Any]:
    """
    Define processors that are common to both dev and prod environments.
    
    Returns:
        List of structlog processors shared across all environments.
    """
    return [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_log_level,
        structlog.stdlib.add_logger_name,
        structlog.processors.TimeStamper(fmt="iso", utc=True),
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.processors.UnicodeDecoder(),
    ]


def _configure_development_logging() -> None:
    """
    Configure structlog for development environment.
    
    Provides colored, human-readable console output with detailed context.
    Ideal for local debugging and development workflows.
    """
    shared_processors = _get_shared_processors()
    
    structlog.configure(
        processors=[
            *shared_processors,
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )
    
    formatter = structlog.stdlib.ProcessorFormatter(
        processors=[
            structlog.stdlib.ProcessorFormatter.remove_processors_meta,
            structlog.dev.ConsoleRenderer(
                colors=True,
                exception_formatter=structlog.dev.rich_traceback,
            ),
        ],
        foreign_pre_chain=shared_processors,
    )
    
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(formatter)
    
    root_logger = logging.getLogger()
    root_logger.addHandler(handler)
    root_logger.setLevel(LOG_LEVELS.get(LOG_LEVEL, logging.INFO))


def _configure_production_logging() -> None:
    """
    Configure structlog for production environment.
    
    Provides structured JSON output suitable for log aggregation systems
    (e.g., ELK stack, CloudWatch, Datadog). Optimized for machine parsing.
    """
    shared_processors = _get_shared_processors()
    
    structlog.configure(
        processors=[
            *shared_processors,
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )
    
    formatter = structlog.stdlib.ProcessorFormatter(
        processors=[
            structlog.stdlib.ProcessorFormatter.remove_processors_meta,
            structlog.processors.JSONRenderer(),
        ],
        foreign_pre_chain=shared_processors,
    )
    
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(formatter)
    
    root_logger = logging.getLogger()
    root_logger.addHandler(handler)
    root_logger.setLevel(LOG_LEVELS.get(LOG_LEVEL, logging.INFO))


# ============================================================================
# Initialization
# ============================================================================

def _initialize_logging() -> None:
    """
    Initialize logging configuration based on environment.
    
    Automatically selects development or production logging based on
    the ENVIRONMENT variable. Called once during module import.
    """
    if ENVIRONMENT == "production":
        _configure_production_logging()
    else:
        _configure_development_logging()


# Initialize on module load
_initialize_logging()


# ============================================================================
# Public API
# ============================================================================

def get_logger(name: str) -> FilteringBoundLogger:
    """
    Create and return a configured logger instance for a specific module.
    
    This function provides a standardized way to create loggers throughout
    the application. Each logger automatically includes the module name,
    timestamp, and log level in every message.
    
    Args:
        name: The name of the module/component requesting the logger.
              Typically __name__ should be passed to maintain hierarchy.
    
    Returns:
        A configured structlog BoundLogger instance with full context.
    
    Example:
        >>> logger = get_logger(__name__)
        >>> logger.info("scraper_started", platform="kariyer.net", jobs_count=150)
        2026-03-09T14:56:32.123456Z [info     ] scraper_started [src.scraper.kariyer] jobs_count=150 platform=kariyer.net
    """
    return structlog.get_logger(name)


def set_log_level(level: str) -> None:
    """
    Dynamically change the logging level at runtime.
    
    Useful for debugging specific issues without restarting the application.
    
    Args:
        level: New log level (DEBUG, INFO, WARNING, ERROR, CRITICAL).
    
    Raises:
        ValueError: If the provided level is not valid.
    
    Example:
        >>> set_log_level("DEBUG")  # Enable verbose logging
    """
    level_upper = level.upper()
    if level_upper not in LOG_LEVELS:
        valid_levels = ", ".join(LOG_LEVELS.keys())
        raise ValueError(
            f"Invalid log level '{level}'. Must be one of: {valid_levels}"
        )
    
    logging.getLogger().setLevel(LOG_LEVELS[level_upper])


def add_global_context(**kwargs: Any) -> None:
    """
    Add context variables that will be included in all subsequent log entries.
    
    Useful for adding request IDs, user IDs, or other contextual information
    that should persist across multiple log statements.
    
    Args:
        **kwargs: Key-value pairs to add to the global logging context.
    
    Example:
        >>> add_global_context(request_id="abc-123", user_id=42)
        >>> logger.info("processing_job")  # Will include request_id and user_id
    """
    structlog.contextvars.bind_contextvars(**kwargs)


def clear_global_context(*args: str) -> None:
    """
    Remove specific context variables or clear all if no arguments provided.
    
    Args:
        *args: Names of context variables to remove. If empty, clears all.
    
    Example:
        >>> clear_global_context("request_id")  # Remove specific key
        >>> clear_global_context()  # Clear all context
    """
    if args:
        structlog.contextvars.unbind_contextvars(*args)
    else:
        structlog.contextvars.clear_contextvars()
