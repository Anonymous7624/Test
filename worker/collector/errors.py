"""Collector-specific errors (kept tiny to avoid import cycles)."""


class CollectorInterruptedError(Exception):
    """Collection stopped due to cancellation, Ctrl+C, or browser/context already closed."""
