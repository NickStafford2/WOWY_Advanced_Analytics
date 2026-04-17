"""Data-layer database access and explicit export I/O. Owns persistence and retrieval.

Examples:

- SQLite schema and repositories
- metric-store persistence
- cache metadata persistence
- loading canonical rows from the database
- writing derived metric results to the database
"""

from rawr_analytics.data.prepare_rebuild import prepare_rebuild_storage

__all__ = ["prepare_rebuild_storage"]
