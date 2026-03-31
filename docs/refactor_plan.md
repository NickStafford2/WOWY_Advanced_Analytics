• Findings

1. The rebuild entrypoint is currently broken and still encodes pre-rebuild interfaces. In scripts/rebuild_player_metrics_db.py:100 it calls scripts/cache_season_data.py with --all-seasons and --first-year, but scripts/cache_season_data.py:37 accepts a positional season, --start-year, and --end-year. The same script also still imports old module paths at scripts/rebuild_player_metrics_db.py:123 and scripts/rebuild_player_metrics_db.py:140 via wowy.web.refresh_cli and wowy.data.db_validation_cli. This is the clearest sign that you do not yet have one stable application boundary for “rebuild the project state”.
2. Your public metric response contract is inconsistent enough to create real runtime failures. RAWR custom rows use "season_id" in src/rawr_analytics/data/rawr.py:136, while WOWY custom rows use "season" in src/rawr_analytics/data/wowy.py:121. But the shared view layer assumes every custom row has "season" at src/rawr_analytics/metrics/metric_query/views.py:139. That means the “generic” metric interface is not actually generic yet. This is exactly the kind of porous boundary that forces repeated refactors.
3. The data package is doing orchestration and metric-specific input shaping instead of just persistence/retrieval, which fights the boundary you say you want. Its own package doc says it owns persistence and retrieval in src/rawr_analytics/data/__init__.py:1, but src/rawr_analytics/data/rawr.py:47 builds RAWR season inputs, completeness policy, opponent expansion, and even imports private metric internals at src/rawr_analytics/data/rawr.py:14. src/rawr_analytics/data/wowy.py:27 does the same for WOWY. This is the main architectural issue in the repo.
4. The metric-store code is a god module and mixes four concerns: cache freshness, metric computation, DB writes, and web-facing span shaping. You can see all of that stacked together in src/rawr_analytics/data/metric_store.py:85, src/rawr_analytics/data/metric_store.py:166, src/rawr_analytics/data/metric_store.py:293, and src/rawr_analytics/data/metric_store.py:355. The split into metric_store.py, metric_store_query.py, metric_store_views.py, and metric_store_scope.py is not a clean domain split; it is one concept fragmented by implementation detail.
5. You already have package APIs, but the rest of the code ignores them and imports deep internals directly, so the boundaries are nominal rather than real. game_cache exposes a public API in src/rawr_analytics/data/game_cache/__init__.py:1, but ingest bypasses it at src/rawr_analytics/workflows/nba_ingest.py:6. nba.source exports a public surface in src/rawr_analytics/nba/source/__init__.py:3, but workflows still reach into cache, parsers, dedupe, and models directly at src/rawr_analytics/workflows/nba_ingest.py:13. If deep imports remain normal, package boundaries will stay porous no matter how many files you rename.
6. There is at least one likely stale-cache bug caused by duplicated freshness logic. Store build writes a fingerprint from _build_cache_load_fingerprint in src/rawr_analytics/data/metric_store.py:426, but read- time validation compares against build_normalized_cache_fingerprint in src/rawr_analytics/data/metric_store_query.py:92, and that function hashes a different shape in src/rawr_analytics/data/game_cache/fingerprints.py:10. Even if they often match in practice, they should not be separate implementations.

Biggest Things To Do

1. Create one application/service layer and push all top-level scripts and web endpoints through it. For example: services/ingest.py, services/metric_store.py, services/metric_query.py. Scripts and Flask should call those services, not assemble workflows ad hoc.
2. Define typed request/response models for every public boundary and delete dict[str, Any] payload assembly from the core. MetricQuery, cached snapshots, custom-query rows, CSV rows, and web JSON payloads should each have one explicit model.
3. Move metric-specific dataset shaping out of data/ and into metrics/ or a dedicated datasets/ package. data/ should load canonical rows and save derived rows. It should not know how RAWR or WOWY are computed.
4. Collapse the metric-store cluster into cleaner nouns. I would aim for something like:
    data/metric_store_repository.py
    services/refresh_metric_store.py
    services/query_metric_store.py
    web/serializers/metric_payloads.py
    That is less DRY-looking than today’s file count, but much more DRY in responsibility.
5. Enforce package boundaries by policy:
    only import from package __init__ or explicitly designated public modules;
    ban cross-package imports of underscore modules;
    make deep internal imports a lint failure once the surfaces are stable.
6. Normalize naming around concepts, not mechanisms. Right now names like metric_store_views.py, metric_store_query.py, build_models.py, and normalize_game.py describe implementation tactics. Prefer stable nouns: catalog, snapshot, refresh, serializer, records, games.

I did not run tests per your instructions. This review is from static inspection of src/ and scripts/, and the three highest-value fixes are the rebuild entrypoint, the typed metric response contract, and moving metric assembly logic out of data/.
