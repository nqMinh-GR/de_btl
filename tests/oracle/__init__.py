"""Independent golden oracle (stdlib only).

The oracle re-implements the domain rules of design C (§2-§7) from scratch so that the golden fixture's
``expected.json`` (hand-written) can be cross-checked three ways: hand values == oracle(fixture) ==
pipeline output. It MUST NOT import ``transitpulse`` or ``pyspark``; ``tests/unit/test_oracle_isolation.py``
walks the AST of every module in this package and enforces that, plus a stdlib-only allowlist.

Modules:
  * ``gtfs``     static GTFS: CSV reading, GTFS time, service anchor, calendar, scope, validation summary,
                 trip-instance resolution and stop resolution.
  * ``realtime`` freshness, uncertainty, prediction/target KPI, alert selector predicate, active state,
                 full/partial alert inheritance, entity schema conformance (mirrors processing/schemas.py).
  * ``scenario`` replays ``timeline.json`` runs and returns the logical outputs in the expected.json shape.
"""
