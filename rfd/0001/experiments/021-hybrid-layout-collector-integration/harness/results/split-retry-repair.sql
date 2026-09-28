-- Bounded repair demonstration (Experiment 021, split-retry).
-- The canonical payload committed first and is retrievable with its identity and
-- checksum, so a minimal serving row can be derived from it: routing identity,
-- timestamps, kind, and the payload checksum. Rebuilding the full typed serving
-- representation from the canonical payload is NOT implemented as a service
-- here; the semantics phase verifies those reconstruction rules against the
-- fixture instead.
INSERT INTO lh021.serving_logs (workspace_id, dataset, event_id, ts, kind, payload_sha256)
SELECT workspace_id, dataset, event_id, ts, 'log', payload_sha256
FROM lh021.canonical_events WHERE position(payload, '"fault-split-retry-1"') > 0;
