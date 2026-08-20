# Decision 001: Start With Hash-Distributed Event Sharding

- **Status:** Accepted
- **Date:** 2026-08-20

## Context

Lighthouse needs to scale full-resolution event storage beyond one ClickHouse
node without making a custom distributed query or placement system a prerequisite.
The common workload constrains tenant, dataset, and time, while individual
tenants can differ by orders of magnitude in volume. A tenant-leading shard key
would isolate tenants but would also limit every large tenant to one shard.

Facebook's original Scuba architecture sent queries to all storage leaves and
obtained interactive latency through time pruning, parallel scans, local partial
aggregation, and a hierarchical result merge. Its only general pruning metadata
was the time range of each batch. This broad-fanout design grew from terabytes
to petabytes before Meta's successor, Kraken, introduced deterministic,
per-dataset virtual partitions and targeted fanout. Kraken operated at a scale
of tens of thousands of datasets and up to thousands of query leaves, where the
fixed cost and network traffic of unnecessary fanout justified that additional
control plane.

ClickHouse already provides the required initial data plane: local MergeTree
tables perform scans and partial aggregation, while the `Distributed` engine
routes inserts, fans queries out, and merges results. Replication belongs to the
local tables rather than the `Distributed` table.

## Decision

1. Use local `ReplicatedMergeTree` event tables behind a ClickHouse
   `Distributed` table when Lighthouse moves beyond one shard.
2. Distribute events using a stable hash over `(tenant_id, dataset, event_id)`.
   Select the exact ClickHouse hash function when the final event identifier DDL
   is chosen, and keep it stable for the lifetime of that table generation.
3. Accept that tenant/dataset/time queries initially fan out to every shard.
   Large tenants benefit from all shards, and small tenants pay a fixed request
   and aggregation cost that must be measured.
4. Do not add virtual partition IDs, partition generations, a placement map, or
   an automatic shard manager initially.
5. Add physical shards without requiring immediate redistribution of retained
   data. Queries continue to visit all shards, while new events spread over the
   expanded topology and old placement ages out with retention. Rebalance only
   if measurements show that this convergence is too slow.
6. Preserve stable tenant, dataset, event, immutable ingest-batch, and replay
   identities so a virtual-partition layer can be introduced later without
   changing event identity.

This decision covers the query-serving shard strategy. Durable ingestion,
archive format, replication factor, and hot/archive query routing are separate
architecture decisions.

## Alternatives Considered

### Shard only by tenant

This minimizes fanout for a tenant query but constrains a high-volume tenant to
one shard and makes tenant skew a physical capacity problem. It is not selected
for a system expected to serve substantially different tenant volumes.

### Implement Kraken-style virtual partitions immediately

This would allow each dataset to use a configurable number of logical
partitions and permit targeted query fanout. It also requires versioned
partition metadata, historical placement tracking, routing availability,
rebalancing behavior, and additional recovery semantics. Those costs are not
justified before broad fanout is measured on a real ClickHouse cluster.

### Build a custom distributed query engine

Scuba and Kraken use specialized aggregation trees, but ClickHouse already
implements distributed scans and mergeable aggregate-state reduction. Replacing
that data plane would duplicate mature database behavior without evidence that
it is the limiting factor.

## Evidence

- [Scuba: Diving into Data at Facebook](https://www.vldb.org/pvldb/vol6/p1057-wiener.pdf)
- [Meta's Next-generation Realtime Monitoring and Analytics Platform](https://www.vldb.org/pvldb/vol15/p3522-mo.pdf)
- [Experiment 007: Full-day Map query baseline](../experiments/007-full-day-map-query-baseline.md)
- [Experiment 008: Tenant attribute acceleration](../experiments/008-tenant-attribute-acceleration.md)
- [Unified Event Storage and Scale Validation plan](../plans/000-unified_event_storage.org)

Kraken's published production study included datasets from 50 GB and 32 shards
to 150 TB, 1.27 trillion records, more than 3,500 columns, and 8,192 shards. Its
deterministic partition map improved P50 query latency and reduced fanout
network traffic at a scale well beyond the current Lighthouse evidence.

## Consequences

### Benefits

- Lighthouse can use standard ClickHouse distribution and replication.
- High-volume tenants scale across the whole cluster.
- Replay does not depend on an external partition registry.
- The initial control plane remains small and failure modes remain observable.
- Virtual partitioning remains available as an incremental optimization.

### Costs and risks

- Every ordinary tenant/time query pays the fanout cost of every shard.
- Event or trace lookup cannot target one shard unless the query exposes the
  complete sharding expression or a separate lookup path supplies placement.
- Adding shards does not automatically rebalance old parts.
- Distributed insertion must still define acknowledgement, retry, and
  deduplication semantics; the `Distributed` engine alone is not a durable
  ingestion source of truth.
- Tenant workload isolation requires quotas and admission control rather than
  physical tenant placement.

### Required follow-up

- Benchmark query latency, throughput, and network use across multiple shards.
- Test shard addition with retained data and continuing ingestion.
- Test replica failover and reconstruction independently of shard fanout.
- Record query coverage and participating shards in plan diagnostics.
- Select the exact stable hash expression with the final identifier schema.

## Revisit Conditions

Add a virtual-partition and placement layer if measurements show one or more of:

- small-dataset latency is dominated by shard fanout;
- fanout network traffic materially limits concurrency;
- the physical shard count grows large enough that fixed per-shard work is no
  longer negligible;
- datasets need substantially different parallelism or placement constraints;
- frequent topology changes make retention-based convergence inadequate; or
- complete-result accounting requires stable logical partition membership.
