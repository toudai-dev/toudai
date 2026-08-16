# Lighthouse Design Documentation

Lighthouse records planned work, measured experiments, and architecture
decisions separately so that later decisions can be traced back to reproducible
evidence.

- [`plans/`](plans/) tracks phased implementation work and open checklist items.
- [`experiments/`](experiments/) records benchmark setup, commands, results, and
  limitations.
- [`decisions/`](decisions/) records accepted architecture choices, alternatives,
  and consequences.

The current design is driven by the
[Unified Event Storage and Scale Validation plan](plans/000-unified_event_storage.org).

An experiment does not become a decision merely because one candidate performed
best in one run. Decision records should cite the experiments and product
constraints that justify the selected design.
