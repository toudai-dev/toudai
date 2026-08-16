# Architecture Decision Records

Decision records explain why Lighthouse selected an architecture, not merely
what the current code does. Use a zero-padded sequence and descriptive name, for
example `001-native-json-attributes.md`.

Each record should contain:

1. **Status and date** — proposed, accepted, superseded, or rejected.
2. **Context** — product constraints and technical forces.
3. **Decision** — the selected behavior or boundary.
4. **Alternatives** — serious options considered and why they were not chosen.
5. **Evidence** — experiment records, production observations, and external
   references.
6. **Consequences** — benefits, costs, risks, and required follow-up work.
7. **Revisit conditions** — evidence that would justify changing the decision.

Do not rewrite accepted history when a decision changes. Add a superseding
record and link both directions.

## Index

| ID | Status | Decision |
|---|---|---|
| 000 | Accepted | [Unify event storage and separate native metrics](000-unified-events-separate-metrics.md) |
