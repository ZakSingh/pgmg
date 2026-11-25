# Change severity: will this plan break running clients?

Every change in a `pgmg plan` carries a **client-compatibility severity**, and
the plan carries the worst case as a rollup. It answers one question: *does
applying this plan break clients compiled against the current schema?* — the
question that decides whether you can roll a plan out under live traffic,
need clients to reconnect afterwards, or must redeploy clients first.

| Severity | Meaning |
|---|---|
| `safe` | Old clients unaffected. |
| `transient` | Old clients briefly error until their statement and type caches flush. Everything they used is still there, but an OID churned or a row shape grew, invalidating cached prepared statements, plans and type definitions. |
| `breaking` | Old clients permanently broken until redeployed: something they reference is gone, renamed, retyped or moved. |

Severity is informational only — it never gates `apply` and never changes
exit codes. `pgmg status` reports the same classification.

## Why most updates are never better than `transient`

pgmg applies an object update as **DROP + CREATE**, which gives the object a
new OID. Already-connected clients holding prepared statements against the old
OID error once until they re-prepare (pgmg's `pgmg.apply_succeeded` NOTIFY
exists so listeners can reset their caches).

The exception is an edited function or procedure whose interface is unchanged
and whose parameter defaults are all still there: it is replaced in place with
`CREATE OR REPLACE`, keeps its OID, and is `safe`. A routine with an unchanged
interface is still dropped and recreated, and so `transient`, when it is bound
to something else that is being dropped in the same apply (its result type is
a view that is recreated, for example) or when a parameter default was
removed.

What separates `transient` from `breaking` is whether everything an old
client relied on in the object's **interface** survives:

- for functions/procedures: the identity argument types, parameter names, and
  result shape,
- for views/materialized views/composite types: the output columns (name,
  relative order, type),
- for enums: the label set,
- for domains: the base type,
- for operators: the operand and result types.

## Additions are not breaking

A change that only adds to an interface is `transient`, never `breaking`:

- a column added to a table (`ALTER TABLE ... ADD COLUMN`), a view or a
  materialized view,
- an attribute added to a composite type,
- a column added to a function's row result (`RETURNS TABLE`, or two or more
  `OUT` parameters),
- a parameter with a default added after a function's existing parameters,
- a label added to an enum.

This assumes clients that find columns and attributes by name and ignore the
ones they do not know, which is what a client has to do to be deployed
separately from its database. A client that reads rows by position, or checks
that a composite or enum matches the definition it was built with exactly,
will break on these changes while pgmg reports `transient`.

They are `transient` and not `safe` because a connection opened before the
change still holds the old picture: a statement prepared with a `*` fails with
"cached plan must not change result type", and a composite is encoded with the
attribute list the connection cached. Both clear once the client drops its
statement and type caches, which `pgmg.apply_succeeded` is the signal for.

Existing columns must keep their name, type and relative order. A change that
adds one column and removes, renames, retypes or reorders another is
`breaking`.

The old interface is read from the live catalog (`pg_proc`, `pg_attribute`,
`pg_enum`, …); the new one is derived from your file's DDL, with type names
normalized through the same database so `int`, `int4`, and
`pg_catalog.int4` compare equal. For views, pgmg prepares the new defining
`SELECT` on its read-only plan connection — literally the same Describe a
client would issue — and compares the resulting columns. A column of a
domain type is compared by its base type, because that is what PostgreSQL
reports to a client describing a query.

## Classification matrix

Rollup: `safe < transient < breaking`; a plan's severity is the max over its
changes (`safe` when the plan is empty).

### Creates — always `safe`

A new object of any kind: nothing existing referenced it.

### Deletes

| Object kind | Severity | Why |
|---|---|---|
| comment, index, trigger, cron job | `safe` | Clients cannot be compiled against these; dropping them affects performance or side effects, never client correctness. |
| table, view, materialized view, function, procedure, aggregate, operator, type, domain | `breaking` | Client references permanently dangle. |

### Updates (DROP + CREATE, except routines replaced in place)

| Object kind | `transient` when | `breaking` when |
|---|---|---|
| comment | always `safe` — comments are re-set in place, nothing is dropped | — |
| index, trigger, cron job | always `safe` — no client-referencable interface | — |
| function, procedure | identity argument types, parameter names, and result shape identical, but the routine had to be dropped (see above); or defaulted parameters were added after the existing ones; or columns were added to a row result. Replaced in place it is `safe`. | an existing parameter changed type or was removed, a parameter was added without a default or before existing ones, the result changed in any other way (including a second `OUT` parameter on a scalar result), or a parameter was renamed (named-notation callers like `fn(a => 1)` would break) |
| aggregate | input types and result type identical | input or result types changed |
| view, materialized view | output columns identical, or columns added | an existing column removed, reordered, renamed, or retyped |
| type (enum) | labels only appended (every old label survives in order) | label removed, renamed, or reordered |
| type (composite) | attributes identical, or attributes added | an existing attribute removed, reordered, renamed, or retyped |
| domain | base type identical (constraint changes affect writes, not compiled clients) | base type changed |
| operator | operand types identical (result type also checked when the implementing function resolves) | operand or result types changed |
| table | — | **always `breaking`** — updating a declaratively-managed table is `DROP TABLE` + `CREATE TABLE`, which destroys the data; keep tables in `migrations/` |

### Migrations

A migration's severity is the worst of its statements:

| Statements | Severity |
|---|---|
| `CREATE TABLE / INDEX / SEQUENCE / EXTENSION / SCHEMA / VIEW / MATERIALIZED VIEW / FUNCTION / TRIGGER / TYPE / DOMAIN / AGGREGATE / OPERATOR`, `COMMENT ON`, DML (`INSERT` / `UPDATE` / `DELETE` / `SELECT`, including `cron.schedule`), `COPY`, `SET`, `TRUNCATE`, `CLUSTER`, `VACUUM`, `ALTER ... OWNER TO`, `GRANT` | `safe` |
| `ALTER TYPE ... ADD VALUE` | `safe` |
| `ALTER TABLE` forms that leave the row shape alone: `ADD / DROP / VALIDATE CONSTRAINT`, `SET / DROP DEFAULT`, `SET NOT NULL`, identity and generated-expression changes, trigger and row-security switches, `OWNER TO`, storage and statistics settings | `safe` |
| `ALTER TABLE ... ADD COLUMN`, `ALTER TYPE ... ADD ATTRIBUTE` | `transient` |
| every other `ALTER TABLE` form (`DROP COLUMN`, `ALTER COLUMN ... TYPE`, `DROP NOT NULL`, `SET SCHEMA`, partition changes, anything pgmg does not recognize), `ALTER VIEW / INDEX / SEQUENCE`, `ALTER TYPE ... DROP / ALTER ATTRIBUTE`, `DROP ...`, `ALTER ... RENAME`, `REVOKE`, `ALTER TYPE ... RENAME VALUE` | `breaking` |
| anything unparseable or unrecognized (including `DO` blocks) | `breaking`, with a logged warning |

## Conservative defaults and caveats

- **Unknown means breaking.** Any operation pgmg cannot classify — a
  statement kind it doesn't recognize, DDL it cannot parse, an object missing
  from the catalog, an unresolvable type — is reported as `breaking` and
  logged as a warning (`RUST_LOG=warn` makes these visible). False alarms err
  toward caution, never the reverse.
- **`DROP NOT NULL` is `breaking`.** The row shape is unchanged, but a client
  that decodes the column as non-null fails on the first NULL it reads.
- **A statement with several subcommands takes the worst one.**
  `ALTER TABLE t ADD COLUMN a int, DROP COLUMN b` is `breaking`.
- **Severity measures client compatibility, not data safety or locks.**
  `TRUNCATE` is `safe` here despite destroying data; a non-concurrent
  `CREATE INDEX` is `safe` despite blocking writes while it builds.
- **typmods are not compared.** `varchar(10) → varchar(20)` reads as
  `transient`: clients bind by type OID, and typmods are not part of the wire
  interface.
- **A view that uses something the same plan adds is checked against
  stand-ins.** The new query of a view that exposes a column a pending
  migration adds (or a column a view upstream of it gains) cannot be prepared
  before the apply. For views that would otherwise be `breaking`, pgmg builds
  the post-apply shapes in scratch schemas named `pgmg_shadow__*`: empty copies
  of the altered tables with the new columns, the types the migrations create,
  and the plan's new and changed views on top. This happens in a transaction
  that is always rolled back and takes no lock stronger than `ACCESS SHARE` on
  an existing object. It needs the `CREATE` privilege on the database; without
  it, or when a stand-in cannot be built (the view uses a table the migration
  creates, or an enum label it adds), the view stays `breaking`.
- **Cascade recreations are probed against pre-apply state.** When a
  migration alters a table in a way that requires it (see "Which table changes
  recreate dependents" in the README) or an upstream object changes, dependent
  objects are recreated and probed against the *current* database. A dependent view
  whose upstream shape is about to change can individually read `transient`,
  but the root cause (the migration or upstream change) carries `breaking`,
  so **the plan rollup is always correct** — trust the rollup over any single
  operation's label.
- **Enum appends are `transient`, not `safe`.** The recreation churns the
  type OID; and a client that deserializes enums exhaustively may reject rows
  carrying the new label once data uses it.
- **A rename appears as delete + create.** Removing an object and adding one
  with a new name yields a `breaking` delete and a `safe` create; pgmg does
  not attempt to pair them.
- **Aggregates are the weakest classifier.** The result type is derived from
  `FINALFUNC`/`STYPE` resolved against the pre-apply catalog; when that
  fails, the update is reported `breaking` with a warning.

## Where it appears

Text output — a tag per change plus a rollup line:

```
Object Changes:
  ~ UPDATE FUNCTION get_user_activity (DDL content has changed) [transient]
  > MIGRATION 003_add_orders [breaking]

Client compatibility: BREAKING (1 breaking, 1 transient, 0 safe)
```

JSON output (`pgmg plan --format json`, same for `status`):

```json
{
  "changes": [
    { "type": "update_object", "...": "...", "severity": "transient" },
    { "type": "apply_migration", "name": "003_add_orders", "severity": "breaking" }
  ],
  "severity": "breaking",
  "severity_counts": { "safe": 0, "transient": 1, "breaking": 1 }
}
```

## Apply lifecycle events

When `pgmg apply` has pending changes it announces its lifecycle on three
NOTIFY channels. Every `apply_initiated` is followed by exactly one of
`apply_succeeded` or `apply_failed`; nothing is emitted when there is nothing
to apply.

**`pgmg.apply_initiated`** — sent **before executing anything**, carrying the
plan's severity rollup, so a listener can react ahead of the change (pause
traffic or drain in-flight work when a `breaking` apply is starting):

```json
{ "severity": "breaking", "severity_counts": { "safe": 0, "transient": 1, "breaking": 1 }, "changes": 2 }
```

**`pgmg.apply_succeeded`** — sent inside the apply transaction, so it arrives
only once the changes have committed. This is the signal to reset statement
caches. The payload carries the plan's severity rollup alongside the change
counts, so a listener that missed `apply_initiated` (NOTIFY is not queued for
disconnected sessions) can still tell that a `breaking` apply just committed:

```json
{ "severity": "breaking", "severity_counts": { "safe": 0, "transient": 1, "breaking": 1 }, "migrations_applied": 1, "objects_created": 0, "objects_updated": 2, "objects_deleted": 0 }
```

**`pgmg.apply_failed`** — sent when an announced apply errors before
committing. The payload mirrors `apply_initiated` (so the two correlate) plus
the error that stopped it, truncated to fit the NOTIFY payload:

```json
{ "severity": "breaking", "severity_counts": { "safe": 0, "transient": 0, "breaking": 1 }, "changes": 1, "error": "Failed migration ..." }
```

A listener that paused work on `apply_initiated` resumes on either terminal
event. In transactional mode a failed apply rolled back completely — the
schema is unchanged. In auto-commit mode (fresh builds, `--test-mode`) a
failure can leave the schema partially changed, so treat `apply_failed` there
as a reason to investigate, not proof of no change.

Delivery mechanics: `apply_initiated` and `apply_failed` are deliberately
sent outside the apply transaction (a NOTIFY inside it would be held until
commit, or discarded by the rollback); `apply_succeeded` is deliberately sent
inside it so it can never arrive for changes that didn't land.
