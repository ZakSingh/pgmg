# pgmg - PostgreSQL Migration Manager

A tool for managing PostgreSQL schema migrations with automatic dependency resolution and state tracking.

## Overview

pgmg handles both traditional sequential migrations for tables and declarative management of views, functions, and types. It automatically manages dependencies and ensures your database objects are recreated in the correct order when table changes affect them. This means that you can manage your views, functions, and types just like normal code, and that you do not need to order them alphanumerically in your filesystem.

## Installation

Prebuilt Linux binaries are attached to the [`latest` prerelease](https://github.com/ZakSingh/pgmg/releases/tag/latest). Every push to `main` publishes two assets per architecture:

| Asset | Behaviour |
|-------|-----------|
| `pgmg-linux-x86_64`, `pgmg-linux-aarch64` | Always the current tip of `main`. Overwritten on every push. |
| `pgmg-linux-x86_64-<sha>`, `pgmg-linux-aarch64-<sha>` | Built from exactly that commit. Immutable once uploaded. |

If you depend on pgmg as a library at a pinned git rev, download the matching sha-suffixed asset so the CLI you run is the same commit you link:

```bash
# Extract the pinned rev from Cargo.lock and fetch the matching binary
SHA=$(grep -A2 'name = "pgmg"' Cargo.lock | grep -o '#[0-9a-f]\{40\}' | tr -d '#')
gh release download latest --repo ZakSingh/pgmg --pattern "pgmg-linux-x86_64-$SHA" --output pgmg
chmod +x pgmg

# Confirm the binary is the commit you expect
./pgmg --version   # pgmg 0.1.0 (<sha>)
```

`pgmg --version` reports the commit the binary was built from. Local builds read it from git and append `-dirty` when the working tree has uncommitted changes; set `PGMG_GIT_SHA` at build time to override.

## Usage

### Basic Commands

```bash
# Analyze what changes need to be applied
pgmg plan --migrations-dir=./migrations --code-dir=./sql --connection-string=...

# Apply pending changes
pgmg apply --migrations-dir=./migrations --code-dir=./sql --connection-string=...
```

### Directory Structure

```
project/
├── migrations/           # Sequential migration files
│   ├── 001_initial_tables.sql
│   ├── 002_add_user_email.sql
│   └── 003_add_orders_table.sql
│
└── sql/                  # Declarative SQL objects
    ├── views/
    │   ├── active_users.sql
    │   └── user_stats.sql
    ├── functions/
    │   ├── calculate_total.sql
    │   └── get_user_summary.sql
    └── types/
        └── user_status.sql
```

### The `plan` Command

Shows what changes would be applied without modifying the database:

```bash
$ pgmg plan --migrations-dir=./migrations --code-dir=./sql --connection-string=...

PLAN:
Pending data migration: 003_add_orders_table.sql
  Affected objects that will be recreated:
    - view.user_stats (depends on modified tables)
    - function.get_user_summary (depends on user_stats)

Code changes detected:
  - views/active_users.sql (modified)
  - functions/calculate_total.sql (new)

Execution order:
  1. DROP VIEW user_stats CASCADE
  2. Run migration: 003_add_orders_table.sql  
  3. CREATE VIEW user_stats (from sql/views/user_stats.sql)
  4. CREATE FUNCTION get_user_summary (from sql/functions/get_user_summary.sql)
  5. CREATE OR REPLACE VIEW active_users (from sql/views/active_users.sql)
  6. CREATE FUNCTION calculate_total (from sql/functions/calculate_total.sql)
```

The plan command is read-only and can be run safely at any time.

Each planned change is tagged with a client-compatibility severity —
`[safe]`, `[transient]`, or `[breaking]` — and the plan reports the worst
case as a `Client compatibility` rollup (also in `--format json` as
`severity` / `severity_counts`). This answers whether applying the plan
breaks clients compiled against the current schema. See
[docs/severity.md](docs/severity.md) for the full classification.

### The `apply` Command

Executes the changes identified by plan:

```bash
$ pgmg apply

Executing changes:
  ↓ Dropping view: user_stats CASCADE
  ✓ Applied migration: 003_add_orders_table.sql
  ✓ Created view: user_stats
  ✓ Created function: get_user_summary  
  ✓ Replaced view: active_users
  ✓ Created function: calculate_total

Updated state tracking:
  ✓ Recorded migration: 003_add_orders_table.sql
  ✓ Updated 4 object hashes

All changes applied successfully.
```

### Common Workflows

#### Adding a new table with dependent views

1. Create your migration file:
```sql
-- migrations/004_add_products_table.sql
CREATE TABLE products (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    price DECIMAL(10,2)
);
```

2. Create or update views that use the new table:
```sql
-- sql/views/product_summary.sql
CREATE VIEW product_summary AS
SELECT COUNT(*) as total_products, AVG(price) as avg_price
FROM products;
```

3. Run plan to see what will happen:
```bash
pgmg plan --migrations-dir=./migrations --code-dir=./sql --connection-string=...
```

4. Apply the changes:
```bash
pgmg apply  --migrations-dir=./migrations --code-dir=./sql --connection-string=...
```

#### Modifying a table that has dependent views

1. Create your migration:
```sql
-- migrations/005_add_user_score.sql
ALTER TABLE users ADD COLUMN score INTEGER DEFAULT 0;
```

2. Update any views that should use the new column:
```sql
-- sql/views/user_stats.sql (modified)
CREATE VIEW user_stats AS
SELECT id, name, score, COUNT(orders.id) as order_count
FROM users
LEFT JOIN orders ON users.id = orders.user_id
GROUP BY users.id, users.name, users.score;
```

3. Run pgmg - it will automatically handle dropping and recreating the view:
```bash
pgmg plan [args...]  # See that user_stats will be recreated
pgmg apply [args...] # Execute the changes
```

#### Code-only changes

When you only modify SQL files without table migrations:

```bash
# Edit a view or function
vim sql/views/user_stats.sql

# Apply the changes directly
pgmg apply --code-dir=./sql --connection-string=...
```

#### Live reloading of code-only changes

During development, you can run `pgmg watch --code-dir=./sql` to automatically reload database
objects upon file changes.

## Implementation

### State Tracking

pgmg uses two tables to track state:

```sql
-- Track which migrations have been applied
CREATE TABLE pgmg_migrations (
    name TEXT PRIMARY KEY,
    applied_at TIMESTAMP NOT NULL DEFAULT NOW()
);

-- Track current state of all SQL objects
CREATE TABLE pgmg_state (
    object_name TEXT PRIMARY KEY,      -- e.g. 'view.user_stats'
    object_hash TEXT NOT NULL,         -- SHA256 of the SQL definition
    last_applied TIMESTAMP NOT NULL DEFAULT NOW()
);
```

### How It Works

1. **Dependency Analysis**
    - Parses SQL files to build a dependency graph
    - Uses PostgreSQL's error messages to discover dependencies
    - Orders operations to respect dependencies

2. **Change Detection**
    - Compares file hashes with stored hashes in `pgmg_state`
    - Identifies new, modified, and deleted objects
    - Detects which objects are affected by table migrations

3. **Migration Wrapping**
    - When a pending migration alters a table in a way its dependents cannot
      survive, pgmg:
        - Drops the dependent objects before the migration
        - Runs the migration
        - Recreates objects using current definitions from sql/
    - What counts depends on the `ALTER TABLE` subcommand; see
      [Which table changes recreate dependents](#which-table-changes-recreate-dependents).
    - Editing a managed object follows the same principle; see
      [What gets dropped and recreated](#what-gets-dropped-and-recreated).

4. **Atomic Execution**
    - All changes run in a single transaction when possible
    - On success: updates both state tables
    - On failure: rolls back everything

### What gets dropped and recreated

pgmg recreates an object only when something forces it to. There are two
forcing reasons, and they apply the same way whether the change comes from an
edited file or from a migration:

- **Binding.** PostgreSQL ties some objects to others in its catalog and will
  not let the one be dropped or altered while the other exists. A view is bound
  to the tables, views and functions it uses; a trigger to its table and its
  function; an index to its table; a function to the types in its signature.
  When an object is dropped, everything bound to it is dropped first and
  recreated afterwards.
- **Stored results.** A materialized view or an index holds values computed
  when it was built. When anything it is derived from changes, it is rebuilt,
  even if nothing forces a drop. "Derived from" is followed through function
  calls: editing a function rebuilds a materialized view that reaches it
  through any chain of other functions.

Everything else is left in place. In particular:

- **A function or procedure body is not a binding.** A body written as a string
  (`AS $$ ... $$`) is parsed when it runs, so the tables and views it queries
  are looked up by name each time. Such a routine stays in place when a
  relation it queries is recreated or altered. What does bind it is its
  signature: a parameter or result of a table's or view's row type, a
  `tbl.col%TYPE` parameter, a type used in a default. A variable declared with
  a composite type, or a cast to one, binds as well. SQL-standard bodies
  (`BEGIN ATOMIC`, `RETURN expr`) are parsed at creation and bind like a view,
  to the functions they call as well as the relations they read.
- **An edited function or procedure is replaced in place** with `CREATE OR
  REPLACE` when its interface is unchanged: same argument types, parameter
  names, result type and `OUT` columns, and no parameter default removed. It
  keeps its OID, its privileges and everything built on it. If the interface
  changed, it is dropped and recreated along with what is bound to it. So is
  a routine whose installed body is SQL-standard and whose new body is a
  string. The plan marks in-place updates as "replaced in place"
  (`"in_place": true` in JSON output).

A routine left in place can still be broken by the change: a column its body
reads is dropped, a function it calls takes different arguments. `LANGUAGE
sql` routines that call or query something the apply changed are therefore
re-validated by replacing them with their own unchanged definition, which
keeps their OID and privileges; a body that is no longer valid fails the apply.
PL/pgSQL bodies are only checked by plpgsql_check, so for those this happens
when `check_plpgsql` is on.

### Which table changes recreate dependents

pgmg reads each pending migration's `ALTER TABLE` statements and recreates
what is bound to the table only when the change calls for it. The most
disruptive change to a table across all pending migrations decides how that
table's dependents are treated.

| `ALTER TABLE ...` | Dependents |
|---|---|
| `ALTER COLUMN ... TYPE`, `DROP COLUMN` | Everything bound to the table is dropped before the migration and recreated after. PostgreSQL refuses these while a dependent view uses the column. |
| `DROP CONSTRAINT` | Left in place, unless something in the database depends on the constraint itself (a view that groups by a primary key and selects other columns does). Then, and when pgmg cannot find the table to check, as above. A constraint that does not exist (`DROP CONSTRAINT IF EXISTS`) has no dependents, unless a pending migration also renames constraints. |
| `ADD COLUMN` | Left in place, except those whose definition the new column changes (below). |
| `ADD CONSTRAINT`, `VALIDATE CONSTRAINT`, `SET`/`DROP DEFAULT`, `SET`/`DROP NOT NULL`, identity and generated-column changes, `OWNER TO`, storage parameters, `SET TABLESPACE`, `ENABLE`/`DISABLE TRIGGER`, row-level security switches | Left in place. |
| Anything else (`INHERIT`, `ATTACH PARTITION`, rules, ...) | Everything bound to the table is dropped and recreated, as a conservative default. |

Migrations that change a **type** are handled the same way. When a pending
migration renames, drops, moves or restructures a type or domain (`ALTER TYPE
... RENAME TO`, `DROP TYPE`, `SET SCHEMA`, `ADD`/`DROP`/`ALTER ATTRIBUTE`),
everything bound to that type is dropped first and recreated after. This is
what makes the usual way of removing an enum label work: rename the type
aside, create its replacement, move the columns over, drop the old one.
`ALTER TYPE ... ADD VALUE` and `RENAME VALUE` leave dependents alone.

A table's row type is a type too. When a pending migration drops a table,
renames it or moves it to another schema, routines whose signature uses its
row type (`RETURNS SETOF item`, a parameter of type `item`) are dropped first
and recreated after, so they end up bound to whatever carries the name once
the migration has run.

`ALTER VIEW`, `ALTER MATERIALIZED VIEW`, `ALTER INDEX` and `ALTER SEQUENCE`
in a migration recreate nothing.

PostgreSQL resolves a view's column references once, when the view is created.
After an `ADD COLUMN`, pgmg therefore still recreates the dependents whose
source would now resolve differently, so the database keeps matching what a
fresh build of your files produces:

- definitions using `*`, `t.*` or `TABLE t`, which would otherwise not expose the new column;
- views and other parsed definitions that use a function whose signature names
  the table (`SELECT ... FROM list_items()` where the function `RETURNS SETOF
  item`), which recorded the function's result columns when they were created;
- definitions using `NATURAL JOIN`, whose join columns may now differ;
- definitions with an unqualified reference spelled like a new column, which
  the new column may capture or make ambiguous. If it is ambiguous the apply
  fails, exactly as a fresh build would.

Three consequences worth knowing:

- **Triggers stay on the table during the migration** unless the migration
  changes a column's type, drops a column, or drops a constraint something
  depends on. A backfill (`UPDATE`) in any other migration fires them, as it
  would in a migration with no `ALTER TABLE` at all. Use `ALTER TABLE ...
  DISABLE TRIGGER USER` around a backfill that must not fire them.
- **Functions exist while migrations run** unless they are bound to something
  being rebuilt. A migration that calls a managed function gets the version
  from before the apply.
- **Only top-level statements are analyzed.** An `ALTER TABLE` inside a `DO`
  block or a function called by the migration is not seen.

### Key Design Decisions

1. **Declarative SQL Objects**: Views, functions, and types are defined declaratively in the sql/ directory. The current file is always the source of truth.

2. **Hash-Based Change Detection**: Objects are updated when their content hash changes, ensuring the database matches the code.

3. **Automatic Dependency Resolution**: Rather than manually managing DROP/CREATE order, pgmg determines the correct sequence automatically.

4. **Unified Workflow**: Whether applying table migrations, code changes, or both, the workflow is consistent: plan then apply.

### Error Recovery

If an apply fails:
- The transaction rolls back, leaving the database unchanged
- State tables remain consistent with actual database state
- Running plan again shows the same pending changes
- Fix the SQL files and run apply again

### Advantages

- **No manual coordination**: Table changes that affect a view recreate it automatically
- **Version control friendly**: Only source SQL in git, not generated migrations
- **Self-healing**: Drift between code and database is automatically corrected
- **Safe iterations**: Plan shows exactly what will happen before execution

### Configuration

Instead of supplying the `--migrations-dir`, `--code-dir`, and `--connection-string` to every command, they can be specified in a `pgmg.toml` file in the root of your project.