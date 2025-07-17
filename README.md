# pgmg - PostgreSQL Migration Manager

A tool for managing PostgreSQL schema migrations with automatic dependency resolution and state tracking.

## Overview

pgmg handles both traditional sequential migrations for tables and declarative management of views, functions, and types. It automatically manages dependencies and ensures your database objects are recreated in the correct order when table changes affect them. This means that you can manage your views, functions, and types just like normal code, and that you do not need to order them alphanumerically in your filesystem.

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
    - When a table migration affects views/functions, pgmg:
        - Drops the dependent objects before the migration
        - Runs the migration
        - Recreates objects using current definitions from sql/

4. **Atomic Execution**
    - All changes run in a single transaction when possible
    - On success: updates both state tables
    - On failure: rolls back everything

### Key Design Decisions

1. **Declarative SQL Objects**: Views, functions, and types are defined declaratively in the sql/ directory. The current file is always the source of truth.

2. **Hash-Based Change Detection**: Objects are recreated when their content hash changes, ensuring the database matches the code.

3. **Automatic Dependency Resolution**: Rather than manually managing DROP/CREATE order, pgmg determines the correct sequence automatically.

4. **Unified Workflow**: Whether applying table migrations, code changes, or both, the workflow is consistent: plan then apply.

### Error Recovery

If an apply fails:
- The transaction rolls back, leaving the database unchanged
- State tables remain consistent with actual database state
- Running plan again shows the same pending changes
- Fix the SQL files and run apply again

### Advantages

- **No manual coordination**: Table changes automatically trigger view recreation
- **Version control friendly**: Only source SQL in git, not generated migrations
- **Self-healing**: Drift between code and database is automatically corrected
- **Safe iterations**: Plan shows exactly what will happen before execution

### Configuration

Instead of supplying the `--migrations-dir`, `--code-dir`, and `--connection-string` to every command, they can be specified in a `pgmg.toml` file in the root of your project.