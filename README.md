# odoo-rapid-quant

Fast(er) warehouse-level stock availability calculator for Odoo designed to produce output
useful for stock feeds, in larger databases with a large amount of nested products,
where the data doesn't have to be 100% upto date.

The tool reads directly from the database, extracts the current Odoo version and then reads product,
quant, move, and BOM data from a source Odoo Postgres database, computes availability,
and can output each computed row to:

- `stdout`
- a sink SQL statement executed per row
- or both at the same time

## Current support

- Odoo major version: 15

Additional versions will be supported later.

## Known limitations

- BoM apply on variants is currently unsupported
- SQL sink writes run inside a single transaction.
- If any row fails during sink execution, the transaction fails and is not committed.

## Rust toolchain

- Rust stable 1.85+ (edition 2024)

## Build

```bash
cargo build --release
```

## Basic usage

```bash
cargo run -- --warehouse <WAREHOUSE_ID> --src-db-url "postgres://user:pass@host:5432/db" --stdout
```

## CLI arguments

- `--warehouse <ID>`: Warehouse id to calculate against.
- `--src-db-url <URL>`: Source Postgres URL (Odoo database).
- `--log-level <off|error|warn|info|debug|trace>`: Tracing level for logs (default: `warn`).
- `--allow-negative`: Emit signed values. By default, all numeric output fields are clamped to `0`.
- `--product <ID>`: Optional product filter; can be repeated.
- `--stdout [human|jsonl]`: Opt-in stdout output. If no value is provided, defaults to `human`.
- `--sink-db-url <URL>`: Sink Postgres URL used when `--sink-db-stmt` is set.
- `--sink-db-stmt <SQL>`: SQL template executed once per computed row.

At least one output must be selected:

- `--stdout`
- and/or `--sink-db-stmt` (with `--sink-db-url`)

If neither is set, the command exits with an error.

## Sink SQL placeholders

Use placeholders in braces inside `--sink-db-stmt`:

- `{product_id}`
- `{warehouse_id}`
- `{quantity}`
- `{reserved}`
- `{incoming}`
- `{outgoing}`
- `{buildable}`
- `{free_immediately}`
- `{virtual_available}`

The tool converts placeholders into positional bind parameters (`$1`, `$2`, ...), then binds
typed values using `sqlx`.

## Stdout formats

- `human`: friendly text output (good for interactive runs).
- `jsonl`: one JSON object per line (good for scripts/pipes).

`jsonl` fields:

- `product_id`
- `warehouse_id`
- `warehouse_name`
- `quantity`
- `reserved`
- `incoming`
- `outgoing`
- `buildable`
- `free_immediately`
- `virtual_available`

Quantity values are emitted as strings to preserve decimal precision.

By default, numeric fields are clamped to `0`. This applies to:

- `quantity`
- `reserved`
- `incoming`
- `outgoing`
- `buildable`
- `free_immediately`
- `virtual_available`

Use `--allow-negative` to emit signed values for all of the above fields (for both stdout and sink placeholders).

## Logging

- Logs are emitted with `tracing` to `stderr` (so stdout stays script-friendly).
- Default level is `warn`.
- Use `--log-level` for quick control per run.
- `RUST_LOG` is supported and takes precedence over `--log-level`.

Examples:

```bash
# show info logs
cargo run -- --warehouse 1 --src-db-url "postgres://..." --log-level info --stdout

# full filter via env var (overrides --log-level)
RUST_LOG=odoo_stock_availability=debug cargo run -- --warehouse 1 --src-db-url "postgres://..." --log-level warn --stdout
```

## Examples

### 1) stdout only

```bash
cargo run -- \
  --warehouse 1 \
  --src-db-url "postgres://reporting:secret@localhost:5432/odoo" \
  --stdout
```

### 2) SQL sink only

```bash
cargo run -- \
  --warehouse 1 \
  --src-db-url "postgres://reporting:secret@localhost:5432/odoo" \
  --sink-db-url "postgres://etl:secret@localhost:5432/reporting" \
  --sink-db-stmt "INSERT INTO stock_availability (product_id, warehouse_id, quantity, virtual_available)
VALUES ({product_id}, {warehouse_id}, {quantity}, {virtual_available})
ON CONFLICT (product_id, warehouse_id) DO UPDATE
SET quantity = EXCLUDED.quantity,
    virtual_available = EXCLUDED.virtual_available"
```

### 3) stdout + SQL sink

```bash
cargo run -- \
  --warehouse 1 \
  --src-db-url "postgres://reporting:secret@localhost:5432/odoo" \
  --stdout \
  --sink-db-url "postgres://etl:secret@localhost:5432/reporting" \
  --sink-db-stmt "INSERT INTO stock_availability (product_id, warehouse_id, quantity)
VALUES ({product_id}, {warehouse_id}, {quantity})"
```

### 4) Filter to specific products

```bash
cargo run -- \
  --warehouse 1 \
  --src-db-url "postgres://reporting:secret@localhost:5432/odoo" \
  --stdout \
  --product 123 \
  --product 456
```

### 5) JSONL output for scripting

```bash
cargo run -- \
  --warehouse 1 \
  --src-db-url "postgres://reporting:secret@localhost:5432/odoo" \
  --stdout jsonl
```

### 6) Emit signed values (no clamping)

```bash
cargo run -- \
  --warehouse 1 \
  --src-db-url "postgres://reporting:secret@localhost:5432/odoo" \
  --stdout \
  --allow-negative
```

## License

This project is licensed under `LGPL-3.0-or-later`.

See `LICENSE` for details.

## Support and Contributing

Whilst the code is open source we have typically built these modules for ourselves, or for customers. As such all support outside of our customer base is limited/at our discretion.

We are happy to accept contributions.

All modules in this repo are released for use "AS IS" without any warranties of any kind, including, but not limited to their installation, use, or performance.

If you require support please contact us via [glo.systems](https://www.glo.systems/).
