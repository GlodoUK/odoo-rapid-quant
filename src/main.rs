#![doc = include_str!("../README.md")]
#![cfg_attr(test, allow(unused_results))]

use anyhow::Context;
use clap::Parser;
use product::{AvailabilityOutputMode, DiagnosticNode, OutputAvailability, ProductId};
use serde::Serialize;
use std::io::{BufWriter, Write, stdout};

use sqlx::postgres::PgPoolOptions;

use crate::{
    cli::{Args, LogLevel, StdoutFormat},
    sink::{SinkExecutionError, SinkPlaceholder},
};

mod cli;
mod dialect;
mod odoo;
mod product;
mod sink;
mod warehouse;

#[derive(Serialize)]
struct JsonlAvailabilityRow<'a> {
    product_id: i32,
    warehouse_id: i32,
    warehouse_name: &'a str,
    quantity: String,
    reserved: String,
    incoming: String,
    outgoing: String,
    buildable: String,
    free_immediately: String,
    virtual_available: String,
}

fn write_diagnostic_tree<W: Write>(
    writer: &mut W,
    node: &DiagnosticNode,
    mode: AvailabilityOutputMode,
    prefix: &mut Vec<bool>,
    is_last: bool,
) -> anyhow::Result<()> {
    let avail = node.availability.output(mode);

    if prefix.is_empty() {
        // Root node
        writeln!(
            writer,
            "Product {} [{}]",
            node.product_id.0,
            node.product.type_label()
        )?;
        // Indent for sub-lines: root has no connector, just two spaces
        let sub_indent = "  ";
        writeln!(writer, "{sub_indent}computed: {avail}")?;
        if let Some(ref q) = node.raw_quant {
            writeln!(
                writer,
                "{sub_indent}raw: qty={}, reserved={}, incoming={}, outgoing={}",
                q.quantity, q.reserved, q.incoming, q.outgoing
            )?;
        }
    } else {
        // Build prefix string from ancestor bools
        let mut line = String::new();
        for &has_sibling in prefix[..prefix.len() - 1].iter() {
            line.push_str(if has_sibling { "│   " } else { "    " });
        }
        let connector = if is_last { "└── " } else { "├── " };
        let req_qty = node
            .required_qty
            .expect("non-root node must have required_qty");
        writeln!(
            writer,
            "{}{}[requires {}] Product {} [{}]",
            line,
            connector,
            req_qty,
            node.product_id.0,
            node.product.type_label()
        )?;

        // Sub-lines indent: same prefix + continuation for this node's depth
        let mut sub_indent = line.clone();
        sub_indent.push_str(if is_last { "    " } else { "│   " });
        sub_indent.push_str("  ");

        writeln!(writer, "{sub_indent}computed: {avail}")?;
        if let Some(ref q) = node.raw_quant {
            writeln!(
                writer,
                "{sub_indent}raw: qty={}, reserved={}, incoming={}, outgoing={}",
                q.quantity, q.reserved, q.incoming, q.outgoing
            )?;
        }
        // Normalized line
        let qty_norm = mode.project(node.availability.quantity / req_qty);
        let free_norm = mode.project(node.availability.free_immediately() / req_qty);
        let virtual_norm = mode.project(node.availability.virtual_available() / req_qty);
        writeln!(
            writer,
            "{sub_indent}normalized (÷{req_qty}): qty={qty_norm}, free={free_norm}, virtual_available={virtual_norm}"
        )?;
    }

    let child_count = node.children.len();
    for (i, child) in node.children.iter().enumerate() {
        let child_is_last = i == child_count - 1;
        prefix.push(!child_is_last);
        write_diagnostic_tree(writer, child, mode, prefix, child_is_last)?;
        let _ = prefix.pop();
    }

    Ok(())
}

fn write_jsonl_row<W: Write>(
    writer: &mut W,
    product: ProductId,
    warehouse: &warehouse::Warehouse,
    availability: &OutputAvailability,
) -> anyhow::Result<()> {
    let row = JsonlAvailabilityRow {
        product_id: product.0,
        warehouse_id: warehouse.id.0,
        warehouse_name: &warehouse.name,
        quantity: availability.quantity.to_string(),
        reserved: availability.reserved.to_string(),
        incoming: availability.incoming.to_string(),
        outgoing: availability.outgoing.to_string(),
        buildable: availability.buildable.to_string(),
        free_immediately: availability.free_immediately.to_string(),
        virtual_available: availability.virtual_available.to_string(),
    };

    serde_json::to_writer(&mut *writer, &row)?;
    writer.write_all(b"\n")?;
    Ok(())
}

fn init_tracing(log_level: LogLevel) -> anyhow::Result<()> {
    let env_filter = if std::env::var_os("RUST_LOG").is_some() {
        tracing_subscriber::EnvFilter::try_from_default_env().context("invalid RUST_LOG value")?
    } else {
        tracing_subscriber::EnvFilter::try_new(log_level.as_str())
            .context("invalid --log-level value")?
    };

    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(env_filter)
        .with_target(false)
        .compact()
        .try_init()
        .map_err(|err| anyhow::anyhow!("failed to initialize tracing: {err}"))?;

    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Args::parse();
    init_tracing(cli.log_level)?;

    let src_pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&cli.src_db_url)
        .await?;

    let detected = odoo::OdooVersion::detect_from_database(&src_pool).await?;
    let adapter = detected.dialect(&src_pool).await?;
    tracing::info!("Using adapter for Odoo major {}.", adapter.major());

    let warehouse = adapter.warehouse(&src_pool, cli.warehouse).await?;

    let mut graph = product::Graph::new(src_pool, warehouse.clone(), adapter).await?;

    let requested_products: Vec<ProductId> = cli.product.iter().copied().map(ProductId).collect();

    if cli.stdout == Some(StdoutFormat::Diagnose) && requested_products.len() != 1 {
        anyhow::bail!("--stdout diagnose requires exactly one --product <ID>");
    }

    graph.collect(&requested_products).await?;

    let products = if requested_products.is_empty() {
        graph.computed_products()
    } else {
        requested_products
    };

    let output_mode = AvailabilityOutputMode::from_allow_negative(cli.allow_negative);

    if let Some(stdout_format) = cli.stdout {
        let lock = stdout().lock();
        let mut writer = BufWriter::new(lock);

        match stdout_format {
            StdoutFormat::Diagnose => {
                let root_id = products[0];
                let tree = graph
                    .diagnostic_tree(root_id, None)
                    .with_context(|| format!("product {} not found in graph", root_id.0))?;
                write_diagnostic_tree(&mut writer, &tree, output_mode, &mut vec![], true)?;
            }
            _ => {
                for product in &products {
                    let availability = graph.get(product).with_context(|| {
                        format!("missing availability for product_id={}", product.0)
                    })?;
                    let output = availability.output(output_mode);
                    match stdout_format {
                        StdoutFormat::Human => {
                            writeln!(writer, "{:?}, {}: {}", product, warehouse.name, output)?;
                        }
                        StdoutFormat::Jsonl => {
                            write_jsonl_row(&mut writer, *product, &warehouse, &output)?;
                        }
                        StdoutFormat::Diagnose => unreachable!(),
                    }
                }
            }
        }
    }

    if let Some(sink_stmt_template) = cli.sink_db_stmt.as_ref() {
        let sink_db_url = cli
            .sink_db_url
            .as_deref()
            .expect("clap requires --sink-db-url when --sink-db-stmt is set");

        let sink_pool = PgPoolOptions::new()
            .max_connections(1)
            .connect(sink_db_url)
            .await?;

        let mut tx = sink_pool.begin().await?;

        for product in &products {
            let availability = graph
                .get(product)
                .with_context(|| format!("missing availability for product_id={}", product.0))?;
            let output = availability.output(output_mode);

            let mut query = sqlx::query(&sink_stmt_template.sql);
            for placeholder in &sink_stmt_template.placeholders {
                query = match placeholder {
                    SinkPlaceholder::ProductId => query.bind(product.0),
                    SinkPlaceholder::WarehouseId => query.bind(warehouse.id.0),
                    SinkPlaceholder::Quantity => query.bind(output.quantity),
                    SinkPlaceholder::Reserved => query.bind(output.reserved),
                    SinkPlaceholder::Incoming => query.bind(output.incoming),
                    SinkPlaceholder::Outgoing => query.bind(output.outgoing),
                    SinkPlaceholder::Buildable => query.bind(output.buildable),
                    SinkPlaceholder::FreeImmediately => query.bind(output.free_immediately),
                    SinkPlaceholder::VirtualAvailable => query.bind(output.virtual_available),
                };
            }

            let _ =
                query
                    .execute(&mut *tx)
                    .await
                    .map_err(|source| SinkExecutionError::Execute {
                        product_id: product.0,
                        warehouse_id: warehouse.id.0,
                        source,
                    })?;
        }

        tx.commit().await?;
    }

    Ok(())
}
