use clap::{ArgGroup, Parser, ValueEnum};

use crate::sink::SinkStmtTemplate;

const SINK_DB_STMT_LONG_HELP: &str = r#"SQL statement template executed once per output row.

Use placeholders wrapped in braces; they are replaced with sqlx bind parameters.
Supported placeholders: {product_id}, {warehouse_id}, {quantity}, {reserved}, {incoming}, {outgoing}, {buildable}, {free_immediately}, {virtual_available}.

Example:
INSERT INTO stock_availability (product_id, warehouse_id, quantity, virtual_available)
VALUES ({product_id}, {warehouse_id}, {quantity}, {virtual_available})
ON CONFLICT (product_id, warehouse_id) DO UPDATE
SET quantity = EXCLUDED.quantity,
    virtual_available = EXCLUDED.virtual_available;"#;

#[derive(Parser, Debug)]
/// Magic stock level calculator for Odoo
#[command(
    version,
    about,
    long_about = None,
    group(
        ArgGroup::new("output_target")
            .args(["stdout", "sink_db_stmt"])
            .required(true)
            .multiple(true)
    )
)]
pub struct Args {
    #[arg(long)]
    pub warehouse: i32,

    #[arg(long)]
    pub product: Vec<i32>,

    #[arg(long)]
    pub src_db_url: String,

    #[arg(long, value_enum, default_value_t = LogLevel::Warn)]
    pub log_level: LogLevel,

    #[arg(
        long,
        help = "Emit signed values; by default, numeric outputs are clamped to zero"
    )]
    pub allow_negative: bool,

    #[arg(
        long,
        value_enum,
        num_args = 0..=1,
        default_missing_value = "human"
    )]
    pub stdout: Option<StdoutFormat>,

    #[arg(long, requires = "sink_db_stmt")]
    pub sink_db_url: Option<String>,

    #[arg(long, requires = "sink_db_url", long_help = SINK_DB_STMT_LONG_HELP)]
    pub sink_db_stmt: Option<SinkStmtTemplate>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
pub enum StdoutFormat {
    Human,
    Jsonl,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
pub enum LogLevel {
    Off,
    Error,
    Warn,
    Info,
    Debug,
    Trace,
}

impl LogLevel {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Off => "off",
            Self::Error => "error",
            Self::Warn => "warn",
            Self::Info => "info",
            Self::Debug => "debug",
            Self::Trace => "trace",
        }
    }
}

#[cfg(test)]
mod tests {
    use clap::Parser;

    use super::Args;

    fn base_args() -> Vec<&'static str> {
        vec![
            "odoo-stock-availability",
            "--warehouse",
            "1",
            "--src-db-url",
            "postgres://user:pass@localhost:5432/odoo",
            "--stdout",
        ]
    }

    #[test]
    fn allow_negative_defaults_to_false() {
        let args = Args::parse_from(base_args());
        assert!(!args.allow_negative);
    }

    #[test]
    fn allow_negative_can_be_enabled() {
        let mut argv = base_args();
        argv.push("--allow-negative");

        let args = Args::parse_from(argv);
        assert!(args.allow_negative);
    }
}
