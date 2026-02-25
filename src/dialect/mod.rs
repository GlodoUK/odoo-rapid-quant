use std::{collections::HashMap, error::Error, fmt};

use async_trait::async_trait;
use petgraph::graphmap::DiGraphMap;
use rust_decimal::Decimal;
use sqlx::PgPool;

use crate::{
    odoo::OdooVersion,
    product::{Product, ProductId, Quant},
    warehouse::Warehouse,
};

pub mod v15;

#[async_trait]
pub trait OdooAdapter: Send + Sync {
    fn major(&self) -> OdooVersion;

    async fn products(
        &self,
        pool: &PgPool,
        catalogue: &mut HashMap<ProductId, Product>,
        graph: &mut DiGraphMap<ProductId, Decimal>,
    ) -> Result<(), sqlx::Error>;

    async fn relations(
        &self,
        pool: &PgPool,
        graph: &mut DiGraphMap<ProductId, Decimal>,
    ) -> Result<(), sqlx::Error>;

    async fn quants(
        &self,
        pool: &PgPool,
        warehouse_location_path: &str,
        scoped_products: Option<&[i32]>,
        decimal_precision: u32,
        raw_quants: &mut HashMap<ProductId, Quant>,
    ) -> Result<(), sqlx::Error>;

    async fn warehouse(&self, pool: &PgPool, id: i32) -> Result<Warehouse, sqlx::Error>;
}

#[derive(Debug)]
pub enum BuildAdapterError {
    UnsupportedMajor(u16),
    Sql(sqlx::Error),
}

impl fmt::Display for BuildAdapterError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnsupportedMajor(major) => write!(
                f,
                "unsupported Odoo major version {} (only 15 is currently implemented)",
                major
            ),
            Self::Sql(err) => write!(f, "failed to initialize Odoo adapter: {err}"),
        }
    }
}

impl Error for BuildAdapterError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::UnsupportedMajor(_) => None,
            Self::Sql(err) => Some(err),
        }
    }
}

impl From<sqlx::Error> for BuildAdapterError {
    fn from(value: sqlx::Error) -> Self {
        Self::Sql(value)
    }
}

async fn table_exists(pool: &PgPool, table_name: &str) -> Result<bool, sqlx::Error> {
    let exists = sqlx::query_as::<_, (bool,)>(
        "
        SELECT EXISTS (
            SELECT FROM information_schema.tables
            WHERE
                table_schema = 'public'
                AND
                table_name = $1
        );
    ",
    )
    .bind(table_name)
    .fetch_one(pool)
    .await?;

    Ok(exists.0)
}
