use std::ops::Deref;

use sqlx::PgPool;

#[derive(sqlx::Type, Debug, Eq, PartialEq, PartialOrd, Ord, Hash, Clone, Copy)]
#[sqlx(transparent)]
pub struct WarehouseId(pub i32);

impl Deref for WarehouseId {
    type Target = i32;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[derive(sqlx::FromRow, Debug, Clone)]
#[allow(unused)]
pub struct Warehouse {
    pub id: WarehouseId,
    pub location_path: String,
    pub name: String,
    pub sequence: i32,
}

pub async fn one(pool: &PgPool, id: i32) -> Result<Warehouse, sqlx::Error> {
    sqlx::query_as::<_, Warehouse>(
        "
        SELECT
            stock_warehouse.id,
            stock_location.parent_path || '%' as location_path,
            stock_warehouse.name,
            stock_warehouse.sequence
        FROM stock_warehouse
        INNER JOIN stock_location ON stock_location.id = stock_warehouse.lot_stock_id
        WHERE
            stock_warehouse.id = $1
            AND
            stock_warehouse.active is true
            AND
            stock_location.active is true
            AND
            stock_location.usage = 'internal'
    ",
    )
    .bind(id)
    .fetch_one(pool)
    .await
}
