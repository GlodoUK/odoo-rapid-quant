use std::ops::Deref;

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
pub struct Warehouse {
    pub id: WarehouseId,
    pub location_path: String,
    pub name: String,
}
