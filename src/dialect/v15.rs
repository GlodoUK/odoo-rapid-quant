use std::collections::HashMap;

use async_trait::async_trait;
use futures::TryStreamExt;
use petgraph::graphmap::DiGraphMap;
use rust_decimal::{Decimal, RoundingStrategy};
use sqlx::{PgPool, QueryBuilder};

use crate::{
    dialect::OdooAdapter,
    odoo::OdooVersion,
    product::{Product, ProductId, Quant},
};

pub struct Adapter {
    has_mrp_bom: bool,
    has_product_commingled: bool,
}

impl Adapter {
    pub async fn new(pool: &PgPool) -> Result<Self, sqlx::Error> {
        Ok(Self {
            has_mrp_bom: super::table_exists(pool, "mrp_bom").await?,
            has_product_commingled: super::table_exists(pool, "product_commingled").await?,
        })
    }
}

#[async_trait]
impl OdooAdapter for Adapter {
    fn major(&self) -> OdooVersion {
        OdooVersion::V15
    }

    async fn products(
        &self,
        pool: &PgPool,
        catalogue: &mut HashMap<ProductId, Product>,
        graph: &mut DiGraphMap<ProductId, Decimal>,
    ) -> Result<(), sqlx::Error> {
        let mut simple_query = QueryBuilder::new(
            "
            SELECT
                product_product.id,
                -log(uom_uom.rounding)::int
            FROM product_product
            INNER JOIN product_template ON product_product.product_tmpl_id = product_template.id
            INNER JOIN uom_uom ON uom_uom.id = product_template.uom_id
            WHERE
                product_product.active is true
                AND product_template.type = 'product'
                AND product_template.active is true
        ",
        );

        if self.has_mrp_bom {
            let _ = simple_query.push(
                " AND NOT EXISTS (
                    SELECT 1
                    FROM mrp_bom
                    WHERE
                        active is true
                        AND (
                            (
                                product_tmpl_id = product_template.id and product_id IS NULL
                            ) OR product_id = product_product.id
                        )
                )",
            );
        }

        if self.has_product_commingled {
            let _ =
                simple_query.push(" AND COALESCE(product_product.commingled_ok, false) is false");
        }

        let mut simple_stream = simple_query
            .build_query_as::<(ProductId, i32)>()
            .fetch(pool);

        while let Some((product_id, dp)) = simple_stream.try_next().await? {
            let _ = catalogue.insert(product_id, Product::Simple(dp as u32));
            let _ = graph.add_node(product_id);
        }

        if self.has_product_commingled {
            tracing::debug!("Collecting commingled products");
            let mut commingled_query = QueryBuilder::new(
                "
                SELECT
                    product_product.id,
                    -log(uom_uom.rounding)::int
                FROM product_product
                INNER JOIN product_template ON product_product.product_tmpl_id = product_template.id
                INNER JOIN uom_uom ON uom_uom.id = product_template.uom_id
                WHERE
                    product_product.active is true
                    AND product_template.active is true
                    AND product_product.commingled_ok is true
            ",
            );

            let mut stream = commingled_query
                .build_query_as::<(ProductId, i32)>()
                .fetch(pool);
            while let Some((product_id, dp)) = stream.try_next().await? {
                let _ = catalogue.insert(product_id, Product::Commingled(dp as u32));
                let _ = graph.add_node(product_id);
            }
        }

        if self.has_mrp_bom {
            tracing::debug!("Collecting BoMs");
            let mut bom_query = QueryBuilder::new(
                "
                SELECT
                    DISTINCT ON (product_product.id)
                    product_product.id,
                    mrp_bom.type,
                    round(
                        mrp_bom.product_qty / mrp_uom.factor * product_uom.factor
                        -log(product_uom.rounding)::int
                    ) AS product_qty,
                    -log(product_uom.rounding)::int
                FROM product_product
                INNER JOIN product_template ON product_product.product_tmpl_id = product_template.id
                INNER JOIN uom_uom AS product_uom ON product_uom.id = product_template.uom_id
                INNER JOIN mrp_bom ON (mrp_bom.product_tmpl_id = product_template.id AND mrp_bom.product_id IS NULL) OR mrp_bom.product_id = product_product.id
                INNER JOIN uom_uom AS mrp_uom ON mrp_uom.id = mrp_bom.product_uom_id
                WHERE
                    product_product.active is true
                    AND product_template.active is true
                    AND product_template.type = 'product'
                    AND mrp_bom.active is true
                    AND mrp_bom.type in ('normal', 'phantom')
            ",
            );

            if self.has_product_commingled {
                let _ =
                    bom_query.push(" AND COALESCE(product_product.commingled_ok, false) is false");
            }

            let _ = bom_query.push(" ORDER BY product_product.id, mrp_bom.sequence ASC");

            let mut stream = bom_query
                .build_query_as::<(ProductId, String, Decimal, i32)>()
                .fetch(pool);

            while let Some((product_id, bom_type, quantity, dp)) = stream.try_next().await? {
                let product = match bom_type.as_str() {
                    "phantom" => Product::MrpPhantom(quantity, dp as u32),
                    "normal" => Product::MrpNormal(quantity, dp as u32),
                    _ => unreachable!("Unhandled BoM type"),
                };

                let _ = catalogue.insert(product_id, product);
                let _ = graph.add_node(product_id);
            }
        }

        Ok(())
    }

    async fn relations(
        &self,
        pool: &PgPool,
        graph: &mut DiGraphMap<ProductId, Decimal>,
    ) -> Result<(), sqlx::Error> {
        tracing::debug!("Building graph edges");

        if self.has_mrp_bom {
            tracing::debug!("Fetching MRP edges");
            let mut mrp_edges_query = QueryBuilder::new(
                "
                select
                  mrp_bom.product_id as parent_product_id,
                  mrp_bom_line.product_id as child_product_id,
                  round(
                      COALESCE(mrp_bom_line.product_qty, 1) / line_uom.factor * line_product_uom.factor,
                     -log(line_product_uom.rounding)::int
                  ) as child_qty
                from mrp_bom_line
                inner join mrp_bom on mrp_bom.id = mrp_bom_line.bom_id
                inner join product_template on product_template.id = mrp_bom.product_tmpl_id
                inner join product_product on product_product.id = mrp_bom.product_id
                inner join product_product as line_product_product on line_product_product.id = mrp_bom_line.product_id
                inner join product_template as line_product_template on line_product_template.id = line_product_product.product_tmpl_id
                inner join uom_uom as line_uom on line_uom.id = mrp_bom_line.product_uom_id
                inner join uom_uom as line_product_uom on line_product_uom.id = line_product_template.uom_id
                where
                  product_template.type = 'product'
                  AND
                  product_template.active is true
                  AND
                  product_product.active is true
                  AND
                  mrp_bom.active is true
                  AND
                  line_product_product.active is true
                  AND
                  line_product_template.type = 'product'
                  AND line_product_template.active is true;
            ",
            );

            let mut stream = mrp_edges_query
                .build_query_as::<(ProductId, ProductId, Decimal)>()
                .fetch(pool);

            while let Some((parent, child, child_qty)) = stream.try_next().await? {
                if graph.contains_node(parent) && graph.contains_node(child) {
                    let _ = graph.add_edge(child, parent, child_qty);
                }
            }
        }

        if self.has_product_commingled {
            tracing::debug!("Fetching commingled edges");
            let mut commingled_edges_query = QueryBuilder::new(
                "
                select
                  parent_product_id,
                  product_id as child_product_id
                from product_commingled
                inner join product_product on product_product.id = product_commingled.parent_product_id
                inner join product_template on product_template.id = product_product.product_tmpl_id
                inner join product_product as child_product_product on child_product_product.id = product_commingled.product_id
                inner join product_template as child_product_template on child_product_template.id = child_product_product.product_tmpl_id
                where
                  product_product.active is true
                  and product_template.type = 'product'
                  and child_product_product.active is true
                  and child_product_template.type = 'product'
                  and child_product_template.active is true;
            ",
            );

            let mut stream = commingled_edges_query
                .build_query_as::<(ProductId, ProductId)>()
                .fetch(pool);

            while let Some((parent, child)) = stream.try_next().await? {
                if graph.contains_node(parent) && graph.contains_node(child) {
                    let _ = graph.add_edge(child, parent, Decimal::ONE);
                }
            }
        }

        Ok(())
    }

    async fn quants(
        &self,
        pool: &PgPool,
        warehouse_location_path: &str,
        scoped_products: Option<&[i32]>,
        decimal_precision: u32,
        raw_quants: &mut HashMap<ProductId, Quant>,
    ) -> Result<(), sqlx::Error> {
        tracing::debug!("Collecting raw quants");
        raw_quants.clear();

        let mut query = sqlx::QueryBuilder::new(
            "
            SELECT
                stock_quant.product_id,
                SUM(COALESCE(stock_quant.quantity, 0)) as quantity,
                SUM(COALESCE(stock_quant.reserved_quantity, 0)) as reserved
            FROM stock_quant
            INNER JOIN stock_location ON stock_location.id = stock_quant.location_id
            WHERE
                stock_location.parent_path like
        ",
        );

        let _ = query.push_bind(warehouse_location_path);

        if let Some(product_ids) = scoped_products {
            if product_ids.is_empty() {
                return Ok(());
            }

            let _ = query.push(" AND stock_quant.product_id = ANY(");
            let _ = query.push_bind(product_ids);
            let _ = query.push(")");
        }

        let _ = query.push(" GROUP BY stock_quant.product_id");

        let mut stream = query
            .build_query_as::<(ProductId, Decimal, Decimal)>()
            .fetch(pool);

        while let Some((product_id, quantity, reserved)) = stream.try_next().await? {
            let _ = raw_quants.insert(
                product_id,
                Quant {
                    quantity: quantity
                        .round_dp_with_strategy(decimal_precision, RoundingStrategy::ToZero),
                    reserved: reserved
                        .round_dp_with_strategy(decimal_precision, RoundingStrategy::ToZero),
                    ..Default::default()
                },
            );
        }

        let mut moves_in_query = QueryBuilder::new(
            "
            SELECT
                product_id, SUM(product_qty)
            FROM stock_move
            INNER JOIN stock_location ON stock_location.id = stock_move.location_dest_id
            WHERE
                stock_move.state in ('waiting', 'confirmed', 'assigned', 'partially_available')
                AND stock_location.parent_path like
        ",
        );

        let _ = moves_in_query.push_bind(warehouse_location_path);

        if let Some(product_ids) = scoped_products {
            if product_ids.is_empty() {
                return Ok(());
            }

            let _ = moves_in_query.push(" AND stock_move.product_id = ANY(");
            let _ = moves_in_query.push_bind(product_ids);
            let _ = moves_in_query.push(")");
        }

        let _ = moves_in_query.push(" GROUP BY product_id");

        let mut stream = moves_in_query
            .build_query_as::<(ProductId, Decimal)>()
            .fetch(pool);

        while let Some((product_id, quantity)) = stream.try_next().await? {
            let entry = raw_quants.entry(product_id).or_default();
            entry.incoming = quantity;
        }

        let mut moves_out_query = QueryBuilder::new(
            "
            SELECT
                product_id, SUM(product_qty)
            FROM stock_move
            INNER JOIN stock_location ON stock_location.id = stock_move.location_id
            WHERE
                stock_move.state in ('waiting', 'confirmed', 'assigned', 'partially_available')
                AND stock_location.parent_path like
        ",
        );

        let _ = moves_out_query.push_bind(warehouse_location_path);

        if let Some(product_ids) = scoped_products {
            if product_ids.is_empty() {
                return Ok(());
            }

            let _ = moves_out_query.push(" AND stock_move.product_id = ANY(");
            let _ = moves_out_query.push_bind(product_ids);
            let _ = moves_out_query.push(")");
        }

        let _ = moves_out_query.push(" GROUP BY product_id");

        let mut stream = moves_out_query
            .build_query_as::<(ProductId, Decimal)>()
            .fetch(pool);

        while let Some((product_id, quantity)) = stream.try_next().await? {
            let entry = raw_quants.entry(product_id).or_default();
            entry.outgoing = quantity;
        }

        Ok(())
    }
}
