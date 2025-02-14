use std::{
    collections::{HashMap, HashSet},
    fmt,
};

use petgraph::visit::EdgeRef;
use rust_decimal::RoundingStrategy;
use sqlx::{PgPool, types::Decimal};

use crate::dialect::OdooAdapter;
use crate::warehouse::Warehouse;

#[derive(sqlx::Type, sqlx::FromRow, Debug, Eq, PartialEq, PartialOrd, Hash, Ord, Clone, Copy)]
#[sqlx(transparent)]
pub struct ProductId(pub i32);

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Hash, Ord, Copy)]
pub enum Product {
    Simple(u32),
    MrpPhantom(Decimal, u32),
    MrpNormal(Decimal, u32),
    Commingled(u32),
}

impl Product {
    pub fn is_normal_bom(&self) -> bool {
        matches!(self, Self::MrpNormal(_, _))
    }

    pub fn is_simple(&self) -> bool {
        matches!(self, Self::Simple(_))
    }
}

#[derive(Debug, Clone)]
pub struct Availability {
    /// on-hand quantity
    pub quantity: Decimal,

    /// reserved quantity
    pub reserved: Decimal,

    /// incoming quantity
    pub incoming: Decimal,

    /// outgoing quantity
    pub outgoing: Decimal,

    /// buildable quantity
    pub buildable: Decimal,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AvailabilityOutputMode {
    ClampToZero,
    Signed,
}

impl AvailabilityOutputMode {
    pub fn from_allow_negative(allow_negative: bool) -> Self {
        if allow_negative {
            Self::Signed
        } else {
            Self::ClampToZero
        }
    }

    fn project(self, value: Decimal) -> Decimal {
        match self {
            Self::ClampToZero if value < Decimal::ZERO => Decimal::ZERO,
            Self::ClampToZero | Self::Signed => value,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct OutputAvailability {
    pub quantity: Decimal,
    pub reserved: Decimal,
    pub incoming: Decimal,
    pub outgoing: Decimal,
    pub buildable: Decimal,
    pub free_immediately: Decimal,
    pub virtual_available: Decimal,
}

impl Availability {
    pub fn free_immediately(&self) -> Decimal {
        self.quantity - self.reserved
    }

    pub fn virtual_available(&self) -> Decimal {
        self.quantity - self.outgoing + self.incoming
    }

    pub fn output(&self, mode: AvailabilityOutputMode) -> OutputAvailability {
        let free_immediately = self.free_immediately();
        let virtual_available = self.virtual_available();

        OutputAvailability {
            quantity: mode.project(self.quantity),
            reserved: mode.project(self.reserved),
            incoming: mode.project(self.incoming),
            outgoing: mode.project(self.outgoing),
            buildable: mode.project(self.buildable),
            free_immediately: mode.project(free_immediately),
            virtual_available: mode.project(virtual_available),
        }
    }
}

impl fmt::Display for OutputAvailability {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "free={}, quantity={}, reserved={}, incoming={}, outgoing={}, buildable={}, virtual_available={}",
            self.free_immediately,
            self.quantity,
            self.reserved,
            self.incoming,
            self.outgoing,
            self.buildable,
            self.virtual_available,
        )
    }
}

impl fmt::Display for Availability {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "free={}, quantity={}, reserved={}, incoming={}, outgoing={}, buildable={}, virtual_available={}",
            self.free_immediately(),
            self.quantity,
            self.reserved,
            self.incoming,
            self.outgoing,
            self.buildable,
            self.virtual_available(),
        )
    }
}

impl Default for Availability {
    fn default() -> Self {
        Self {
            quantity: Decimal::ZERO,
            reserved: Decimal::ZERO,
            incoming: Decimal::ZERO,
            outgoing: Decimal::ZERO,
            buildable: Decimal::ZERO,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Quant {
    /// on-hand quantity
    pub quantity: Decimal,

    /// reserved quantity
    pub reserved: Decimal,

    /// incoming quantity
    pub incoming: Decimal,

    /// outgoing quantity
    pub outgoing: Decimal,
}

impl Quant {
    const EMPTY: Self = Self {
        quantity: Decimal::ZERO,
        reserved: Decimal::ZERO,
        incoming: Decimal::ZERO,
        outgoing: Decimal::ZERO,
    };
}

impl Default for Quant {
    fn default() -> Self {
        Self {
            quantity: Decimal::ZERO,
            reserved: Decimal::ZERO,
            incoming: Decimal::ZERO,
            outgoing: Decimal::ZERO,
        }
    }
}

pub struct Graph {
    /// Postgres handle
    pub pool: PgPool,

    /// Odoo adapter for current Odoo version
    pub adapter: Box<dyn OdooAdapter>,

    /// Precision
    pub decimal_precision: u32,

    /// Internal graph of products
    pub graph: petgraph::graphmap::DiGraphMap<ProductId, Decimal>,

    /// catalogue
    pub catalogue: HashMap<ProductId, Product>,

    /// Warehouse
    pub warehouse: Warehouse,

    /// Cached availability for products
    pub avail: HashMap<ProductId, Availability>,

    /// Raw quants in Odoo
    pub raw_quants: HashMap<ProductId, Quant>,
}

impl Graph {
    pub async fn new(
        pool: PgPool,
        warehouse: Warehouse,
        adapter: Box<dyn OdooAdapter>,
    ) -> Result<Self, sqlx::Error> {
        let decimal_precision: u32 = Self::get_decimal_precision(&pool).await?;

        Ok(Self {
            pool,
            adapter,
            decimal_precision,
            graph: petgraph::graphmap::DiGraphMap::new(),
            raw_quants: HashMap::new(),
            avail: HashMap::new(),
            catalogue: HashMap::new(),
            warehouse,
        })
    }

    async fn get_decimal_precision(pool: &PgPool) -> Result<u32, sqlx::Error> {
        tracing::debug!("Fetching decimal precision");
        let digits = sqlx::query_as::<_, (i32,)>(
            "
            SELECT digits FROM decimal_precision WHERE name = 'Product Unit of Measure' limit 1;
        ",
        )
        .fetch_one(pool)
        .await?;

        Ok(digits.0 as u32)
    }

    fn dependency_closure(
        graph: &petgraph::graphmap::DiGraphMap<ProductId, Decimal>,
        requested_products: &[ProductId],
    ) -> HashSet<ProductId> {
        let mut closure: HashSet<ProductId> = HashSet::with_capacity(requested_products.len());
        let mut stack: Vec<ProductId> = requested_products.to_vec();

        while let Some(product) = stack.pop() {
            if !closure.insert(product) {
                continue;
            }

            if !graph.contains_node(product) {
                continue;
            }

            for dependency in graph.neighbors_directed(product, petgraph::Incoming) {
                stack.push(dependency);
            }
        }

        closure
    }

    pub async fn collect(&mut self, requested_products: &[ProductId]) -> Result<(), sqlx::Error> {
        tracing::info!("Building graph");

        self.catalogue.clear();
        self.graph.clear();

        self.adapter
            .products(&self.pool, &mut self.catalogue, &mut self.graph)
            .await?;
        self.adapter.relations(&self.pool, &mut self.graph).await?;

        let sorted_nodes = petgraph::algo::toposort(&self.graph, None).expect("Graph has cycles!");

        let scope = if requested_products.is_empty() {
            None
        } else {
            Some(Self::dependency_closure(&self.graph, requested_products))
        };

        let scoped_product_ids = scope.as_ref().map(|products| {
            let mut ids = Vec::with_capacity(products.len());
            for product in products {
                ids.push(product.0);
            }
            ids
        });

        self.adapter
            .quants(
                &self.pool,
                &self.warehouse.location_path,
                scoped_product_ids.as_deref(),
                self.decimal_precision,
                &mut self.raw_quants,
            )
            .await?;

        tracing::info!("Pre-computing stock levels");
        self.avail.clear();
        Self::compute_stock_levels(
            &self.graph,
            &self.catalogue,
            &mut self.avail,
            &self.raw_quants,
            &sorted_nodes,
            scope.as_ref(),
            self.decimal_precision,
        );
        tracing::info!("Pre-computing done");

        Ok(())
    }

    fn compute_stock_levels(
        graph: &petgraph::graphmap::DiGraphMap<ProductId, Decimal>,
        catalogue: &HashMap<ProductId, Product>,
        stock_cache: &mut HashMap<ProductId, Availability>,
        raw_quants: &HashMap<ProductId, Quant>,
        sorted_nodes: &[ProductId],
        scope: Option<&HashSet<ProductId>>,
        default_dp: u32,
    ) {
        let zero = Decimal::ZERO.round_dp_with_strategy(default_dp, RoundingStrategy::ToZero);

        // Iterate in topological order
        for product in sorted_nodes.iter().copied() {
            if let Some(scoped_products) = scope {
                if !scoped_products.contains(&product) {
                    continue;
                }
            }

            // If already in stock cache, it's a raw product; skip processing
            if stock_cache.contains_key(&product) {
                tracing::warn!(
                    product_id = product.0,
                    "Traversed product already present in stock cache"
                );
                continue;
            }

            let info = catalogue.get(&product).unwrap_or_else(|| {
                panic!(
                    "Somehow we have a product in the graph not in the catalogue?!: {:?}",
                    product
                )
            });
            if info.is_simple() {
                let mut avail = Availability::default();

                if let Some(quant) = raw_quants.get(&product) {
                    avail.quantity = quant.quantity;
                    avail.reserved = quant.reserved;

                    avail.incoming = quant.incoming;
                    avail.outgoing = quant.outgoing;
                }

                let _ = stock_cache.insert(product, avail);
                continue;
            }

            let mut quantity = Vec::new();
            let mut reserved = Vec::new();
            let mut incoming = Vec::new();
            let mut outgoing = Vec::new();
            let mut buildable = Vec::new();

            // Iterate dependencies (incoming edges)
            for edge in graph.edges_directed(product, petgraph::Incoming) {
                let (dependency, required_qty) = (edge.source(), *edge.weight());
                if let Some(dependency_stock) = stock_cache.get(&dependency) {
                    if !info.is_normal_bom() {
                        // only do this work if we need to
                        quantity.push(dependency_stock.quantity / required_qty);
                        reserved.push(dependency_stock.reserved / required_qty);

                        incoming.push(dependency_stock.incoming / required_qty);
                        outgoing.push(dependency_stock.outgoing / required_qty);
                    }

                    if info.is_normal_bom() {
                        buildable.push(
                            (dependency_stock.buildable + dependency_stock.free_immediately())
                                / required_qty,
                        );
                    }
                }
            }

            match info {
                Product::MrpPhantom(decimal, dp) => {
                    // If it has dependencies, store the calculated stock
                    let _ = stock_cache.insert(
                        product,
                        Availability {
                            quantity: (*quantity.iter().min().unwrap_or(&zero) * decimal)
                                .round_dp_with_strategy(*dp, RoundingStrategy::ToZero),
                            reserved: (*reserved.iter().min().unwrap_or(&zero) * decimal)
                                .round_dp_with_strategy(*dp, RoundingStrategy::ToZero),
                            incoming: (*incoming.iter().min().unwrap_or(&zero) * decimal)
                                .round_dp_with_strategy(*dp, RoundingStrategy::ToZero),
                            outgoing: (*outgoing.iter().min().unwrap_or(&zero) * decimal)
                                .round_dp_with_strategy(*dp, RoundingStrategy::ToZero),
                            buildable: zero,
                        },
                    );
                }
                Product::MrpNormal(decimal, dp) => {
                    let raw = if let Some(quant) = raw_quants.get(&product) {
                        quant
                    } else {
                        &Quant::EMPTY
                    };

                    // If it has dependencies, store the calculated stock
                    let _ = stock_cache.insert(
                        product,
                        Availability {
                            quantity: raw.quantity,
                            reserved: raw.reserved,
                            incoming: raw.incoming,
                            outgoing: raw.outgoing,
                            buildable: *buildable.iter().min().unwrap_or(&zero)
                                * decimal.round_dp_with_strategy(*dp, RoundingStrategy::ToZero),
                        },
                    );
                }
                Product::Commingled(dp) => {
                    let _ = stock_cache.insert(
                        product,
                        Availability {
                            quantity: (quantity.iter().fold(zero, |acc, x: &Decimal| acc + x))
                                .round_dp_with_strategy(*dp, RoundingStrategy::ToZero),
                            reserved: (reserved.iter().fold(zero, |acc, x: &Decimal| acc + x))
                                .round_dp_with_strategy(*dp, RoundingStrategy::ToZero),
                            incoming: (incoming.iter().fold(zero, |acc, x: &Decimal| acc + x))
                                .round_dp_with_strategy(*dp, RoundingStrategy::ToZero),
                            outgoing: (outgoing.iter().fold(zero, |acc, x: &Decimal| acc + x))
                                .round_dp_with_strategy(*dp, RoundingStrategy::ToZero),
                            buildable: (buildable.iter().fold(zero, |acc, x: &Decimal| acc + x))
                                .round_dp_with_strategy(*dp, RoundingStrategy::ToZero),
                        },
                    );
                }
                _ => unimplemented!(),
            }
        }
    }

    pub fn get(&self, product_id: &ProductId) -> Option<&Availability> {
        self.avail.get(product_id)
    }

    pub fn computed_products(&self) -> Vec<ProductId> {
        let mut products: Vec<ProductId> = self.avail.keys().copied().collect();
        products.sort_unstable();
        products
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};

    use petgraph::graphmap::DiGraphMap;
    use rust_decimal::Decimal;

    use super::{Availability, AvailabilityOutputMode, Graph, Product, ProductId, Quant};

    fn d(value: &str) -> Decimal {
        Decimal::from_str_exact(value).expect("test decimal must parse")
    }

    fn quant(quantity: &str, reserved: &str, incoming: &str, outgoing: &str) -> Quant {
        Quant {
            quantity: d(quantity),
            reserved: d(reserved),
            incoming: d(incoming),
            outgoing: d(outgoing),
        }
    }

    fn compute_stock_levels(
        graph: &DiGraphMap<ProductId, Decimal>,
        catalogue: &HashMap<ProductId, Product>,
        raw_quants: &HashMap<ProductId, Quant>,
        sorted_nodes: &[ProductId],
        scope: Option<&HashSet<ProductId>>,
        default_dp: u32,
    ) -> HashMap<ProductId, Availability> {
        let mut stock_cache = HashMap::new();

        Graph::compute_stock_levels(
            graph,
            catalogue,
            &mut stock_cache,
            raw_quants,
            sorted_nodes,
            scope,
            default_dp,
        );

        stock_cache
    }

    #[test]
    fn availability_calculates_free_and_virtual() {
        // Sanity check helper formulas used in output and buildable math:
        // free_immediately = quantity - reserved, virtual_available = quantity - outgoing + incoming.
        let availability = Availability {
            quantity: d("10"),
            reserved: d("2"),
            incoming: d("5"),
            outgoing: d("3"),
            buildable: d("0"),
        };

        assert_eq!(availability.free_immediately(), d("8"));
        assert_eq!(availability.virtual_available(), d("12"));
    }

    #[test]
    fn availability_allows_negative_virtual_available() {
        let availability = Availability {
            quantity: d("2"),
            reserved: d("0"),
            incoming: d("1"),
            outgoing: d("5"),
            buildable: d("0"),
        };

        assert_eq!(availability.virtual_available(), d("-2"));
    }

    #[test]
    fn output_mode_maps_allow_negative_flag() {
        assert_eq!(
            AvailabilityOutputMode::from_allow_negative(false),
            AvailabilityOutputMode::ClampToZero
        );
        assert_eq!(
            AvailabilityOutputMode::from_allow_negative(true),
            AvailabilityOutputMode::Signed
        );
    }

    #[test]
    fn output_clamps_negative_values_in_all_fields() {
        let availability = Availability {
            quantity: d("-5"),
            reserved: d("-2"),
            incoming: d("-1"),
            outgoing: d("-3"),
            buildable: d("-4"),
        };

        let output = availability.output(AvailabilityOutputMode::ClampToZero);

        assert_eq!(output.quantity, d("0"));
        assert_eq!(output.reserved, d("0"));
        assert_eq!(output.incoming, d("0"));
        assert_eq!(output.outgoing, d("0"));
        assert_eq!(output.buildable, d("0"));
        assert_eq!(output.free_immediately, d("0"));
        assert_eq!(output.virtual_available, d("0"));
    }

    #[test]
    fn output_keeps_negative_values_in_signed_mode() {
        let availability = Availability {
            quantity: d("-5"),
            reserved: d("-2"),
            incoming: d("-1"),
            outgoing: d("-3"),
            buildable: d("-4"),
        };

        let output = availability.output(AvailabilityOutputMode::Signed);

        assert_eq!(output.quantity, d("-5"));
        assert_eq!(output.reserved, d("-2"));
        assert_eq!(output.incoming, d("-1"));
        assert_eq!(output.outgoing, d("-3"));
        assert_eq!(output.buildable, d("-4"));
        assert_eq!(output.free_immediately, d("-3"));
        assert_eq!(output.virtual_available, d("-3"));
    }

    #[test]
    fn simple_product_uses_raw_quant_values() {
        // Simple products are passthrough: computed availability should match raw quant fields,
        // with buildable forced to zero.
        let simple = ProductId(1);

        let mut graph = DiGraphMap::new();
        graph.add_node(simple);

        let mut catalogue = HashMap::new();
        catalogue.insert(simple, Product::Simple(2));

        let mut raw_quants = HashMap::new();
        raw_quants.insert(simple, quant("10", "2", "3", "1"));

        let stock = compute_stock_levels(&graph, &catalogue, &raw_quants, &[simple], None, 2);
        let availability = stock.get(&simple).expect("simple product must be computed");

        assert_eq!(availability.quantity, d("10"));
        assert_eq!(availability.reserved, d("2"));
        assert_eq!(availability.incoming, d("3"));
        assert_eq!(availability.outgoing, d("1"));
        assert_eq!(availability.buildable, d("0"));
    }

    #[test]
    fn phantom_product_uses_dependency_mins_for_all_fields() {
        // Phantom BoM aggregates by dependency minimum per field (quantity/reserved/incoming/outgoing)
        // after normalizing by required_qty.
        let dep_a = ProductId(1);
        let dep_b = ProductId(2);
        let phantom = ProductId(3);

        let mut graph = DiGraphMap::new();
        graph.add_edge(dep_a, phantom, d("1"));
        graph.add_edge(dep_b, phantom, d("1"));

        let mut catalogue = HashMap::new();
        catalogue.insert(dep_a, Product::Simple(2));
        catalogue.insert(dep_b, Product::Simple(2));
        catalogue.insert(phantom, Product::MrpPhantom(d("1"), 2));

        let mut raw_quants = HashMap::new();
        raw_quants.insert(dep_a, quant("10", "4", "6", "1"));
        raw_quants.insert(dep_b, quant("8", "2", "3", "5"));

        let stock = compute_stock_levels(
            &graph,
            &catalogue,
            &raw_quants,
            &[dep_a, dep_b, phantom],
            None,
            2,
        );

        let availability = stock
            .get(&phantom)
            .expect("phantom product must be computed");

        assert_eq!(availability.quantity, d("8"));
        assert_eq!(availability.reserved, d("2"));
        assert_eq!(availability.incoming, d("3"));
        assert_eq!(availability.outgoing, d("1"));
        assert_eq!(availability.buildable, d("0"));
    }

    #[test]
    fn normal_bom_product_uses_raw_quant_and_buildable_min() {
        // Buildable is min((dep.buildable + dep.free_immediately) / required_qty) * bom_output_qty.
        // Here: dep_a = (0 + (10 - 3)) / 1 = 7, dep_b = (0 + (5 - 1)) / 1 = 4, min = 4,
        // and Product::MrpNormal(d("2"), ..) scales by 2, so buildable = 4 * 2 = 8.
        let dep_a = ProductId(1);
        let dep_b = ProductId(2);
        let normal_bom = ProductId(3);

        let mut graph = DiGraphMap::new();
        graph.add_edge(dep_a, normal_bom, d("1"));
        graph.add_edge(dep_b, normal_bom, d("1"));

        let mut catalogue = HashMap::new();
        catalogue.insert(dep_a, Product::Simple(2));
        catalogue.insert(dep_b, Product::Simple(2));
        catalogue.insert(normal_bom, Product::MrpNormal(d("2"), 2));

        let mut raw_quants = HashMap::new();
        raw_quants.insert(dep_a, quant("10", "3", "1", "0"));
        raw_quants.insert(dep_b, quant("5", "1", "2", "0"));
        raw_quants.insert(normal_bom, quant("9", "2", "4", "1"));

        let stock = compute_stock_levels(
            &graph,
            &catalogue,
            &raw_quants,
            &[dep_a, dep_b, normal_bom],
            None,
            2,
        );

        let availability = stock
            .get(&normal_bom)
            .expect("normal bom product must be computed");

        assert_eq!(availability.quantity, d("9"));
        assert_eq!(availability.reserved, d("2"));
        assert_eq!(availability.incoming, d("4"));
        assert_eq!(availability.outgoing, d("1"));
        assert_eq!(availability.buildable, d("8"));
    }

    #[test]
    fn commingled_product_sums_dependencies_with_rounding() {
        // Commingled products sum dependency contributions per field, then round to product dp.
        // Example: quantity = 1.239 + 2.455 = 3.694 -> 3.69 (dp=2, ToZero).
        let dep_a = ProductId(1);
        let dep_b = ProductId(2);
        let commingled = ProductId(3);

        let mut graph = DiGraphMap::new();
        graph.add_edge(dep_a, commingled, d("1"));
        graph.add_edge(dep_b, commingled, d("1"));

        let mut catalogue = HashMap::new();
        catalogue.insert(dep_a, Product::Simple(2));
        catalogue.insert(dep_b, Product::Simple(2));
        catalogue.insert(commingled, Product::Commingled(2));

        let mut raw_quants = HashMap::new();
        raw_quants.insert(dep_a, quant("1.239", "0.101", "0.009", "0.001"));
        raw_quants.insert(dep_b, quant("2.455", "1.208", "0.111", "0.019"));

        let stock = compute_stock_levels(
            &graph,
            &catalogue,
            &raw_quants,
            &[dep_a, dep_b, commingled],
            None,
            2,
        );

        let availability = stock
            .get(&commingled)
            .expect("commingled product must be computed");

        assert_eq!(availability.quantity, d("3.69"));
        assert_eq!(availability.reserved, d("1.30"));
        assert_eq!(availability.incoming, d("0.12"));
        assert_eq!(availability.outgoing, d("0.02"));
        assert_eq!(availability.buildable, d("0"));
    }

    #[test]
    fn scope_only_computes_requested_products() {
        // Only products present in scope are computed/cached.
        let product_a = ProductId(1);
        let product_b = ProductId(2);

        let mut graph = DiGraphMap::new();
        graph.add_node(product_a);
        graph.add_node(product_b);

        let mut catalogue = HashMap::new();
        catalogue.insert(product_a, Product::Simple(2));
        catalogue.insert(product_b, Product::Simple(2));

        let mut raw_quants = HashMap::new();
        raw_quants.insert(product_a, quant("1", "0", "0", "0"));
        raw_quants.insert(product_b, quant("2", "0", "0", "0"));

        let mut scope = HashSet::new();
        scope.insert(product_a);

        let stock = compute_stock_levels(
            &graph,
            &catalogue,
            &raw_quants,
            &[product_a, product_b],
            Some(&scope),
            2,
        );

        assert!(stock.contains_key(&product_a));
        assert!(!stock.contains_key(&product_b));
    }
}
