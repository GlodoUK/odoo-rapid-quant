use regex::Regex;

const SUPPORTED_SINK_PLACEHOLDERS: &str = "{product_id}, {warehouse_id}, {quantity}, {reserved}, {incoming}, {outgoing}, {buildable}, {free_immediately}, {virtual_available}";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SinkPlaceholder {
    ProductId,
    WarehouseId,
    Quantity,
    Reserved,
    Incoming,
    Outgoing,
    Buildable,
    FreeImmediately,
    VirtualAvailable,
}

impl SinkPlaceholder {
    pub fn parse(name: &str) -> Option<Self> {
        match name {
            "product_id" => Some(Self::ProductId),
            "warehouse_id" => Some(Self::WarehouseId),
            "quantity" => Some(Self::Quantity),
            "reserved" => Some(Self::Reserved),
            "incoming" => Some(Self::Incoming),
            "outgoing" => Some(Self::Outgoing),
            "buildable" => Some(Self::Buildable),
            "free_immediately" => Some(Self::FreeImmediately),
            "virtual_available" => Some(Self::VirtualAvailable),
            _ => None,
        }
    }
}

#[derive(Clone, Debug)]
pub struct SinkStmtTemplate {
    pub sql: String,
    pub placeholders: Vec<SinkPlaceholder>,
}

impl SinkStmtTemplate {
    pub fn parse(input: &str) -> Result<Self, SinkStmtTemplateError> {
        let placeholder_regex = Regex::new(r"\{([^}]*)\}").expect("placeholder regex must compile");

        let mut sql = String::with_capacity(input.len());
        let mut placeholders = Vec::new();
        let mut last_match_end = 0;

        for captures in placeholder_regex.captures_iter(input) {
            let full_match = captures.get(0).expect("capture group 0 is always present");
            let name = captures
                .get(1)
                .expect("capture group 1 is always present")
                .as_str()
                .trim();

            sql.push_str(&input[last_match_end..full_match.start()]);

            if name.is_empty() {
                return Err(SinkStmtTemplateError::EmptyPlaceholder);
            }

            let placeholder = SinkPlaceholder::parse(name)
                .ok_or_else(|| SinkStmtTemplateError::UnknownPlaceholder(name.to_string()))?;

            placeholders.push(placeholder);
            sql.push('$');
            sql.push_str(&placeholders.len().to_string());

            last_match_end = full_match.end();
        }

        sql.push_str(&input[last_match_end..]);

        let non_placeholder = placeholder_regex.replace_all(input, "");
        if non_placeholder.contains('}') {
            return Err(SinkStmtTemplateError::UnmatchedClosingBrace);
        }
        if non_placeholder.contains('{') {
            return Err(SinkStmtTemplateError::UnclosedPlaceholder);
        }

        if placeholders.is_empty() {
            return Err(SinkStmtTemplateError::NoPlaceholders);
        }

        Ok(Self { sql, placeholders })
    }
}

impl std::str::FromStr for SinkStmtTemplate {
    type Err = SinkStmtTemplateError;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        Self::parse(input)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum SinkStmtTemplateError {
    #[error("unclosed placeholder in --sink-db-stmt")]
    UnclosedPlaceholder,
    #[error("unmatched closing brace in --sink-db-stmt")]
    UnmatchedClosingBrace,
    #[error("empty placeholder '{{}}' in --sink-db-stmt")]
    EmptyPlaceholder,
    #[error(
        "unknown placeholder '{{{0}}}' in --sink-db-stmt (supported placeholders: {SUPPORTED_SINK_PLACEHOLDERS})"
    )]
    UnknownPlaceholder(String),
    #[error("--sink-db-stmt must include at least one placeholder ({SUPPORTED_SINK_PLACEHOLDERS})")]
    NoPlaceholders,
}

#[derive(Debug, thiserror::Error)]
pub enum SinkExecutionError {
    #[error(
        "failed executing --sink-db-stmt for product_id={product_id}, warehouse_id={warehouse_id}: {source}"
    )]
    Execute {
        product_id: i32,
        warehouse_id: i32,
        source: sqlx::Error,
    },
}

#[cfg(test)]
mod tests {
    use super::{SinkPlaceholder, SinkStmtTemplate, SinkStmtTemplateError};

    #[test]
    fn parse_rewrites_placeholders_with_positional_binds() {
        let parsed = SinkStmtTemplate::parse(
            "INSERT INTO sink_rows (product_id, quantity, duplicate_id) VALUES ({product_id}, {quantity}, {product_id})",
        )
        .expect("template should parse");

        assert_eq!(
            parsed.sql,
            "INSERT INTO sink_rows (product_id, quantity, duplicate_id) VALUES ($1, $2, $3)"
        );
        assert_eq!(
            parsed.placeholders,
            vec![
                SinkPlaceholder::ProductId,
                SinkPlaceholder::Quantity,
                SinkPlaceholder::ProductId
            ]
        );
    }

    #[test]
    fn parse_accepts_whitespace_inside_placeholders() {
        let parsed = SinkStmtTemplate::parse("VALUES ({ product_id }, { quantity })")
            .expect("template should parse");

        assert_eq!(parsed.sql, "VALUES ($1, $2)");
        assert_eq!(
            parsed.placeholders,
            vec![SinkPlaceholder::ProductId, SinkPlaceholder::Quantity]
        );
    }

    #[test]
    fn parse_rejects_unknown_placeholders() {
        let err = SinkStmtTemplate::parse("SELECT {does_not_exist}")
            .expect_err("template should fail for unknown placeholder");

        assert!(matches!(
            err,
            SinkStmtTemplateError::UnknownPlaceholder(name) if name == "does_not_exist"
        ));
    }

    #[test]
    fn parse_requires_at_least_one_placeholder() {
        let err = SinkStmtTemplate::parse("SELECT 1")
            .expect_err("template without placeholders should fail");

        assert!(matches!(err, SinkStmtTemplateError::NoPlaceholders));
    }

    #[test]
    fn parse_rejects_malformed_braces() {
        let unclosed = SinkStmtTemplate::parse("VALUES ({product_id")
            .expect_err("unclosed placeholder should fail");
        assert!(matches!(
            unclosed,
            SinkStmtTemplateError::UnclosedPlaceholder
        ));

        let unmatched = SinkStmtTemplate::parse("VALUES (product_id})")
            .expect_err("unmatched closing brace should fail");
        assert!(matches!(
            unmatched,
            SinkStmtTemplateError::UnmatchedClosingBrace
        ));
    }

    #[test]
    fn parse_rejects_empty_placeholder() {
        let err =
            SinkStmtTemplate::parse("VALUES ({})").expect_err("empty placeholder should fail");

        assert!(matches!(err, SinkStmtTemplateError::EmptyPlaceholder));
    }
}
