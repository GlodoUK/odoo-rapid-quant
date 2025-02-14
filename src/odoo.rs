use std::fmt;

use sqlx::PgPool;

use crate::dialect::{BuildAdapterError, OdooAdapter, v15};

#[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum OdooVersion {
    V15,
    V16,
    V17,
    V18,
    V19,
    Other(u16),
}

impl OdooVersion {
    fn parse_latest_version(latest_version: &str) -> Result<OdooVersion, DetectOdooVersionError> {
        let major = latest_version
            .split('.')
            .next()
            .ok_or_else(|| DetectOdooVersionError::InvalidBaseVersion(latest_version.to_owned()))?
            .parse::<u16>()
            .map_err(|_| DetectOdooVersionError::InvalidBaseVersion(latest_version.to_owned()))?;

        Ok(OdooVersion::from_u16(major))
    }

    pub async fn detect_from_database(
        pool: &PgPool,
    ) -> Result<OdooVersion, DetectOdooVersionError> {
        let row = sqlx::query_as::<_, (Option<String>,)>(
            "
            SELECT latest_version
            FROM ir_module_module
            WHERE name = 'base'
        ",
        )
        .fetch_optional(pool)
        .await?;

        let latest_version = match row {
            Some((Some(version),)) => version,
            Some((None,)) | None => return Err(DetectOdooVersionError::MissingBaseVersion),
        };

        Self::parse_latest_version(&latest_version)
    }

    pub fn from_u16(major: u16) -> Self {
        match major {
            15 => Self::V15,
            16 => Self::V16,
            17 => Self::V17,
            18 => Self::V18,
            19 => Self::V19,
            _ => Self::Other(major),
        }
    }

    pub fn as_u16(self) -> u16 {
        match self {
            Self::V15 => 15,
            Self::V16 => 16,
            Self::V17 => 17,
            Self::V18 => 18,
            Self::V19 => 19,
            Self::Other(major) => major,
        }
    }

    pub async fn dialect(self, pool: &PgPool) -> Result<Box<dyn OdooAdapter>, BuildAdapterError> {
        match self {
            OdooVersion::V15 => {
                let adapter = v15::Adapter::new(pool).await?;
                Ok(Box::new(adapter))
            }
            _ => Err(BuildAdapterError::UnsupportedMajor(self.as_u16())),
        }
    }
}

impl fmt::Display for OdooVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_u16())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum DetectOdooVersionError {
    #[error("database error while detecting Odoo version: {0}")]
    Sql(#[from] sqlx::Error),
    #[error("could not find base module version in ir_module_module")]
    MissingBaseVersion,
    #[error("could not parse Odoo major version from '{0}'")]
    InvalidBaseVersion(String),
}

#[cfg(test)]
mod tests {
    use super::OdooVersion;

    #[test]
    fn parses_v19_version_shapes() {
        for version in ["19.0", "19.0.1", "19.1", "19.0.1.0.1"] {
            let parsed = OdooVersion::parse_latest_version(version)
                .expect("version should parse as a valid Odoo major");
            assert_eq!(parsed, OdooVersion::V19, "failed parsing {version}");
        }
    }
}
