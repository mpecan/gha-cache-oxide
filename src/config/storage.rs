//! Storage driver configuration, parsed from `STORAGE_DRIVER` and the
//! driver-specific env vars. Env-var names match upstream `lib/schemas.ts`
//! verbatim.

use std::path::PathBuf;

use url::Url;

use super::env::{optional, optional_url, required};
use super::error::ConfigError;
use super::secret::Secret;

/// Storage driver configuration. Parsed from `STORAGE_DRIVER` plus the
/// variant-specific env vars below.
#[derive(Debug, Clone)]
pub enum StorageConfig {
    /// Local filesystem: `STORAGE_DRIVER=filesystem`, `STORAGE_FILESYSTEM_PATH`.
    Filesystem { path: PathBuf },
    /// S3-compatible: `STORAGE_DRIVER=s3`, `STORAGE_S3_BUCKET`, `AWS_*`.
    S3 {
        bucket: String,
        region: String,
        endpoint_url: Option<Url>,
        access_key_id: Option<String>,
        secret_access_key: Option<Secret>,
    },
    /// Google Cloud Storage: `STORAGE_DRIVER=gcs`, `STORAGE_GCS_*`.
    Gcs {
        bucket: String,
        service_account_key: Option<PathBuf>,
        endpoint: Option<Url>,
    },
}

impl StorageConfig {
    /// Dispatches on `STORAGE_DRIVER` and parses the variant-specific vars.
    ///
    /// # Errors
    /// Returns [`ConfigError::Missing`] if `STORAGE_DRIVER` or a required
    /// variant-specific var is unset, [`ConfigError::UnknownStorageDriver`]
    /// on an unrecognised driver name, and [`ConfigError::InvalidUrl`] on a
    /// malformed URL.
    pub fn from_env() -> Result<Self, ConfigError> {
        let driver = required("STORAGE_DRIVER")?;
        match driver.as_str() {
            "filesystem" => parse_filesystem(),
            "s3" => parse_s3(),
            "gcs" => parse_gcs(),
            other => Err(ConfigError::UnknownStorageDriver(other.to_string())),
        }
    }
}

fn parse_filesystem() -> Result<StorageConfig, ConfigError> {
    Ok(StorageConfig::Filesystem {
        path: PathBuf::from(required("STORAGE_FILESYSTEM_PATH")?),
    })
}

fn parse_s3() -> Result<StorageConfig, ConfigError> {
    Ok(StorageConfig::S3 {
        bucket: required("STORAGE_S3_BUCKET")?,
        region: optional("AWS_REGION").unwrap_or_else(|| "us-east-1".to_string()),
        endpoint_url: optional_url("AWS_ENDPOINT_URL")?,
        access_key_id: optional("AWS_ACCESS_KEY_ID"),
        secret_access_key: optional("AWS_SECRET_ACCESS_KEY").map(Secret::new),
    })
}

fn parse_gcs() -> Result<StorageConfig, ConfigError> {
    Ok(StorageConfig::Gcs {
        bucket: required("STORAGE_GCS_BUCKET")?,
        service_account_key: optional("STORAGE_GCS_SERVICE_ACCOUNT_KEY").map(PathBuf::from),
        endpoint: optional_url("STORAGE_GCS_ENDPOINT")?,
    })
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::panic)]
mod tests {
    use super::*;
    use crate::config::env_test::{set, with_env};

    const ALL_STORAGE_VARS: &[&str] = &[
        "STORAGE_DRIVER",
        "STORAGE_FILESYSTEM_PATH",
        "STORAGE_S3_BUCKET",
        "AWS_REGION",
        "AWS_ENDPOINT_URL",
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
        "STORAGE_GCS_BUCKET",
        "STORAGE_GCS_SERVICE_ACCOUNT_KEY",
        "STORAGE_GCS_ENDPOINT",
    ];

    fn clear_all() -> Vec<(&'static str, Option<&'static str>)> {
        ALL_STORAGE_VARS.iter().map(|v| (*v, None)).collect()
    }

    #[test]
    fn storage_driver_required() {
        with_env(&clear_all(), || {
            let err = StorageConfig::from_env().unwrap_err();
            assert!(matches!(
                err,
                ConfigError::Missing {
                    var: "STORAGE_DRIVER"
                }
            ));
        });
    }

    #[test]
    fn storage_driver_rejects_unknown() {
        let mut setup = clear_all();
        set(&mut setup, "STORAGE_DRIVER", Some("azure"));
        with_env(&setup, || {
            let err = StorageConfig::from_env().unwrap_err();
            assert!(matches!(err, ConfigError::UnknownStorageDriver(ref s) if s == "azure"));
        });
    }

    #[test]
    fn filesystem_requires_path() {
        let mut setup = clear_all();
        set(&mut setup, "STORAGE_DRIVER", Some("filesystem"));
        with_env(&setup, || {
            let err = StorageConfig::from_env().unwrap_err();
            assert!(matches!(
                err,
                ConfigError::Missing {
                    var: "STORAGE_FILESYSTEM_PATH"
                }
            ));
        });
    }

    #[test]
    fn filesystem_rejects_empty_path() {
        // Post-remediation: empty string is treated as missing, not as
        // "set to the empty path".
        let mut setup = clear_all();
        set(&mut setup, "STORAGE_DRIVER", Some("filesystem"));
        set(&mut setup, "STORAGE_FILESYSTEM_PATH", Some(""));
        with_env(&setup, || {
            let err = StorageConfig::from_env().unwrap_err();
            assert!(matches!(
                err,
                ConfigError::Missing {
                    var: "STORAGE_FILESYSTEM_PATH"
                }
            ));
        });
    }

    #[test]
    fn filesystem_happy_path() {
        let mut setup = clear_all();
        set(&mut setup, "STORAGE_DRIVER", Some("filesystem"));
        set(
            &mut setup,
            "STORAGE_FILESYSTEM_PATH",
            Some("/var/cache/gha"),
        );
        with_env(&setup, || {
            let cfg = StorageConfig::from_env().unwrap();
            let StorageConfig::Filesystem { path } = cfg else {
                panic!("expected Filesystem variant");
            };
            assert_eq!(path, PathBuf::from("/var/cache/gha"));
        });
    }

    #[test]
    fn s3_requires_bucket() {
        let mut setup = clear_all();
        set(&mut setup, "STORAGE_DRIVER", Some("s3"));
        with_env(&setup, || {
            let err = StorageConfig::from_env().unwrap_err();
            assert!(matches!(
                err,
                ConfigError::Missing {
                    var: "STORAGE_S3_BUCKET"
                }
            ));
        });
    }

    #[test]
    fn s3_defaults_region_to_us_east_1() {
        let mut setup = clear_all();
        set(&mut setup, "STORAGE_DRIVER", Some("s3"));
        set(&mut setup, "STORAGE_S3_BUCKET", Some("my-bucket"));
        with_env(&setup, || {
            let cfg = StorageConfig::from_env().unwrap();
            let StorageConfig::S3 { region, .. } = cfg else {
                panic!("expected S3 variant");
            };
            assert_eq!(region, "us-east-1");
        });
    }

    #[test]
    fn s3_happy_path_with_optional_fields() {
        let mut setup = clear_all();
        set(&mut setup, "STORAGE_DRIVER", Some("s3"));
        set(&mut setup, "STORAGE_S3_BUCKET", Some("my-bucket"));
        set(&mut setup, "AWS_REGION", Some("eu-west-1"));
        set(
            &mut setup,
            "AWS_ENDPOINT_URL",
            Some("https://s3.example.com"),
        );
        set(&mut setup, "AWS_ACCESS_KEY_ID", Some("AKIA..."));
        set(&mut setup, "AWS_SECRET_ACCESS_KEY", Some("supersecret"));
        with_env(&setup, || {
            let cfg = StorageConfig::from_env().unwrap();
            let StorageConfig::S3 {
                bucket,
                region,
                endpoint_url,
                access_key_id,
                secret_access_key,
            } = cfg
            else {
                panic!("expected S3 variant");
            };
            assert_eq!(bucket, "my-bucket");
            assert_eq!(region, "eu-west-1");
            assert_eq!(
                endpoint_url.as_ref().map(Url::as_str),
                Some("https://s3.example.com/")
            );
            assert_eq!(access_key_id.as_deref(), Some("AKIA..."));
            assert_eq!(
                secret_access_key.as_ref().map(Secret::expose),
                Some("supersecret")
            );
        });
    }

    #[test]
    fn s3_endpoint_url_rejects_non_url() {
        let mut setup = clear_all();
        set(&mut setup, "STORAGE_DRIVER", Some("s3"));
        set(&mut setup, "STORAGE_S3_BUCKET", Some("my-bucket"));
        set(&mut setup, "AWS_ENDPOINT_URL", Some("not a url"));
        with_env(&setup, || {
            let err = StorageConfig::from_env().unwrap_err();
            assert!(matches!(
                err,
                ConfigError::InvalidUrl {
                    var: "AWS_ENDPOINT_URL",
                    ..
                }
            ));
        });
    }

    #[test]
    fn gcs_requires_bucket() {
        let mut setup = clear_all();
        set(&mut setup, "STORAGE_DRIVER", Some("gcs"));
        with_env(&setup, || {
            let err = StorageConfig::from_env().unwrap_err();
            assert!(matches!(
                err,
                ConfigError::Missing {
                    var: "STORAGE_GCS_BUCKET"
                }
            ));
        });
    }

    #[test]
    fn gcs_happy_path() {
        let mut setup = clear_all();
        set(&mut setup, "STORAGE_DRIVER", Some("gcs"));
        set(&mut setup, "STORAGE_GCS_BUCKET", Some("my-gcs-bucket"));
        set(
            &mut setup,
            "STORAGE_GCS_SERVICE_ACCOUNT_KEY",
            Some("/etc/sa.json"),
        );
        set(
            &mut setup,
            "STORAGE_GCS_ENDPOINT",
            Some("https://storage.googleapis.com"),
        );
        with_env(&setup, || {
            let cfg = StorageConfig::from_env().unwrap();
            let StorageConfig::Gcs {
                bucket,
                service_account_key,
                endpoint,
            } = cfg
            else {
                panic!("expected Gcs variant");
            };
            assert_eq!(bucket, "my-gcs-bucket");
            assert_eq!(service_account_key, Some(PathBuf::from("/etc/sa.json")));
            assert_eq!(
                endpoint.as_ref().map(Url::as_str),
                Some("https://storage.googleapis.com/")
            );
        });
    }

    #[test]
    fn gcs_endpoint_rejects_non_url() {
        let mut setup = clear_all();
        set(&mut setup, "STORAGE_DRIVER", Some("gcs"));
        set(&mut setup, "STORAGE_GCS_BUCKET", Some("my-gcs-bucket"));
        set(&mut setup, "STORAGE_GCS_ENDPOINT", Some(":::"));
        with_env(&setup, || {
            let err = StorageConfig::from_env().unwrap_err();
            assert!(matches!(
                err,
                ConfigError::InvalidUrl {
                    var: "STORAGE_GCS_ENDPOINT",
                    ..
                }
            ));
        });
    }
}
