//! Database driver configuration, parsed from `DB_DRIVER` and the
//! driver-specific env vars. Env-var names match upstream `lib/schemas.ts`
//! verbatim.

use std::path::PathBuf;

use super::env::{optional, required, required_u16};
use super::error::ConfigError;
use super::secret::Secret;

/// Database driver configuration.
#[derive(Debug, Clone)]
pub enum DbConfig {
    /// Local `SQLite` file: `DB_DRIVER=sqlite`, `DB_SQLITE_PATH`.
    Sqlite { path: PathBuf },
    /// `PostgreSQL`: either a connection URL or the 5-tuple, never both.
    Postgres(PostgresConfig),
    /// `MySQL`: either a connection URL or the 5-tuple, never both.
    Mysql(MysqlConfig),
}

/// Two-form `PostgreSQL` connection config.
///
/// Mirrors upstream's XOR: `DB_POSTGRES_URL` **or** the 5-var tuple, never
/// both. The `ConfigError::PostgresConflict` variant is produced when both
/// forms are present.
#[derive(Debug, Clone)]
pub enum PostgresConfig {
    Url(Secret),
    Parts {
        host: String,
        port: u16,
        user: String,
        password: Secret,
        database: String,
    },
}

/// Two-form `MySQL` connection config.
///
/// Same XOR shape as [`PostgresConfig`]: `DB_MYSQL_URL` **or** the
/// 5-var tuple, never both. The URL form is a port-side extension;
/// upstream only supports the 5-var shape. Operators preferring a
/// single env var (`mysql://user:pw@host:3306/db`) get the same
/// ergonomics they have for postgres.
#[derive(Debug, Clone)]
pub enum MysqlConfig {
    Url(Secret),
    Parts {
        host: String,
        port: u16,
        user: String,
        password: Secret,
        database: String,
    },
}

impl DbConfig {
    /// Dispatches on `DB_DRIVER`.
    ///
    /// # Errors
    /// [`ConfigError::Missing`] when a required var is unset,
    /// [`ConfigError::UnknownDbDriver`] on an unrecognised driver name,
    /// [`ConfigError::PostgresConflict`] when both postgres forms are set,
    /// [`ConfigError::Invalid`] on a malformed numeric field.
    pub fn from_env() -> Result<Self, ConfigError> {
        let driver = required("DB_DRIVER")?;
        match driver.as_str() {
            "sqlite" => parse_sqlite(),
            "postgres" => parse_postgres(),
            "mysql" => parse_mysql(),
            other => Err(ConfigError::UnknownDbDriver(other.to_string())),
        }
    }
}

fn parse_sqlite() -> Result<DbConfig, ConfigError> {
    Ok(DbConfig::Sqlite {
        path: PathBuf::from(required("DB_SQLITE_PATH")?),
    })
}

fn parse_postgres() -> Result<DbConfig, ConfigError> {
    let url = optional("DB_POSTGRES_URL");
    let parts_set = [
        "DB_POSTGRES_HOST",
        "DB_POSTGRES_PORT",
        "DB_POSTGRES_USER",
        "DB_POSTGRES_PASSWORD",
        "DB_POSTGRES_DATABASE",
    ]
    .iter()
    .any(|v| optional(v).is_some());

    match (url, parts_set) {
        (Some(_), true) => Err(ConfigError::PostgresConflict),
        (Some(u), false) => Ok(DbConfig::Postgres(PostgresConfig::Url(Secret::new(u)))),
        (None, true) => Ok(DbConfig::Postgres(PostgresConfig::Parts {
            host: required("DB_POSTGRES_HOST")?,
            port: required_u16("DB_POSTGRES_PORT")?,
            user: required("DB_POSTGRES_USER")?,
            password: Secret::new(required("DB_POSTGRES_PASSWORD")?),
            database: required("DB_POSTGRES_DATABASE")?,
        })),
        (None, false) => Err(ConfigError::Missing {
            var: "DB_POSTGRES_URL",
        }),
    }
}

fn parse_mysql() -> Result<DbConfig, ConfigError> {
    let url = optional("DB_MYSQL_URL");
    let parts_set = [
        "DB_MYSQL_HOST",
        "DB_MYSQL_PORT",
        "DB_MYSQL_USER",
        "DB_MYSQL_PASSWORD",
        "DB_MYSQL_DATABASE",
    ]
    .iter()
    .any(|v| optional(v).is_some());

    match (url, parts_set) {
        (Some(_), true) => Err(ConfigError::MysqlConflict),
        (Some(u), false) => Ok(DbConfig::Mysql(MysqlConfig::Url(Secret::new(u)))),
        (None, true) => Ok(DbConfig::Mysql(MysqlConfig::Parts {
            host: required("DB_MYSQL_HOST")?,
            port: required_u16("DB_MYSQL_PORT")?,
            user: required("DB_MYSQL_USER")?,
            password: Secret::new(required("DB_MYSQL_PASSWORD")?),
            database: required("DB_MYSQL_DATABASE")?,
        })),
        (None, false) => Err(ConfigError::Missing {
            var: "DB_MYSQL_URL",
        }),
    }
}

// required/optional/required_u16 imported from super::env

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::panic)]
mod tests {
    use super::*;
    use crate::config::env_test::{set, with_env};

    const ALL_DB_VARS: &[&str] = &[
        "DB_DRIVER",
        "DB_SQLITE_PATH",
        "DB_POSTGRES_URL",
        "DB_POSTGRES_HOST",
        "DB_POSTGRES_PORT",
        "DB_POSTGRES_USER",
        "DB_POSTGRES_PASSWORD",
        "DB_POSTGRES_DATABASE",
        "DB_MYSQL_URL",
        "DB_MYSQL_HOST",
        "DB_MYSQL_PORT",
        "DB_MYSQL_USER",
        "DB_MYSQL_PASSWORD",
        "DB_MYSQL_DATABASE",
    ];

    fn clear_all() -> Vec<(&'static str, Option<&'static str>)> {
        ALL_DB_VARS.iter().map(|v| (*v, None)).collect()
    }

    #[test]
    fn db_driver_required() {
        with_env(&clear_all(), || {
            let err = DbConfig::from_env().unwrap_err();
            assert!(matches!(err, ConfigError::Missing { var: "DB_DRIVER" }));
        });
    }

    #[test]
    fn db_driver_rejects_unknown() {
        let mut setup = clear_all();
        set(&mut setup, "DB_DRIVER", Some("oracle"));
        with_env(&setup, || {
            let err = DbConfig::from_env().unwrap_err();
            assert!(matches!(err, ConfigError::UnknownDbDriver(ref s) if s == "oracle"));
        });
    }

    #[test]
    fn sqlite_requires_path() {
        let mut setup = clear_all();
        set(&mut setup, "DB_DRIVER", Some("sqlite"));
        with_env(&setup, || {
            let err = DbConfig::from_env().unwrap_err();
            assert!(matches!(
                err,
                ConfigError::Missing {
                    var: "DB_SQLITE_PATH"
                }
            ));
        });
    }

    #[test]
    fn sqlite_happy_path() {
        let mut setup = clear_all();
        set(&mut setup, "DB_DRIVER", Some("sqlite"));
        set(
            &mut setup,
            "DB_SQLITE_PATH",
            Some("/var/cache/gha/cache.db"),
        );
        with_env(&setup, || {
            let cfg = DbConfig::from_env().unwrap();
            let DbConfig::Sqlite { path } = cfg else {
                panic!("expected Sqlite variant");
            };
            assert_eq!(path, PathBuf::from("/var/cache/gha/cache.db"));
        });
    }

    #[test]
    fn postgres_rejects_both_url_and_parts() {
        let mut setup = clear_all();
        set(&mut setup, "DB_DRIVER", Some("postgres"));
        set(&mut setup, "DB_POSTGRES_URL", Some("postgres://u:p@h/d"));
        set(&mut setup, "DB_POSTGRES_HOST", Some("h"));
        with_env(&setup, || {
            let err = DbConfig::from_env().unwrap_err();
            assert!(matches!(err, ConfigError::PostgresConflict));
        });
    }

    #[test]
    fn postgres_rejects_partial_parts() {
        let mut setup = clear_all();
        set(&mut setup, "DB_DRIVER", Some("postgres"));
        set(&mut setup, "DB_POSTGRES_HOST", Some("h"));
        // PORT, USER, PASSWORD, DATABASE all missing
        with_env(&setup, || {
            let err = DbConfig::from_env().unwrap_err();
            assert!(
                matches!(err, ConfigError::Missing { var } if var.starts_with("DB_POSTGRES_")),
                "expected missing-var error, got {err:?}"
            );
        });
    }

    #[test]
    fn postgres_url_happy_path() {
        let mut setup = clear_all();
        set(&mut setup, "DB_DRIVER", Some("postgres"));
        set(
            &mut setup,
            "DB_POSTGRES_URL",
            Some("postgres://user:pass@db.example:5432/app"),
        );
        with_env(&setup, || {
            let cfg = DbConfig::from_env().unwrap();
            let DbConfig::Postgres(PostgresConfig::Url(secret)) = cfg else {
                panic!("expected Postgres::Url variant");
            };
            assert_eq!(secret.expose(), "postgres://user:pass@db.example:5432/app");
        });
    }

    #[test]
    fn postgres_parts_happy_path() {
        let mut setup = clear_all();
        set(&mut setup, "DB_DRIVER", Some("postgres"));
        set(&mut setup, "DB_POSTGRES_HOST", Some("db.example"));
        set(&mut setup, "DB_POSTGRES_PORT", Some("5432"));
        set(&mut setup, "DB_POSTGRES_USER", Some("cache"));
        set(&mut setup, "DB_POSTGRES_PASSWORD", Some("topsecret"));
        set(&mut setup, "DB_POSTGRES_DATABASE", Some("gha_cache"));
        with_env(&setup, || {
            let cfg = DbConfig::from_env().unwrap();
            let DbConfig::Postgres(PostgresConfig::Parts {
                host,
                port,
                user,
                password,
                database,
            }) = cfg
            else {
                panic!("expected Postgres::Parts variant");
            };
            assert_eq!(host, "db.example");
            assert_eq!(port, 5432);
            assert_eq!(user, "cache");
            assert_eq!(password.expose(), "topsecret");
            assert_eq!(database, "gha_cache");
        });
    }

    #[test]
    fn postgres_invalid_port_rejected() {
        let mut setup = clear_all();
        set(&mut setup, "DB_DRIVER", Some("postgres"));
        set(&mut setup, "DB_POSTGRES_HOST", Some("h"));
        set(&mut setup, "DB_POSTGRES_PORT", Some("not-a-number"));
        set(&mut setup, "DB_POSTGRES_USER", Some("u"));
        set(&mut setup, "DB_POSTGRES_PASSWORD", Some("p"));
        set(&mut setup, "DB_POSTGRES_DATABASE", Some("d"));
        with_env(&setup, || {
            let err = DbConfig::from_env().unwrap_err();
            assert!(matches!(
                err,
                ConfigError::Invalid {
                    var: "DB_POSTGRES_PORT",
                    ..
                }
            ));
        });
    }

    #[test]
    fn mysql_rejects_both_url_and_parts() {
        let mut setup = clear_all();
        set(&mut setup, "DB_DRIVER", Some("mysql"));
        set(&mut setup, "DB_MYSQL_URL", Some("mysql://u:p@h/d"));
        set(&mut setup, "DB_MYSQL_HOST", Some("h"));
        with_env(&setup, || {
            let err = DbConfig::from_env().unwrap_err();
            assert!(matches!(err, ConfigError::MysqlConflict));
        });
    }

    #[test]
    fn mysql_with_neither_form_set_reports_url_missing() {
        // Mirrors `parse_postgres`: when neither form is configured, the
        // error names the URL variable since it's the simplest fix.
        let mut setup = clear_all();
        set(&mut setup, "DB_DRIVER", Some("mysql"));
        with_env(&setup, || {
            let err = DbConfig::from_env().unwrap_err();
            assert!(matches!(
                err,
                ConfigError::Missing {
                    var: "DB_MYSQL_URL"
                }
            ));
        });
    }

    #[test]
    fn mysql_url_happy_path() {
        let mut setup = clear_all();
        set(&mut setup, "DB_DRIVER", Some("mysql"));
        set(
            &mut setup,
            "DB_MYSQL_URL",
            Some("mysql://user:pass@db.example:3306/app"),
        );
        with_env(&setup, || {
            let cfg = DbConfig::from_env().unwrap();
            let DbConfig::Mysql(MysqlConfig::Url(secret)) = cfg else {
                panic!("expected Mysql::Url variant");
            };
            assert_eq!(secret.expose(), "mysql://user:pass@db.example:3306/app");
        });
    }

    #[test]
    fn mysql_parts_rejects_partial() {
        // Any single missing var among host/port/user/password/database
        // surfaces the next-needed one.
        let mut setup = clear_all();
        set(&mut setup, "DB_DRIVER", Some("mysql"));
        set(&mut setup, "DB_MYSQL_HOST", Some("h"));
        // PORT missing
        set(&mut setup, "DB_MYSQL_USER", Some("u"));
        set(&mut setup, "DB_MYSQL_PASSWORD", Some("p"));
        set(&mut setup, "DB_MYSQL_DATABASE", Some("d"));
        with_env(&setup, || {
            let err = DbConfig::from_env().unwrap_err();
            assert!(matches!(
                err,
                ConfigError::Missing {
                    var: "DB_MYSQL_PORT"
                }
            ));
        });
    }

    #[test]
    fn mysql_parts_happy_path() {
        let mut setup = clear_all();
        set(&mut setup, "DB_DRIVER", Some("mysql"));
        set(&mut setup, "DB_MYSQL_HOST", Some("db.example"));
        set(&mut setup, "DB_MYSQL_PORT", Some("3306"));
        set(&mut setup, "DB_MYSQL_USER", Some("cache"));
        set(&mut setup, "DB_MYSQL_PASSWORD", Some("hunter2"));
        set(&mut setup, "DB_MYSQL_DATABASE", Some("gha_cache"));
        with_env(&setup, || {
            let cfg = DbConfig::from_env().unwrap();
            let DbConfig::Mysql(MysqlConfig::Parts {
                host,
                port,
                user,
                password,
                database,
            }) = cfg
            else {
                panic!("expected Mysql::Parts variant");
            };
            assert_eq!(host, "db.example");
            assert_eq!(port, 3306);
            assert_eq!(user, "cache");
            assert_eq!(password.expose(), "hunter2");
            assert_eq!(database, "gha_cache");
        });
    }

    #[test]
    fn mysql_invalid_port_rejected() {
        let mut setup = clear_all();
        set(&mut setup, "DB_DRIVER", Some("mysql"));
        set(&mut setup, "DB_MYSQL_HOST", Some("h"));
        set(&mut setup, "DB_MYSQL_PORT", Some("not-a-number"));
        set(&mut setup, "DB_MYSQL_USER", Some("u"));
        set(&mut setup, "DB_MYSQL_PASSWORD", Some("p"));
        set(&mut setup, "DB_MYSQL_DATABASE", Some("d"));
        with_env(&setup, || {
            let err = DbConfig::from_env().unwrap_err();
            assert!(matches!(
                err,
                ConfigError::Invalid {
                    var: "DB_MYSQL_PORT",
                    ..
                }
            ));
        });
    }
}
