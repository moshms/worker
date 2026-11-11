use anyhow::{bail, Context};
use figment::providers::{Env, Serialized};
use figment::Figment;
use serde::{Deserialize, Serialize};

pub const DEFAULT_PATH_TEMPLATE: &str =
    "{albumartist}/{year} - {album}/{disc}-{track} {title}.{ext}";

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Config {
    pub database_url: String,
    pub database_max_connections: u32,
    pub master_key: Option<String>,
    pub health_addr: String,
    pub worker_id: String,
    pub scan_interval_seconds: u64,
    pub import_path_template: String,
    pub metrics: bool,
    pub log_json: bool,
    pub log_filter: String,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            database_url: String::new(),
            database_max_connections: 5,
            master_key: None,
            health_addr: "0.0.0.0:8081".to_owned(),
            worker_id: "worker-1".to_owned(),
            scan_interval_seconds: 300,
            import_path_template: DEFAULT_PATH_TEMPLATE.to_owned(),
            metrics: false,
            log_json: true,
            log_filter: "info".to_owned(),
        }
    }
}

impl Config {
    pub fn from_env() -> anyhow::Result<Self> {
        let mut config: Self = Figment::from(Serialized::defaults(Self::default()))
            .merge(Env::prefixed("MC_"))
            .extract()
            .context("read configuration from environment")?;

        if config.database_url.trim().is_empty() {
            bail!("MC_DATABASE_URL is required");
        }

        if config.import_path_template.trim().is_empty() {
            config.import_path_template = DEFAULT_PATH_TEMPLATE.to_owned();
        }

        config.scan_interval_seconds = config.scan_interval_seconds.max(10);
        Ok(config)
    }
}
