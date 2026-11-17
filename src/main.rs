mod config;
mod probe;
mod worker;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Context;
use config::Config;
use mc_crypto::MasterKey;
use mc_db::PgPool;
use mc_media::LoftyParser;
use mc_runtime::StorageRegistry;
use mc_scanner::Scanner;
use mc_telemetry::{check_fn, Health, TelemetryConfig};
use tokio::net::TcpListener;
use tokio::sync::RwLock;
use tracing::{error, info};
use worker::WorkerState;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    if std::env::args().nth(1).as_deref() == Some("healthcheck") {
        return probe::run();
    }

    let config = Config::from_env()?;

    mc_telemetry::init(&TelemetryConfig::new(
        config.log_json,
        config.log_filter.clone(),
    ));

    info!(
        worker_id = %config.worker_id,
        scan_interval_seconds = config.scan_interval_seconds,
        "starting mc-worker"
    );

    let metrics_handle = if config.metrics {
        Some(mc_telemetry::install_metrics().map_err(|error| anyhow::anyhow!(error))?)
    } else {
        None
    };

    let pool = mc_db::connect_and_migrate(&config.database_url, config.database_max_connections)
        .await
        .context("connect to postgres")?;

    let master_key = config
        .master_key
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(MasterKey::from_encoded)
        .transpose()
        .context("parse MC_MASTER_KEY")?;

    let registry = StorageRegistry::load(&pool, master_key.as_ref())
        .await
        .context("load storage registry")?;
    let state = WorkerState {
        pool: pool.clone(),
        scanner: Arc::new(Scanner::new(pool.clone(), Arc::new(LoftyParser))),
        parser: Arc::new(LoftyParser),
        registry: Arc::new(RwLock::new(registry)),
        master_key,
        config: config.clone(),
    };

    let health = health_router(pool.clone(), metrics_handle);
    let health_listener = TcpListener::bind(&config.health_addr)
        .await
        .with_context(|| format!("bind health server {}", config.health_addr))?;
    tokio::spawn(async move {
        if let Err(error) = axum::serve(health_listener, health).await {
            error!(%error, "health server failed");
        }
    });

    mc_jobs::heartbeat(&pool, &config.worker_id, env!("CARGO_PKG_VERSION"))
        .await
        .context("send initial heartbeat")?;

    let heartbeat_pool = pool.clone();
    let heartbeat_worker = config.worker_id.clone();
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_secs(10));
        loop {
            tick.tick().await;
            if let Err(error) = mc_jobs::heartbeat(
                &heartbeat_pool,
                &heartbeat_worker,
                env!("CARGO_PKG_VERSION"),
            )
            .await
            {
                error!(%error, "failed to send heartbeat");
            }
        }
    });

    let (fs_tx, mut fs_rx) = tokio::sync::mpsc::unbounded_channel();
    let mut watchers = HashMap::new();
    worker::refresh_watchers(&state, &mut watchers, &fs_tx).await;

    let mut job_tick = tokio::time::interval(Duration::from_secs(2));
    let mut scan_tick = tokio::time::interval(Duration::from_secs(config.scan_interval_seconds));
    let mut watch_tick = tokio::time::interval(Duration::from_secs(30));
    let mut last_fs_scan: Option<Instant> = None;
    let mut shutdown = Box::pin(shutdown_signal());

    loop {
        tokio::select! {
            _ = job_tick.tick() => {
                worker::process_jobs(&state).await;
            }
            _ = scan_tick.tick() => {
                worker::reload_and_scan(&state).await;
                worker::refresh_watchers(&state, &mut watchers, &fs_tx).await;
            }
            _ = watch_tick.tick() => {
                worker::refresh_watchers(&state, &mut watchers, &fs_tx).await;
            }
            Some(()) = fs_rx.recv() => {
                tokio::time::sleep(Duration::from_secs(2)).await;
                while fs_rx.try_recv().is_ok() {}

                if last_fs_scan.is_none_or(|last| last.elapsed() >= Duration::from_secs(10)) {
                    worker::reload_and_scan(&state).await;
                    last_fs_scan = Some(Instant::now());
                }
            }
            _ = &mut shutdown => {
                info!("shutdown signal received");
                break;
            }
        }
    }

    Ok(())
}

fn health_router(pool: PgPool, metrics: Option<mc_telemetry::PrometheusHandle>) -> axum::Router {
    let health = Health::new().with_check(check_fn("postgres", move || {
        let pool = pool.clone();
        async move { mc_db::ping(&pool).await.map_err(|error| error.to_string()) }
    }));

    let mut router = health.router();
    if let Some(handle) = metrics {
        router = router.merge(mc_telemetry::metrics_router(handle));
    }
    router
}

async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c().await.ok();
    };

    #[cfg(unix)]
    let terminate = async {
        if let Ok(mut signal) =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        {
            signal.recv().await;
        }
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        () = ctrl_c => {}
        () = terminate => {}
    }
}
