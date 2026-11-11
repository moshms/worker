use std::collections::HashSet;
use std::path::Path;
use std::sync::Arc;

use bytes::Bytes;
use mc_core::{CoreError, Result};
use mc_crypto::MasterKey;
use mc_db::repos::{catalog, libraries, media_files, tag_edits, uploads};
use mc_db::PgPool;
use mc_jobs::JobRecord;
use mc_media::{write_tags_to_bytes, write_tags_to_path, MediaMetadata, MediaParser, TagChanges};
use mc_runtime::StorageRegistry;
use mc_scanner::{ScanMode, Scanner};
use mc_storage::{Key, Storage};
use notify::event::ModifyKind;
use notify::{EventKind, RecursiveMode, Watcher};
use serde::Deserialize;
use tokio::sync::{mpsc, RwLock};
use tracing::{error, info, warn};
use uuid::Uuid;

use crate::config::Config;

pub struct WorkerState {
    pub pool: PgPool,
    pub scanner: Arc<Scanner>,
    pub parser: Arc<dyn MediaParser>,
    pub registry: Arc<RwLock<StorageRegistry>>,
    pub master_key: Option<MasterKey>,
    pub config: Config,
}

pub async fn spawn_watchers(
    state: &WorkerState,
    tx: mpsc::UnboundedSender<()>,
) -> Vec<notify::RecommendedWatcher> {
    let registry = state.registry.read().await.clone();
    let mut watchers = Vec::new();

    for library in registry.libraries() {
        let Some(storage) = registry.storage_for_library(library.id) else {
            continue;
        };
        let Some(root) = storage.local_path(&Key::root()) else {
            continue;
        };

        if !root.is_dir() {
            continue;
        }

        let sender = tx.clone();
        let mut watcher =
            match notify::recommended_watcher(move |result: notify::Result<notify::Event>| {
                if let Ok(event) = result {
                    if matches!(
                        event.kind,
                        EventKind::Create(_)
                            | EventKind::Remove(_)
                            | EventKind::Modify(ModifyKind::Data(_))
                            | EventKind::Modify(ModifyKind::Name(_))
                    ) {
                        let _ = sender.send(());
                    }
                }
            }) {
                Ok(watcher) => watcher,
                Err(error) => {
                    warn!(library = %library.name, %error, "failed to create filesystem watcher");
                    continue;
                }
            };

        match watcher.watch(&root, RecursiveMode::Recursive) {
            Ok(()) => {
                info!(library = %library.name, root = %root.display(), "watching library");
                watchers.push(watcher);
            }
            Err(error) => {
                warn!(library = %library.name, %error, "failed to watch library");
            }
        }
    }

    watchers
}

pub async fn process_jobs(state: &WorkerState) {
    for _ in 0..5 {
        let job = match mc_jobs::claim_one(
            &state.pool,
            &state.config.worker_id,
            &["import", "writeback"],
        )
        .await
        {
            Ok(Some(job)) => job,
            Ok(None) => break,
            Err(error) => {
                error!(%error, "failed to claim job");
                break;
            }
        };

        let result = match job.kind.as_str() {
            "import" => process_import(state, &job).await,
            "writeback" => process_writeback(state, &job).await,
            other => Err(CoreError::Validation(format!(
                "unsupported job kind: {other}"
            ))),
        };

        match result {
            Ok(()) => {
                if let Err(error) = mc_jobs::complete(&state.pool, job.id).await {
                    error!(%error, "failed to complete job");
                }
            }
            Err(error) => {
                warn!(job = %job.id, %error, "job failed");
                if let Err(fail_error) = mc_jobs::fail(&state.pool, &job, &error.to_string()).await
                {
                    error!(%fail_error, "failed to mark job failed");
                }
            }
        }
    }
}

pub async fn reload_and_scan(state: &WorkerState) {
    let registry = match StorageRegistry::load(&state.pool, state.master_key.as_ref()).await {
        Ok(registry) => registry,
        Err(error) => {
            error!(%error, "failed to reload storage registry");
            return;
        }
    };
    let libraries = registry.libraries().to_vec();
    *state.registry.write().await = registry.clone();

    for library in &libraries {
        let Some(storage) = registry.storage_for_library(library.id) else {
            warn!(library = %library.name, "library storage is unavailable");
            continue;
        };

        match state
            .scanner
            .scan_library(library, storage, ScanMode::Full)
            .await
        {
            Ok(stats) => {
                metrics::counter!("mc_worker_scans_total").increment(1);
                metrics::gauge!("mc_worker_files_seen").set(stats.files_seen as f64);
                info!(library = %library.name, ?stats, "worker scan finished")
            }
            Err(error) => {
                metrics::counter!("mc_worker_scans_total", "status" => "failed").increment(1);
                error!(library = %library.name, %error, "worker scan failed")
            }
        }
    }
}

#[derive(Debug, Deserialize)]
struct ImportPayload {
    upload_id: Option<Uuid>,
    library_id: Uuid,
    source_key: String,
}

async fn process_import(state: &WorkerState, job: &JobRecord) -> Result<()> {
    let payload: ImportPayload = serde_json::from_value(job.payload.clone())
        .map_err(|error| CoreError::Validation(format!("invalid import payload: {error}")))?;

    let library = libraries::get(&state.pool, payload.library_id)
        .await?
        .ok_or_else(|| CoreError::not_found("library", payload.library_id))?;
    let registry = StorageRegistry::load(&state.pool, state.master_key.as_ref()).await?;
    *state.registry.write().await = registry.clone();
    let storage = registry
        .storage_for_library(library.id)
        .ok_or_else(|| CoreError::Unavailable("library storage is unavailable".to_owned()))?;

    let source_key = Key::new(payload.source_key)?;
    let data = storage.read(&source_key).await?;
    let metadata = state.parser.parse(&data)?;
    let target_key = build_target_key(&state.config.import_path_template, &source_key, &metadata)?;

    if target_key != source_key {
        let target_key = unique_key(&storage, target_key, job.id).await?;
        storage.rename(&source_key, &target_key).await?;
        info!(from = %source_key, to = %target_key, "imported file");

        if let Err(error) =
            media_files::delete_by_key(&state.pool, library.id, source_key.as_str()).await
        {
            warn!(%error, "failed to delete old media file after import");
        }
    }

    if let Err(error) = state
        .scanner
        .scan_library(&library, storage, ScanMode::Full)
        .await
    {
        warn!(library = %library.name, %error, "scan after import failed");
    }

    if let Some(upload_id) = payload.upload_id {
        if let Err(error) = uploads::finish(&state.pool, upload_id, "imported", None).await {
            warn!(%error, "failed to mark upload imported");
        }
    }

    Ok(())
}

fn build_target_key(template: &str, source: &Key, metadata: &MediaMetadata) -> Result<Key> {
    let ext = Path::new(source.as_str())
        .extension()
        .and_then(|extension| extension.to_str())
        .unwrap_or("bin")
        .to_ascii_lowercase();
    let stem = Path::new(source.as_str())
        .file_stem()
        .and_then(|stem| stem.to_str())
        .unwrap_or("track");
    let title = sanitize_component(metadata.title.as_deref().and_then(clean).unwrap_or(stem));
    let artist = sanitize_component(
        metadata
            .artist
            .as_deref()
            .and_then(clean)
            .unwrap_or("Unknown Artist"),
    );
    let album_artist = sanitize_component(
        metadata
            .album_artist
            .as_deref()
            .and_then(clean)
            .unwrap_or(&artist),
    );
    let album = sanitize_component(
        metadata
            .album
            .as_deref()
            .and_then(clean)
            .unwrap_or("Unknown Album"),
    );
    let year = metadata
        .year
        .filter(|year| *year > 0)
        .map_or_else(|| "Unknown Year".to_owned(), |year| year.to_string());
    let disc = format!("{:02}", metadata.disc_no.unwrap_or(0));
    let track = format!("{:02}", metadata.track_no.unwrap_or(0));
    let ext = sanitize_component(&ext);

    let mut rendered = template.to_owned();
    for (key, value) in [
        ("albumartist", album_artist.as_str()),
        ("artist", artist.as_str()),
        ("album", album.as_str()),
        ("year", year.as_str()),
        ("disc", disc.as_str()),
        ("track", track.as_str()),
        ("title", title.as_str()),
        ("ext", ext.as_str()),
    ] {
        rendered = rendered.replace(&format!("{{{key}}}"), value);
    }

    let segments = rendered
        .split('/')
        .map(sanitize_component)
        .filter(|segment| !segment.is_empty())
        .collect::<Vec<_>>();

    let mut path = if segments.is_empty() {
        format!("{album_artist}/{album}/{title}.{ext}")
    } else {
        segments.join("/")
    };

    if !path.to_ascii_lowercase().ends_with(&format!(".{ext}")) {
        path.push('.');
        path.push_str(&ext);
    }

    Key::new(path)
}

async fn unique_key(storage: &Arc<dyn Storage>, key: Key, job_id: Uuid) -> Result<Key> {
    match storage.stat(&key).await {
        Ok(_) => {
            let suffix = job_id.to_string().chars().take(8).collect::<String>();
            let path = key.as_str();
            let ext = Path::new(path)
                .extension()
                .and_then(|extension| extension.to_str())
                .unwrap_or("");

            let new_path = if ext.is_empty() {
                format!("{path}-{suffix}")
            } else {
                let stem = path.strip_suffix(&format!(".{ext}")).unwrap_or(path);
                format!("{stem}-{suffix}.{ext}")
            };

            Key::new(new_path)
        }
        Err(CoreError::NotFound { .. }) => Ok(key),
        Err(error) => Err(error),
    }
}

fn sanitize_component(value: &str) -> String {
    let cleaned = value
        .chars()
        .map(|character| {
            if character.is_control() || character == '/' || character == '\\' {
                '_'
            } else {
                character
            }
        })
        .collect::<String>();

    let trimmed = cleaned.trim().trim_matches('.').to_owned();

    if trimmed.is_empty() {
        "Unknown".to_owned()
    } else {
        trimmed.chars().take(120).collect()
    }
}

fn clean(value: &str) -> Option<&str> {
    let value = value.trim();
    if value.is_empty() {
        None
    } else {
        Some(value)
    }
}

#[cfg(test)]
mod tests {
    use mc_media::MediaMetadata;

    use super::{build_target_key, sanitize_component};
    use mc_storage::Key;

    #[test]
    fn renders_path_template() {
        let metadata = MediaMetadata {
            title: Some("Song / One".to_owned()),
            artist: Some("Artist".to_owned()),
            album_artist: Some("Album Artist".to_owned()),
            album: Some("Album".to_owned()),
            year: Some(2024),
            track_no: Some(3),
            disc_no: Some(1),
            ..MediaMetadata::default()
        };
        let source = Key::new("uploads/user/file.mp3").unwrap();
        let key = build_target_key(
            "{albumartist}/{year} - {album}/{disc}-{track} {title}.{ext}",
            &source,
            &metadata,
        )
        .unwrap();

        assert_eq!(
            key.as_str(),
            "Album Artist/2024 - Album/01-03 Song _ One.mp3"
        );
    }

    #[test]
    fn sanitizes_components() {
        assert_eq!(sanitize_component("../evil"), "_evil");
        assert_eq!(sanitize_component(""), "Unknown");
    }
}

#[derive(Debug, Deserialize)]
struct WritebackPayload {
    tag_edit_id: Uuid,
}

async fn process_writeback(state: &WorkerState, job: &JobRecord) -> Result<()> {
    let payload: WritebackPayload = serde_json::from_value(job.payload.clone())
        .map_err(|error| CoreError::Validation(format!("invalid writeback payload: {error}")))?;
    let edit = tag_edits::get(&state.pool, payload.tag_edit_id)
        .await?
        .ok_or_else(|| CoreError::not_found("tag edit", payload.tag_edit_id))?;

    if edit.status == "done" {
        return Ok(());
    }

    let changes: TagChanges = serde_json::from_value(edit.changes.clone())
        .map_err(|error| CoreError::Validation(format!("invalid tag changes: {error}")))?;
    tag_edits::set_status(&state.pool, edit.id, "processing").await?;

    let result = apply_writeback(state, &edit, &changes).await;

    match &result {
        Ok(()) => {
            tag_edits::finish(&state.pool, edit.id, "done", None).await?;
        }
        Err(error) => {
            let _ = tag_edits::finish(
                &state.pool,
                edit.id,
                "needs_review",
                Some(&error.to_string()),
            )
            .await;
        }
    }

    result
}

async fn apply_writeback(
    state: &WorkerState,
    edit: &tag_edits::TagEditRecord,
    changes: &TagChanges,
) -> Result<()> {
    let registry = StorageRegistry::load(&state.pool, state.master_key.as_ref()).await?;
    *state.registry.write().await = registry.clone();
    let artwork = edit.artwork_data.as_deref();
    let mut libraries_to_scan = HashSet::new();
    let mut applied = 0_u64;

    for track_id in &edit.track_ids {
        let Some(track) = catalog::get_track(&state.pool, *track_id).await? else {
            warn!(%track_id, "track disappeared before tag writeback");
            continue;
        };
        let storage = registry
            .storage_for_library(track.library_id)
            .ok_or_else(|| CoreError::Unavailable("library storage is unavailable".to_owned()))?;
        let key = Key::new(track.file_key.clone())?;

        if let Some(path) = storage.local_path(&key) {
            write_tags_to_path(&path, changes, artwork)?;
        } else {
            let data = storage.read(&key).await?.to_vec();
            let updated = write_tags_to_bytes(data, changes, artwork)?;
            storage.write(&key, Bytes::from(updated)).await?;
        }

        libraries_to_scan.insert(track.library_id);
        applied += 1;
    }

    if applied == 0 {
        return Err(CoreError::not_found("track", "all tracks disappeared"));
    }

    for library_id in libraries_to_scan {
        let Some(library) = libraries::get(&state.pool, library_id).await? else {
            continue;
        };
        let Some(storage) = registry.storage_for_library(library.id) else {
            continue;
        };

        state
            .scanner
            .scan_library(&library, storage, ScanMode::Full)
            .await?;
    }

    Ok(())
}
