use crate::playlist::select_playlist;
use crate::state::{AppState, MatchSizeEntry, PlayerInfo, SchedulerStats};
use crate::team::{build_teams, max_players_for};
use serde_json::json;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tracing::{info, warn};

const MAX_WAIT_MS: u64 = 180_000;
const SCHEDULER_INTERVAL_MS: u64 = 2_000;
const ABSOLUTE_MIN_PLAYERS: u32 = 1;

pub fn start(state: Arc<AppState>) {
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_millis(SCHEDULER_INTERVAL_MS)).await;
            process(&state).await;
        }
    });
}

async fn process(state: &Arc<AppState>) {
    let mut regions: HashSet<String> = HashSet::new();

    for entry in state.sessions.iter() {
        if let Ok(s) = entry.value().session.lock() {
            if !s.is_assigned && !s.assign_match_sent {
                regions.insert(s.region.clone());
            }
        }
    }

    for region in regions {
        let selection = select_playlist(state, &region);
        if selection.count == 0 {
            continue;
        }

        let session_key = state.sessions.iter().find_map(|entry| {
            if let Ok(s) = entry.value().session.lock() {
                let ready = s.region == region
                    && !s.is_assigned
                    && !s.is_assigning
                    && !s.assign_match_sent
                    && !s.is_sending;
                if ready {
                    return Some(entry.key().clone());
                }
            }
            None
        });

        let session_key = match session_key {
            Some(k) => k,
            None => continue,
        };

        let min_players = {
            let stats = state.scheduler_stats.lock().unwrap();
            let wait_ms = state
                .sessions
                .get(&session_key)
                .and_then(|h| h.session.lock().ok().map(|s| {
                    let now = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap()
                        .as_millis() as u64;
                    now.saturating_sub(s.start_time_ms)
                }))
                .unwrap_or(0);

            adaptive_min_players(&stats, &region, &selection.playlist, wait_ms, state.dev_mode)
        };

        if selection.count < min_players as usize {
            info!(
                "Region={} playlist={} players={}/{} — waiting",
                region, selection.playlist, selection.count, min_players
            );
            continue;
        }

        let players: Vec<PlayerInfo> = state
            .players
            .iter()
            .filter(|e| {
                e.value().info.region.to_lowercase() == region.to_lowercase()
                    && e.value().info.playlist.to_lowercase() == selection.playlist
            })
            .map(|e| e.value().info.clone())
            .collect();

        let max_players = max_players_for(&selection.playlist);
        let teams = build_teams(&players, &selection.playlist, max_players);

        let (match_id, bucket_id, _build_unique_id, tx) = {
            let handle = match state.sessions.get(&session_key) {
                Some(h) => h,
                None => continue,
            };
            let mut session = handle.session.lock().unwrap();
            session.is_sending = true;
            session.is_assigning = true;
            session.playlist = selection.playlist.clone();
            session.teams = teams.clone();
            session.max_players = max_players;
            (
                session.match_id.clone(),
                session.bucket_id.clone(),
                session.build_unique_id.clone(),
                handle.tx.clone(),
            )
        };

        let assign_payload = json!({
            "name": "AssignMatch",
            "payload": {
                "bucketId": bucket_id,
                "matchId": match_id,
                "matchOptions": build_match_options(state, &session_key),
                "teams": teams,
            }
        });

        if tx.send(assign_payload.to_string()).is_ok() {
            if let Some(handle) = state.sessions.get(&session_key) {
                if let Ok(mut s) = handle.session.lock() {
                    s.assign_match_sent = true;
                }
            }

            {
                let mut stats = state.scheduler_stats.lock().unwrap();
                record_match_size(&mut stats, &region, &selection.playlist, players.len() as u32);
            }

            info!(
                "Match assigned: session={} region={} playlist={} players={}",
                session_key,
                region,
                selection.playlist,
                players.len()
            );
        } else {
            if let Some(handle) = state.sessions.get(&session_key) {
                if let Ok(mut s) = handle.session.lock() {
                    s.is_sending = false;
                    s.is_assigning = false;
                }
            }
            warn!("Failed to send AssignMatch to session={}", session_key);
        }
    }
}

fn adaptive_min_players(
    stats: &SchedulerStats,
    region: &str,
    playlist: &str,
    wait_ms: u64,
    dev_mode: bool,
) -> u32 {
    if dev_mode {
        return ABSOLUTE_MIN_PLAYERS;
    }

    let previous: Vec<u32> = stats
        .previous_match_sizes
        .iter()
        .filter(|e| e.region == region && e.playlist == playlist)
        .map(|e| e.count)
        .collect();

    let base = if previous.len() >= 3 {
        statistical_min(&previous)
    } else {
        ema_min(stats, region, playlist, previous.last().copied().unwrap_or(1))
    };

    time_decay_min(base, wait_ms)
}

fn statistical_min(sizes: &[u32]) -> u32 {
    let avg = sizes.iter().sum::<u32>() as f64 / sizes.len() as f64;
    let variance = sizes.iter().map(|&x| (x as f64 - avg).powi(2)).sum::<f64>() / sizes.len() as f64;
    let std_dev = variance.sqrt();
    let min = (avg - std_dev * 0.5).round() as u32;
    min.clamp(ABSOLUTE_MIN_PLAYERS, 100)
}

fn ema_min(stats: &SchedulerStats, region: &str, playlist: &str, last_size: u32) -> u32 {
    let alpha = 0.6f64;
    let last_ema = stats
        .last_match_sizes
        .iter()
        .find(|e| e.region == region && e.playlist == playlist)
        .map(|e| e.count)
        .unwrap_or(last_size);

    let ema = (alpha * last_size as f64 + (1.0 - alpha) * last_ema as f64).round() as u32;
    let scale = if last_size > 50 { 0.9f64 } else { 0.8f64 };
    ((ema as f64 * scale).round() as u32).clamp(ABSOLUTE_MIN_PLAYERS, 100)
}

fn time_decay_min(base: u32, wait_ms: u64) -> u32 {
    if wait_ms >= MAX_WAIT_MS {
        return ABSOLUTE_MIN_PLAYERS;
    }
    let ratio = wait_ms as f64 / MAX_WAIT_MS as f64;
    let reduction = ((base - ABSOLUTE_MIN_PLAYERS) as f64 * ratio).round() as u32;
    base.saturating_sub(reduction).max(ABSOLUTE_MIN_PLAYERS)
}

fn record_match_size(stats: &mut SchedulerStats, region: &str, playlist: &str, count: u32) {
    let entry = MatchSizeEntry {
        region: region.to_string(),
        playlist: playlist.to_string(),
        count,
    };

    stats.last_match_sizes.retain(|e| !(e.region == region && e.playlist == playlist));
    stats.last_match_sizes.push(entry.clone());
    if stats.last_match_sizes.len() > 5 {
        stats.last_match_sizes.remove(0);
    }

    stats.previous_match_sizes.push(entry);
    if stats.previous_match_sizes.len() > 10 {
        stats.previous_match_sizes.remove(0);
    }
}

fn build_match_options(state: &Arc<AppState>, session_key: &str) -> serde_json::Value {
    let mut opts: HashMap<String, String> = HashMap::new();
    if let Some(handle) = state.sessions.get(session_key) {
        if let Ok(s) = handle.session.lock() {
            if let Some(ref tid) = s.tournament_id {
                opts.insert("match.option.events.tournamentid".to_string(), tid.clone());
            }
            if let Some(ref wid) = s.window_id {
                opts.insert("match.option.events.windowid".to_string(), wid.clone());
            }
        }
    }
    serde_json::to_value(opts).unwrap_or_default()
}
