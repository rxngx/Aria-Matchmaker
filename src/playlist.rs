use crate::state::AppState;
use std::collections::HashMap;
use std::sync::Arc;

pub struct PlaylistSelection {
    pub playlist: String,
    pub count: usize,
}

pub fn select_playlist(state: &Arc<AppState>, region: &str) -> PlaylistSelection {
    let mut player_counts: HashMap<String, usize> = HashMap::new();
    let mut session_counts: HashMap<String, usize> = HashMap::new();

    for entry in state.players.iter() {
        let p = entry.value();
        if p.info.region.to_lowercase() == region.to_lowercase() {
            *player_counts.entry(p.info.playlist.clone()).or_insert(0) += 1;
        }
    }

    for entry in state.sessions.iter() {
        let handle = entry.value();
        if let Ok(session) = handle.session.lock() {
            if session.region.to_lowercase() == region.to_lowercase() {
                *session_counts.entry(session.playlist.clone()).or_insert(0) += 1;
            }
        }
    }

    if player_counts.is_empty() {
        return PlaylistSelection {
            playlist: "playlist_defaultsolo".to_string(),
            count: 0,
        };
    }

    struct Metric {
        playlist: String,
        player_count: usize,
        needs_server: bool,
    }

    let metrics: Vec<Metric> = player_counts
        .iter()
        .map(|(playlist, &player_count)| {
            let server_count = *session_counts.get(playlist).unwrap_or(&0);
            let max_per_server: usize = if playlist.contains("playlist_default") { 100 } else { 100 };
            let players_per_server = if server_count > 0 {
                player_count / server_count
            } else {
                player_count
            };
            let needs_server = players_per_server >= max_per_server || server_count == 0;
            Metric {
                playlist: playlist.clone(),
                player_count,
                needs_server,
            }
        })
        .collect();

    if let Some(m) = metrics.iter().find(|m| m.needs_server) {
        return PlaylistSelection {
            playlist: m.playlist.clone(),
            count: m.player_count,
        };
    }

    let best = metrics.iter().max_by_key(|m| m.player_count).unwrap();
    PlaylistSelection {
        playlist: best.playlist.clone(),
        count: best.player_count,
    }
}
