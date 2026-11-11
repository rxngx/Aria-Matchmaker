use dashmap::DashMap;
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc;
use serde::Serialize;

#[derive(Debug, Clone, Serialize)]
pub struct PlayerInfo {
    pub account_id: String,
    pub region: String,
    pub playlist: String,
    pub version: String,
    pub party: String,
    pub party_player_ids: Vec<String>,
    pub is_fill: bool,
    pub is_leader: bool,
    pub ticket_id: String,
    pub connected_at: u64,
}

#[derive(Debug)]
pub struct Player {
    pub info: PlayerInfo,
    pub tx: mpsc::UnboundedSender<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct GameSession {
    pub match_id: String,
    pub session_id: String,
    pub region: String,
    pub playlist: String,
    pub version: String,
    pub bucket_id: String,
    pub build_unique_id: String,
    pub max_players: u32,
    pub teams: Vec<Vec<Vec<String>>>,
    pub is_assigned: bool,
    pub is_assigning: bool,
    pub is_sending: bool,
    pub assign_match_sent: bool,
    pub stop_allowing_connections: bool,
    pub start_time_ms: u64,
    pub tournament_id: Option<String>,
    pub window_id: Option<String>,
}

impl GameSession {
    pub fn wait_time_secs(&self) -> u64 {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        (now.saturating_sub(self.start_time_ms)) / 1000
    }
}

#[derive(Debug)]
pub struct SessionHandle {
    pub session: Arc<Mutex<GameSession>>,
    pub tx: mpsc::UnboundedSender<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct PrivateLobby {
    pub custom_key: String,
    pub owner_id: String,
    pub region: String,
    pub playlist: String,
    pub version: String,
    pub created_at: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct MatchSizeEntry {
    pub region: String,
    pub playlist: String,
    pub count: u32,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct SchedulerStats {
    pub previous_match_sizes: Vec<MatchSizeEntry>,
    pub last_match_sizes: Vec<MatchSizeEntry>,
}

pub struct AppState {
    pub players: DashMap<String, Player>,
    pub sessions: DashMap<String, SessionHandle>,
    pub private_lobbies: DashMap<String, PrivateLobby>,
    pub lobby_players: DashMap<String, Vec<String>>,
    pub scheduler_stats: Mutex<SchedulerStats>,
    pub hmac_key: String,
    pub dev_mode: bool,
}

impl AppState {
    pub fn new(hmac_key: String, dev_mode: bool) -> Arc<Self> {
        Arc::new(Self {
            players: DashMap::new(),
            sessions: DashMap::new(),
            private_lobbies: DashMap::new(),
            lobby_players: DashMap::new(),
            scheduler_stats: Mutex::new(SchedulerStats::default()),
            hmac_key,
            dev_mode,
        })
    }
}

pub type SharedState = Arc<AppState>;
