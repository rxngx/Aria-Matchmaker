use crate::state::SharedState;
use axum::{
    extract::State,
    http::StatusCode,
    response::Json,
    routing::{delete, get},
    Router,
};
use serde_json::{json, Value};

pub fn router() -> Router<SharedState> {
    Router::new()
        .route("/s/players", get(get_players))
        .route("/s/servers", get(get_servers))
        .route("/s/lobbies", get(get_lobbies))
        .route("/s/stats", get(get_stats))
        .route("/s/stats", delete(reset_stats))
}

async fn get_players(State(state): State<SharedState>) -> Json<Value> {
    let players: Vec<Value> = state
        .players
        .iter()
        .map(|e| {
            json!({
                "accountId": e.value().info.account_id,
                "region": e.value().info.region,
                "playlist": e.value().info.playlist,
                "version": e.value().info.version,
                "isFill": e.value().info.is_fill,
            })
        })
        .collect();

    Json(json!({ "count": players.len(), "players": players }))
}

async fn get_servers(State(state): State<SharedState>) -> Json<Value> {
    let sessions: Vec<Value> = state
        .sessions
        .iter()
        .filter_map(|e| {
            e.value().session.lock().ok().map(|s| {
                json!({
                    "matchId": s.match_id,
                    "sessionId": s.session_id,
                    "region": s.region,
                    "playlist": s.playlist,
                    "version": s.version,
                    "isAssigned": s.is_assigned,
                    "isAssigning": s.is_assigning,
                    "waitTimeSecs": s.wait_time_secs(),
                })
            })
        })
        .collect();

    Json(json!({ "count": sessions.len(), "sessions": sessions }))
}

async fn get_lobbies(State(state): State<SharedState>) -> Json<Value> {
    let lobbies: Vec<Value> = state
        .private_lobbies
        .iter()
        .map(|e| {
            let key = e.key().clone();
            let lobby = e.value().clone();
            let members = state
                .lobby_players
                .get(&key)
                .map(|m| m.clone())
                .unwrap_or_default();
            json!({
                "customKey": lobby.custom_key,
                "ownerId": lobby.owner_id,
                "region": lobby.region,
                "playlist": lobby.playlist,
                "playerCount": members.len(),
            })
        })
        .collect();

    Json(json!({ "count": lobbies.len(), "lobbies": lobbies }))
}

async fn get_stats(State(state): State<SharedState>) -> Json<Value> {
    let stats = state.scheduler_stats.lock().unwrap().clone();
    Json(serde_json::to_value(stats).unwrap_or_default())
}

async fn reset_stats(State(state): State<SharedState>) -> StatusCode {
    let mut stats = state.scheduler_stats.lock().unwrap();
    stats.previous_match_sizes.clear();
    stats.last_match_sizes.clear();
    StatusCode::NO_CONTENT
}
