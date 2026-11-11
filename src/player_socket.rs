use crate::auth::validate;
use crate::state::{Player, PlayerInfo, PrivateLobby, SharedState};
use crate::team::squad_size_for;
use axum::{
    extract::{
        ws::{Message, WebSocket},
        State, WebSocketUpgrade,
    },
    http::{HeaderMap, StatusCode},
    response::Response,
};
use futures_util::{SinkExt, StreamExt};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc;
use tracing::{info, warn};
use uuid::Uuid;

const LOBBY_MAX_PLAYERS: usize = 100;
const QUEUED_UPDATE_INTERVAL_MS: u64 = 3_500;

pub async fn handler(
    ws: WebSocketUpgrade,
    headers: HeaderMap,
    State(state): State<SharedState>,
) -> Result<Response, StatusCode> {
    let auth = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");

    match validate(auth, &state.hmac_key) {
        Ok(claims) => Ok(ws.on_upgrade(move |socket| handle(socket, claims, state))),
        Err(e) => {
            warn!("Player auth failed: {}", e);
            Err(StatusCode::UNAUTHORIZED)
        }
    }
}

async fn handle(socket: WebSocket, claims: crate::auth::TokenClaims, state: SharedState) {
    let account_id = claims.account_id.clone();
    let (tx, mut rx) = mpsc::unbounded_channel::<String>();
    let (mut sink, mut stream) = socket.split();

    if let Some(existing) = state.players.get(&account_id) {
        let _ = existing.tx.send(close_msg("player.duplicate_player"));
    }

    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;

    let ticket_id = Uuid::new_v4().to_string().replace('-', "");

    let party_size = claims.party_player_ids.len().max(1);
    let max_squad = squad_size_for(&claims.playlist) as usize;

    if party_size > max_squad {
        let _ = sink.send(Message::Text(error_msg("player.party_too_big"))).await;
        return;
    }

    if let Some(ref custom_key) = claims.custom_key {
        if !claims.can_create_private_match {
            if !state.private_lobbies.contains_key(custom_key) {
                let _ = sink.send(Message::Text(error_msg("match.custom_key_not_found"))).await;
                return;
            }
        }

        let current_count = state
            .lobby_players
            .get(custom_key)
            .map(|m| m.len())
            .unwrap_or(0);

        if current_count >= LOBBY_MAX_PLAYERS {
            let _ = sink.send(Message::Text(error_msg("player.capacity_exceeded"))).await;
            return;
        }

        state.private_lobbies.entry(custom_key.clone()).or_insert_with(|| PrivateLobby {
            custom_key: custom_key.clone(),
            owner_id: account_id.clone(),
            region: claims.region.clone(),
            playlist: claims.playlist.clone(),
            version: claims.version.clone(),
            created_at: now_ms,
        });

        state
            .lobby_players
            .entry(custom_key.clone())
            .or_default()
            .push(account_id.clone());

        info!("Player {} joined private lobby key={}", account_id, custom_key);
    }

    let info = PlayerInfo {
        account_id: account_id.clone(),
        region: claims.region.clone(),
        playlist: claims.playlist.clone(),
        version: claims.version.clone(),
        party: claims.party.clone(),
        party_player_ids: claims.party_player_ids.clone(),
        is_fill: claims.is_fill,
        is_leader: claims.is_leader,
        ticket_id: ticket_id.clone(),
        connected_at: now_ms,
    };

    let player = Player { info: info.clone(), tx: tx.clone() };
    state.players.insert(account_id.clone(), player);

    let connected = count_players_in(&state, &info.region, &info.playlist, &info.version);

    send_ws(&mut sink, status_connecting()).await;
    send_ws(&mut sink, status_waiting(connected)).await;
    send_ws(&mut sink, status_queued(&ticket_id, connected)).await;

    info!(
        "Player connected: id={} region={} playlist={} version={} party_size={}",
        account_id, info.region, info.playlist, info.version, party_size
    );

    let account_id_clone = account_id.clone();
    let state_clone = state.clone();

    let send_task = tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            if sink.send(Message::Text(msg)).await.is_err() {
                break;
            }
        }
    });

    let tx_queued = tx.clone();
    let state_queued = state.clone();
    let ticket_id_queued = ticket_id.clone();
    let region_queued = info.region.clone();
    let playlist_queued = info.playlist.clone();
    let version_queued = info.version.clone();

    let queued_task = tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_millis(QUEUED_UPDATE_INTERVAL_MS)).await;
            let count = count_players_in(&state_queued, &region_queued, &playlist_queued, &version_queued);
            if tx_queued.send(status_queued(&ticket_id_queued, count)).is_err() {
                break;
            }
        }
    });

    while let Some(Ok(msg)) = stream.next().await {
        match msg {
            Message::Text(text) if text == "ping" => {
                let _ = tx.send("pong".to_string());
            }
            Message::Text(text) => {
                handle_player_message(&text, &account_id, &claims.custom_key, &state);
            }
            Message::Close(_) => break,
            _ => {}
        }
    }

    send_task.abort();
    queued_task.abort();

    state_clone.players.remove(&account_id_clone);

    if let Some(ref custom_key) = claims.custom_key {
        cleanup_lobby_player(&state_clone, custom_key, &account_id_clone);
    } else {
        notify_party_disconnect(&state_clone, &claims.party_player_ids, &account_id_clone);
    }

    info!("Player disconnected: id={}", account_id_clone);
}

fn handle_player_message(text: &str, account_id: &str, custom_key: &Option<String>, state: &SharedState) {
    let Ok(data) = serde_json::from_str::<serde_json::Value>(text) else {
        return;
    };

    if data["name"].as_str() != Some("Exec") {
        return;
    }

    let command = data["payload"]["command"].as_str().unwrap_or("");

    if command == "p.StartMatch" {
        let Some(ref key) = custom_key else { return };

        let is_owner = state
            .private_lobbies
            .get(key)
            .map(|l| l.owner_id == account_id)
            .unwrap_or(false);

        if !is_owner {
            return;
        }

        let members: Vec<String> = state
            .lobby_players
            .get(key)
            .map(|m| m.clone())
            .unwrap_or_default();

        let session = state.sessions.iter().find_map(|entry| {
            if let Ok(s) = entry.value().session.lock() {
                let matches = !s.is_assigned
                    && !s.assign_match_sent
                    && s.region == state.private_lobbies.get(key).map(|l| l.region.clone()).unwrap_or_default();
                if matches {
                    return Some(entry.key().clone());
                }
            }
            None
        });

        let Some(session_key) = session else {
            warn!("p.StartMatch: no available session for lobby key={}", key);
            return;
        };

        let playlist = state
            .private_lobbies
            .get(key)
            .map(|l| l.playlist.clone())
            .unwrap_or_default();

        let teams: Vec<Vec<Vec<String>>> = members
            .iter()
            .map(|id| vec![vec![id.clone()]])
            .collect();

        let (match_id, bucket_id) = {
            let handle = match state.sessions.get(&session_key) {
                Some(h) => h,
                None => return,
            };
            let mut s = handle.session.lock().unwrap();
            s.is_sending = true;
            s.is_assigning = true;
            s.playlist = playlist.clone();
            s.teams = teams.clone();
            (s.match_id.clone(), s.bucket_id.clone())
        };

        let payload = serde_json::json!({
            "name": "AssignMatch",
            "payload": {
                "bucketId": bucket_id,
                "matchId": match_id,
                "matchOptions": {},
                "teams": teams,
            }
        });

        if let Some(handle) = state.sessions.get(&session_key) {
            let _ = handle.tx.send(payload.to_string());
            if let Ok(mut s) = handle.session.lock() {
                s.assign_match_sent = true;
            }
        }

        info!("p.StartMatch forced by owner={} key={} session={}", account_id, key, session_key);
    }
}

fn count_players_in(state: &SharedState, region: &str, playlist: &str, version: &str) -> usize {
    state
        .players
        .iter()
        .filter(|e| {
            let p = &e.value().info;
            p.region.to_lowercase() == region.to_lowercase()
                && p.playlist.to_lowercase() == playlist.to_lowercase()
                && p.version == version
        })
        .count()
}

fn notify_party_disconnect(state: &SharedState, party_ids: &[String], disconnected_id: &str) {
    for pid in party_ids {
        if pid == disconnected_id {
            continue;
        }
        if let Some(p) = state.players.get(pid) {
            let _ = p.tx.send(error_msg("player.party_member_disconnected"));
        }
    }
}

fn cleanup_lobby_player(state: &SharedState, custom_key: &str, account_id: &str) {
    let mut empty = false;
    let mut is_owner = false;

    if let Some(lobby) = state.private_lobbies.get(custom_key) {
        is_owner = lobby.owner_id == account_id;
    }

    if let Some(mut members) = state.lobby_players.get_mut(custom_key) {
        members.retain(|id| id != account_id);
        empty = members.is_empty();
    }

    if empty || is_owner {
        state.private_lobbies.remove(custom_key);
        state.lobby_players.remove(custom_key);
        info!("Private lobby disbanded: key={}", custom_key);
    }
}

async fn send_ws(sink: &mut (impl SinkExt<Message, Error = axum::Error> + Unpin), msg: String) {
    let _ = sink.send(Message::Text(msg)).await;
}

fn status_connecting() -> String {
    serde_json::json!({
        "name": "StatusUpdate",
        "payload": { "state": "Connecting" }
    })
    .to_string()
}

fn status_waiting(connected: usize) -> String {
    serde_json::json!({
        "name": "StatusUpdate",
        "payload": {
            "state": "Waiting",
            "totalPlayers": connected,
            "connectedPlayers": connected
        }
    })
    .to_string()
}

fn status_queued(ticket_id: &str, queued: usize) -> String {
    let wait: u64 = 30 + rand_u64() % 60;
    serde_json::json!({
        "name": "StatusUpdate",
        "payload": {
            "state": "Queued",
            "ticketId": ticket_id,
            "queuedPlayers": queued,
            "estimatedWaitSec": wait,
            "status": {}
        }
    })
    .to_string()
}

fn error_msg(code: &str) -> String {
    serde_json::json!({
        "name": "StatusUpdate",
        "payload": { "state": "Error", "errorCode": code }
    })
    .to_string()
}

fn close_msg(reason: &str) -> String {
    error_msg(reason)
}

fn rand_u64() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .subsec_nanos() as u64
}
