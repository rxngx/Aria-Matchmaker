use crate::auth::validate;
use crate::state::{GameSession, SessionHandle, SharedState};
use axum::{
    extract::{
        ws::{Message, WebSocket},
        State, WebSocketUpgrade,
    },
    http::{HeaderMap, StatusCode},
    response::Response,
};
use futures_util::{SinkExt, StreamExt};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc;
use tracing::{info, warn};
use uuid::Uuid;

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
        Ok(claims) => {
            if claims.token_type.as_deref() != Some("d") {
                warn!("Session socket rejected: token type is not 'd'");
                return Err(StatusCode::UNAUTHORIZED);
            }
            Ok(ws.on_upgrade(move |socket| handle(socket, claims, state)))
        }
        Err(e) => {
            warn!("Session auth failed: {}", e);
            Err(StatusCode::UNAUTHORIZED)
        }
    }
}

async fn handle(socket: WebSocket, claims: crate::auth::TokenClaims, state: SharedState) {
    let session_id = claims
        .session_id
        .clone()
        .unwrap_or_else(|| Uuid::new_v4().to_string());

    let match_id = Uuid::new_v4().to_string().replace('-', "");

    let (tx, mut rx) = mpsc::unbounded_channel::<String>();
    let (mut sink, mut stream) = socket.split();

    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;

    let game_session = Arc::new(Mutex::new(GameSession {
        match_id: match_id.clone(),
        session_id: session_id.clone(),
        region: claims.region.clone(),
        playlist: String::new(),
        version: claims.version.clone(),
        bucket_id: claims.bucket_id.clone(),
        build_unique_id: claims.build_unique_id.clone(),
        max_players: 100,
        teams: Vec::new(),
        is_assigned: false,
        is_assigning: false,
        is_sending: false,
        assign_match_sent: false,
        stop_allowing_connections: false,
        start_time_ms: now_ms,
        tournament_id: claims.tournament_id.clone(),
        window_id: claims.window_id.clone(),
    }));

    let handle = SessionHandle {
        session: game_session.clone(),
        tx: tx.clone(),
    };

    state.sessions.insert(match_id.clone(), handle);

    let registered = serde_json::json!({ "name": "Registered", "payload": {} }).to_string();
    let _ = sink.send(Message::Text(registered)).await;

    info!(
        "Session connected: session_id={} match_id={} region={}",
        session_id, match_id, claims.region
    );

    let state_send = state.clone();
    let match_id_send = match_id.clone();

    let send_task = tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            if sink.send(Message::Text(msg)).await.is_err() {
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
                handle_message(&text, &match_id, &state);
            }
            Message::Close(_) => break,
            _ => {}
        }
    }

    send_task.abort();
    state_send.sessions.remove(&match_id_send);

    info!("Session disconnected: match_id={}", match_id_send);
}

fn handle_message(text: &str, match_id: &str, state: &SharedState) {
    let Ok(data) = serde_json::from_str::<serde_json::Value>(text) else {
        return;
    };

    let name = data["name"].as_str().unwrap_or("");

    if name == "AssignMatchResult" {
        let result = data["payload"]["result"].as_str().unwrap_or("");
        let assigned_match_id = data["payload"]["matchId"]
            .as_str()
            .unwrap_or("")
            .to_string();

        if result == "ready" {
            if let Some(handle) = state.sessions.get(match_id) {
                if let Ok(mut session) = handle.session.lock() {
                    session.is_assigned = true;
                }

                let players_to_notify: Vec<_> = state
                    .players
                    .iter()
                    .filter_map(|e| {
                        let p = e.value();
                        let is_in_session = state
                            .sessions
                            .get(match_id)
                            .and_then(|h| {
                                h.session.lock().ok().map(|s| {
                                    s.teams.iter().any(|team| {
                                        team.iter().any(|members| {
                                            members.iter().any(|id| id == &p.info.account_id)
                                        })
                                    })
                                })
                            })
                            .unwrap_or(false);

                        if is_in_session {
                            Some((p.tx.clone(), p.info.account_id.clone()))
                        } else {
                            None
                        }
                    })
                    .collect();

                let (session_id, teams_match_id) = state
                    .sessions
                    .get(match_id)
                    .and_then(|h| {
                        h.session.lock().ok().map(|s| (s.session_id.clone(), s.match_id.clone()))
                    })
                    .unwrap_or_default();

                for (tx, account_id) in &players_to_notify {
                    let play_msg = serde_json::json!({
                        "name": "Play",
                        "payload": {
                            "matchId": teams_match_id,
                            "sessionId": session_id,
                            "joinDelaySec": 0
                        }
                    })
                    .to_string();
                    let _ = tx.send(play_msg);

                    state.players.remove(account_id);
                }

                info!(
                    "Match ready: match_id={} notified {} players",
                    match_id,
                    players_to_notify.len()
                );
            }
        } else {
            if let Some(handle) = state.sessions.get(match_id) {
                if let Ok(mut s) = handle.session.lock() {
                    s.is_assigning = false;
                    s.is_sending = false;
                    s.assign_match_sent = false;
                }
            }

            let unassign = serde_json::json!({
                "name": "UnassignMatch",
                "payload": { "matchId": assigned_match_id }
            })
            .to_string();

            if let Some(handle) = state.sessions.get(match_id) {
                let _ = handle.tx.send(unassign);
            }

            warn!("AssignMatch failed for match_id={}", match_id);
        }
    }
}
