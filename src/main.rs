mod auth;
mod http;
mod player_socket;
mod playlist;
mod scheduler;
mod session_socket;
mod state;
mod team;

use axum::{routing::get, Router};
use dotenvy::dotenv;
use state::AppState;
use std::net::SocketAddr;
use tower_http::cors::CorsLayer;
use tracing::info;

#[tokio::main]
async fn main() {
    dotenv().ok();
    tracing_subscriber::fmt()
        .with_env_filter(
            std::env::var("RUST_LOG")
                .unwrap_or_else(|_| "info".to_string())
                .as_str(),
        )
        .init();

    let hmac_key = std::env::var("HMAC_KEY")
        .unwrap_or_else(|_| "ed14ba700b1aeb4103b457c2a43028e0".to_string());

    let dev_mode = std::env::var("DEV")
        .map(|v| v == "true" || v == "1")
        .unwrap_or(false);

    let port_players: u16 = std::env::var("PORT_PLAYERS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(2053);

    let port_sessions: u16 = std::env::var("PORT_SESSIONS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(30213);

    let port_http: u16 = std::env::var("PORT_HTTP")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(38912);

    let state = AppState::new(hmac_key, dev_mode);

    scheduler::start(state.clone());

    let player_app = Router::new()
        .route("/", get(player_socket::handler))
        .with_state(state.clone())
        .layer(CorsLayer::permissive());

    let session_app = Router::new()
        .route("/", get(session_socket::handler))
        .with_state(state.clone())
        .layer(CorsLayer::permissive());

    let http_app = http::router()
        .with_state(state.clone())
        .layer(CorsLayer::permissive());

    info!("Aria Matchmaker starting");
    info!("  Players  WebSocket → port {}", port_players);
    info!("  Sessions WebSocket → port {}", port_sessions);
    info!("  HTTP API            → port {}", port_http);
    if dev_mode {
        info!("  DEV mode enabled — min players = 1");
    }

    let addr_players = SocketAddr::from(([0, 0, 0, 0], port_players));
    let addr_sessions = SocketAddr::from(([0, 0, 0, 0], port_sessions));
    let addr_http = SocketAddr::from(([0, 0, 0, 0], port_http));

    let listener_players = tokio::net::TcpListener::bind(addr_players).await.unwrap();
    let listener_sessions = tokio::net::TcpListener::bind(addr_sessions).await.unwrap();
    let listener_http = tokio::net::TcpListener::bind(addr_http).await.unwrap();

    let (result_players, result_sessions, result_http) = tokio::join!(
        axum::serve(listener_players, player_app),
        axum::serve(listener_sessions, session_app),
        axum::serve(listener_http, http_app),
    );

    if let Err(e) = result_players { tracing::error!("Players server error: {}", e); }
    if let Err(e) = result_sessions { tracing::error!("Sessions server error: {}", e); }
    if let Err(e) = result_http { tracing::error!("HTTP server error: {}", e); }
}
