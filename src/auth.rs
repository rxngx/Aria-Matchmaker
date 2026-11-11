use anyhow::{bail, Result};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use hmac::{Hmac, Mac};
use sha2::Sha256;
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

pub struct TokenClaims {
    pub account_id: String,
    pub region: String,
    pub playlist: String,
    pub version: String,
    pub bucket_id: String,
    pub build_unique_id: String,
    pub party: String,
    pub party_player_ids: Vec<String>,
    pub is_fill: bool,
    pub is_leader: bool,
    pub custom_key: Option<String>,
    pub can_create_private_match: bool,
    pub tournament_id: Option<String>,
    pub window_id: Option<String>,
    pub token_type: Option<String>,
    pub session_id: Option<String>,
}

pub fn validate(authorization: &str, hmac_key: &str) -> Result<TokenClaims> {
    let parts: Vec<&str> = authorization.split_whitespace().collect();

    if parts.len() < 4 || parts[0] != "Epic-Signed" {
        bail!("malformed authorization header");
    }

    let payload_b64 = parts[2];
    let signature_b64 = parts[3];

    let expected = compute_hmac(hmac_key, payload_b64)?;
    let provided = URL_SAFE_NO_PAD
        .decode(signature_b64)
        .or_else(|_| {
            base64::engine::general_purpose::STANDARD.decode(signature_b64)
        })?;

    if !constant_time_eq(&expected, &provided) {
        bail!("invalid signature");
    }

    let json_bytes = URL_SAFE_NO_PAD
        .decode(payload_b64)
        .or_else(|_| {
            base64::engine::general_purpose::STANDARD.decode(payload_b64)
        })?;

    let claims: HashMap<String, serde_json::Value> = serde_json::from_slice(&json_bytes)?;

    if let Some(exp) = claims.get("exp").and_then(|v| v.as_i64()) {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
        if exp < now {
            bail!("token expired");
        }
    }

    let str_claim = |key: &str| -> String {
        claims
            .get(key)
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string()
    };

    let bool_claim = |key: &str| -> bool {
        claims
            .get(key)
            .and_then(|v| v.as_str())
            .and_then(|s| s.parse::<bool>().ok())
            .unwrap_or(false)
    };

    let optional_str = |key: &str| -> Option<String> {
        claims
            .get(key)
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty())
            .map(|s| s.to_string())
    };

    let party_player_ids: Vec<String> = str_claim("partyPlayerIds")
        .split(',')
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .collect();

    let custom_key = optional_str("customKey").filter(|k| k != "???");

    Ok(TokenClaims {
        account_id: str_claim("accountId"),
        region: str_claim("region"),
        playlist: str_claim("playlist").to_lowercase(),
        version: str_claim("version"),
        bucket_id: str_claim("bucketId"),
        build_unique_id: str_claim("buildUniqueId"),
        party: str_claim("party"),
        party_player_ids,
        is_fill: bool_claim("bIsFill"),
        is_leader: bool_claim("partyLeader"),
        custom_key,
        can_create_private_match: bool_claim("canCreatePrivateMatch"),
        tournament_id: optional_str("tournamentId"),
        window_id: optional_str("windowId"),
        token_type: optional_str("type"),
        session_id: optional_str("sessionId"),
    })
}

fn compute_hmac(key: &str, data: &str) -> Result<Vec<u8>> {
    let mut mac = Hmac::<Sha256>::new_from_slice(key.as_bytes())?;
    mac.update(data.as_bytes());
    Ok(mac.finalize().into_bytes().to_vec())
}

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff: u8 = 0;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}
