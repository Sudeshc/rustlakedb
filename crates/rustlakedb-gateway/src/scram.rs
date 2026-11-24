// Copyright (C) 2026 Sudeshc
// SPDX-License-Identifier: GPL-3.0-or-later

use base64::engine::general_purpose::STANDARD as B64;
use base64::Engine;
use ring::digest::{digest, SHA256};
use ring::hmac;
use ring::pbkdf2;
use ring::rand::{SecureRandom, SystemRandom};
use rustlakedb_common::{Result, RustLakeError};
use std::num::NonZeroU32;

// RFC 5802 / RFC 7677 / PostgreSQL 16 SCRAM-SHA-256 cryptographic authentication
// using `ring` PBKDF2-HMAC-SHA256, HMAC-SHA256, and SHA-256.

pub const SCRAM_ITERATIONS: u32 = 4096;

pub struct ScramServerChallenge {
    pub full_nonce: String,
    pub salt: [u8; 16],
    pub server_first_message: String,
    pub client_first_bare: String,
}

/// Parses `client-first-message` (`n,,n=<user>,r=<client_nonce>`) and builds `server-first-message`.
pub fn scram_server_first(client_first: &str) -> Result<ScramServerChallenge> {
    let bare = client_first
        .strip_prefix("n,,")
        .or_else(|| client_first.strip_prefix("y,,"))
        .unwrap_or(client_first)
        .to_string();

    let mut client_nonce = None;
    for part in bare.split(',') {
        if let Some(r) = part.strip_prefix("r=") {
            client_nonce = Some(r.to_string());
        }
    }
    let c_nonce = client_nonce
        .ok_or_else(|| RustLakeError::Execution("SCRAM-SHA-256 missing client nonce".into()))?;

    let rng = SystemRandom::new();
    let mut salt = [0u8; 16];
    let mut s_nonce_bytes = [0u8; 18];
    rng.fill(&mut salt)
        .map_err(|_| RustLakeError::Execution("RNG error generating SCRAM salt".into()))?;
    rng.fill(&mut s_nonce_bytes)
        .map_err(|_| RustLakeError::Execution("RNG error generating SCRAM nonce".into()))?;

    let full_nonce = format!("{c_nonce}{}", B64.encode(s_nonce_bytes));
    let server_first_message = format!(
        "r={full_nonce},s={},i={SCRAM_ITERATIONS}",
        B64.encode(salt)
    );

    Ok(ScramServerChallenge {
        full_nonce,
        salt,
        server_first_message,
        client_first_bare: bare,
    })
}

/// Verifies `client-final-message` (`c=biws,r=<nonce>,p=<proof>`) against the stored password
/// and returns `server-final-message` (`v=<base64_server_signature>`) on cryptographic match.
pub fn scram_server_verify(
    challenge: &ScramServerChallenge,
    client_final: &str,
    expected_password: &str,
) -> Result<String> {
    let (without_proof, proof_b64) = client_final
        .rsplit_once(",p=")
        .ok_or_else(|| RustLakeError::Execution("SCRAM-SHA-256 missing client proof".into()))?;

    // Validate nonce matches challenge
    let mut got_nonce = None;
    for part in without_proof.split(',') {
        if let Some(r) = part.strip_prefix("r=") {
            got_nonce = Some(r);
        }
    }
    if got_nonce != Some(challenge.full_nonce.as_str()) {
        return Err(RustLakeError::Execution(
            "SCRAM-SHA-256 nonce mismatch".into(),
        ));
    }

    let client_proof = B64
        .decode(proof_b64)
        .map_err(|_| RustLakeError::Execution("SCRAM-SHA-256 invalid base64 proof".into()))?;
    if client_proof.len() != 32 {
        return Err(RustLakeError::Execution(
            "SCRAM-SHA-256 invalid proof length".into(),
        ));
    }

    let salted_pw = derive_salted_password(expected_password, &challenge.salt, SCRAM_ITERATIONS);
    let client_key = hmac_sha256(&salted_pw, b"Client Key");
    let stored_key = sha256(&client_key);

    let auth_message = format!(
        "{},{},{}",
        challenge.client_first_bare, challenge.server_first_message, without_proof
    );
    let client_signature = hmac_sha256(&stored_key, auth_message.as_bytes());

    let mut recovered_client_key = [0u8; 32];
    for i in 0..32 {
        recovered_client_key[i] = client_proof[i] ^ client_signature[i];
    }
    let recovered_stored = sha256(&recovered_client_key);

    // Constant-time comparison
    let mut diff = 0u8;
    for i in 0..32 {
        diff |= recovered_stored[i] ^ stored_key[i];
    }
    if diff != 0 {
        return Err(RustLakeError::Execution(
            "password authentication failed (SCRAM-SHA-256)".into(),
        ));
    }

    let server_key = hmac_sha256(&salted_pw, b"Server Key");
    let server_sig = hmac_sha256(&server_key, auth_message.as_bytes());
    Ok(format!("v={}", B64.encode(server_sig)))
}

/// Client-side SCRAM-SHA-256 proof builder (used by `PgTestClient::connect_scram` and tests).
pub fn scram_client_final(
    client_first_bare: &str,
    server_first: &str,
    password: &str,
) -> Result<(String, String)> {
    let mut nonce = None;
    let mut salt_b64 = None;
    let mut iters = SCRAM_ITERATIONS;
    for part in server_first.split(',') {
        if let Some(r) = part.strip_prefix("r=") {
            nonce = Some(r);
        } else if let Some(s) = part.strip_prefix("s=") {
            salt_b64 = Some(s);
        } else if let Some(i) = part.strip_prefix("i=") {
            iters = i.parse::<u32>().unwrap_or(SCRAM_ITERATIONS);
        }
    }
    let full_nonce = nonce.ok_or_else(|| RustLakeError::Execution("missing server nonce".into()))?;
    let salt = B64
        .decode(salt_b64.ok_or_else(|| RustLakeError::Execution("missing salt".into()))?)
        .map_err(|_| RustLakeError::Execution("bad base64 salt".into()))?;

    let without_proof = format!("c=biws,r={full_nonce}");
    let auth_message = format!("{client_first_bare},{server_first},{without_proof}");
    let salted_pw = derive_salted_password(password, &salt, iters);
    let client_key = hmac_sha256(&salted_pw, b"Client Key");
    let stored_key = sha256(&client_key);
    let client_sig = hmac_sha256(&stored_key, auth_message.as_bytes());
    let mut proof = [0u8; 32];
    for i in 0..32 {
        proof[i] = client_key[i] ^ client_sig[i];
    }
    let server_key = hmac_sha256(&salted_pw, b"Server Key");
    let expected_server_sig = B64.encode(hmac_sha256(&server_key, auth_message.as_bytes()));
    Ok((
        format!("{without_proof},p={}", B64.encode(proof)),
        format!("v={expected_server_sig}"),
    ))
}

fn derive_salted_password(password: &str, salt: &[u8], iterations: u32) -> [u8; 32] {
    let mut out = [0u8; 32];
    pbkdf2::derive(
        pbkdf2::PBKDF2_HMAC_SHA256,
        NonZeroU32::new(iterations.max(1)).unwrap(),
        salt,
        password.as_bytes(),
        &mut out,
    );
    out
}

fn hmac_sha256(key: &[u8], data: &[u8]) -> [u8; 32] {
    let k = hmac::Key::new(hmac::HMAC_SHA256, key);
    let tag = hmac::sign(&k, data);
    let mut out = [0u8; 32];
    out.copy_from_slice(tag.as_ref());
    out
}

fn sha256(data: &[u8]) -> [u8; 32] {
    let d = digest(&SHA256, data);
    let mut out = [0u8; 32];
    out.copy_from_slice(d.as_ref());
    out
}
