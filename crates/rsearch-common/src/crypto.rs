//! FIPS-approved credential primitives via aws-lc-rs (FIPS mode):
//! PBKDF2-HMAC-SHA256 password hashing and SHA-256 token digests.

use std::num::NonZeroU32;

use aws_lc_rs::{digest, pbkdf2, rand};

const PBKDF2_ITERATIONS: u32 = 600_000;
const SALT_LEN: usize = 16;
const HASH_LEN: usize = 32;

/// Hash a password. Output: `pbkdf2-sha256$<iters>$<salt_b64>$<hash_b64>`.
pub fn hash_password(password: &str) -> Result<String, String> {
    let mut salt = [0u8; SALT_LEN];
    rand::fill(&mut salt).map_err(|e| format!("rng failure: {e:?}"))?;
    let mut out = [0u8; HASH_LEN];
    pbkdf2::derive(
        pbkdf2::PBKDF2_HMAC_SHA256,
        NonZeroU32::new(PBKDF2_ITERATIONS).unwrap(),
        &salt,
        password.as_bytes(),
        &mut out,
    );
    Ok(format!(
        "pbkdf2-sha256${PBKDF2_ITERATIONS}${}${}",
        b64(&salt),
        b64(&out)
    ))
}

/// Constant-time-ish verification against a stored hash string.
pub fn verify_password(password: &str, stored: &str) -> bool {
    let mut parts = stored.split('$');
    let (Some(alg), Some(iters), Some(salt), Some(hash)) =
        (parts.next(), parts.next(), parts.next(), parts.next())
    else {
        return false;
    };
    if alg != "pbkdf2-sha256" {
        return false;
    }
    let Ok(iters) = iters.parse::<u32>() else {
        return false;
    };
    let Some(iters) = NonZeroU32::new(iters) else {
        return false;
    };
    let (Some(salt), Some(hash)) = (unb64(salt), unb64(hash)) else {
        return false;
    };
    pbkdf2::verify(
        pbkdf2::PBKDF2_HMAC_SHA256,
        iters,
        &salt,
        password.as_bytes(),
        &hash,
    )
    .is_ok()
}

/// A fixed, valid PBKDF2 hash to verify against when a user does not
/// exist, so the absent-user auth path costs the same as a real verify
/// (defeats username-enumeration timing). Verifying any password against
/// it returns false.
pub fn dummy_password_hash() -> String {
    // Precomputed hash of a random secret nobody knows; format matches
    // hash_password so verify_password runs the full 600k iterations.
    "pbkdf2-sha256$600000$AAAAAAAAAAAAAAAAAAAAAA$\
     x9QJ0dG0Z0mVn2yqTiVn5x6eXbYy9k3s7Xy0oP2mQ4A"
        .to_string()
}

/// SHA-256 hex digest — used to store session/API-key tokens.
pub fn token_digest(token: &str) -> String {
    let digest = digest::digest(&digest::SHA256, token.as_bytes());
    digest.as_ref().iter().map(|b| format!("{b:02x}")).collect()
}

/// Generate an opaque random token (32 bytes, base64url).
pub fn generate_token() -> Result<String, String> {
    let mut bytes = [0u8; 32];
    rand::fill(&mut bytes).map_err(|e| format!("rng failure: {e:?}"))?;
    Ok(b64(&bytes))
}

// Minimal base64url (no padding) — avoids pulling a base64 crate.
const B64_CHARS: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";

fn b64(data: &[u8]) -> String {
    let mut out = String::with_capacity(data.len().div_ceil(3) * 4);
    for chunk in data.chunks(3) {
        let b = [
            chunk[0],
            chunk.get(1).copied().unwrap_or(0),
            chunk.get(2).copied().unwrap_or(0),
        ];
        let n = u32::from_be_bytes([0, b[0], b[1], b[2]]);
        out.push(B64_CHARS[(n >> 18) as usize & 63] as char);
        out.push(B64_CHARS[(n >> 12) as usize & 63] as char);
        if chunk.len() > 1 {
            out.push(B64_CHARS[(n >> 6) as usize & 63] as char);
        }
        if chunk.len() > 2 {
            out.push(B64_CHARS[n as usize & 63] as char);
        }
    }
    out
}

/// Decode base64 (standard or url alphabet, padding optional) — used for
/// HTTP Basic credentials as well as our own tokens.
pub fn b64_decode(s: &str) -> Option<Vec<u8>> {
    unb64(s.trim_end_matches('='))
}

fn unb64(s: &str) -> Option<Vec<u8>> {
    let value = |c: u8| -> Option<u32> {
        match c {
            b'+' => Some(62),
            b'/' => Some(63),
            _ => B64_CHARS.iter().position(|&x| x == c).map(|p| p as u32),
        }
    };
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len() * 3 / 4);
    for chunk in bytes.chunks(4) {
        if chunk.len() < 2 {
            return None;
        }
        let mut n = 0u32;
        for (i, &c) in chunk.iter().enumerate() {
            n |= value(c)? << (18 - 6 * i);
        }
        out.push((n >> 16) as u8);
        if chunk.len() > 2 {
            out.push((n >> 8) as u8);
        }
        if chunk.len() > 3 {
            out.push(n as u8);
        }
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn password_roundtrip() {
        let hash = hash_password("correct horse battery staple").unwrap();
        assert!(hash.starts_with("pbkdf2-sha256$600000$"));
        assert!(verify_password("correct horse battery staple", &hash));
        assert!(!verify_password("wrong", &hash));
        assert!(!verify_password("correct horse battery staple", "garbage"));
    }

    #[test]
    fn b64_roundtrip() {
        for data in [&b""[..], b"a", b"ab", b"abc", b"abcd", &[0xff, 0x00, 0x7f]] {
            assert_eq!(unb64(&b64(data)).unwrap(), data);
        }
    }

    #[test]
    fn tokens_are_unique_and_digestable() {
        let a = generate_token().unwrap();
        let b = generate_token().unwrap();
        assert_ne!(a, b);
        assert_eq!(token_digest(&a).len(), 64);
    }
}

#[cfg(test)]
mod dummy_hash_test {
    use super::*;
    use std::time::Instant;

    #[test]
    fn dummy_hash_runs_full_verify() {
        // Must parse and run the full 600k iterations (return false), not
        // early-out on a malformed hash — otherwise the timing defense is
        // defeated. Compare its cost to a real hash's verify.
        let real = hash_password("some-password-value").unwrap();
        let dummy = dummy_password_hash();

        let t0 = Instant::now();
        assert!(!verify_password("guess", &dummy));
        let dummy_cost = t0.elapsed();

        let t1 = Instant::now();
        assert!(!verify_password("guess", &real));
        let real_cost = t1.elapsed();

        // Within 3x of each other means the dummy ran real PBKDF2 work.
        let ratio = dummy_cost.as_secs_f64() / real_cost.as_secs_f64().max(1e-9);
        assert!(ratio > 0.3 && ratio < 3.0, "dummy {dummy_cost:?} vs real {real_cost:?}");
    }
}
