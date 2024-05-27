use jwt_simple::prelude::*;

use super::super::bindings::supabase::wrappers::jwt;
use super::FdwHost;

impl jwt::Host for FdwHost {
    fn encode(
        &mut self,
        payload: Vec<(String, String)>,
        algo: String,
        key: String,
        ttl_hours: u32,
    ) -> jwt::JwtResult {
        let mut claims = Claims::create(Duration::from_hours(ttl_hours as u64));
        for (claim, value) in payload {
            match claim.as_str() {
                "iss" => {
                    claims = claims.with_issuer(value);
                }
                "sub" => {
                    claims = claims.with_subject(value);
                }
                _ => return Err(format!("claim {} not implemented", claim)),
            }
        }

        match algo.as_str() {
            "RS256" => RS256KeyPair::from_pem(&key),
            _ => return Err(format!("algorithm {} not implemented", algo)),
        }
        .and_then(|keypair| keypair.sign(claims))
        .map_err(|e| e.to_string())
    }
}
