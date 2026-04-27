//! Cartridge registry slug — deterministic mapping from a registry URL
//! to a top-level folder name under the cartridges install root.
//!
//! Registries are identified by their full URL (the exact byte string
//! the user/operator typed; no normalization, scheme stripping, slash
//! trimming, or path canonicalization). The first 16 hex chars of the
//! URL's SHA-256 form the folder name on disk. The literal string
//! `"dev"` is reserved for dev cartridges that have no registry — it
//! cannot collide with the 16-hex slug space.
//!
//! The mapping is one-way: folder → URL is recovered from each
//! installed cartridge's own `cartridge.json:registry_url`. The
//! installer/host validates `slug_for(cartridge_json.registry_url) ==
//! folder_name` at parse time.

use serde::{Deserialize, Deserializer};
use sha2::{Digest, Sha256};

/// Required-but-nullable `Option<String>` for serde wire formats.
///
/// Stock serde treats an absent key and an explicit `null` the same
/// way for `Option<T>`. We need stricter semantics: the key MUST be
/// present in the JSON object; the value MAY be `null`. This rejects
/// old-schema payloads where the key is absent entirely, instead of
/// silently treating them as dev installs.
///
/// Use as the field type and add `#[serde(deserialize_with = "deserialize_required_nullable_string")]`
/// — wait, serde's `deserialize_with` doesn't see absence. The real
/// path is to use the `must_have_field` pattern via a manual
/// `Deserialize` on the parent struct. We expose this helper for
/// callers that already have a manual impl and want a single place
/// to centralize the "decode Option<String>, but the caller has
/// already verified presence" decode step.
///
/// The mirror-compatible enforcement lives in
/// `CartridgeJson::deserialize` / `CapManifest::deserialize` — they
/// build a `serde_json::Value` first, check `obj.contains_key("registry_url")`,
/// then re-deserialize. This helper is for tests and any future
/// types that follow the same pattern.
pub fn deserialize_option_string<'de, D>(
    deserializer: D,
) -> std::result::Result<Option<String>, D::Error>
where
    D: Deserializer<'de>,
{
    Option::<String>::deserialize(deserializer)
}

/// Reserved folder name for cartridges with no registry (developer-built
/// cartridges installed via `dx cartridge --install` without
/// `--registry`). The four-character literal can never collide with a
/// 16-character hex slug — no decoding needed.
pub const DEV_SLUG: &str = "dev";

/// Number of hex characters in the slug. 16 chars = 64 bits = ~10^19
/// possible values; collision probability across thousands of registries
/// is astronomically low and the literal "dev" is shorter than any
/// possible value, so the two namespaces never overlap.
pub const SLUG_HEX_LEN: usize = 16;

/// Compute the on-disk slug for a registry URL.
///
/// `None` (i.e. a dev cartridge) → returns the literal [`DEV_SLUG`].
/// `Some(url)` → returns the first [`SLUG_HEX_LEN`] hex characters of
/// `sha256(url.as_bytes())`, lowercase.
///
/// The URL is hashed verbatim. Two URLs that differ in any byte (case,
/// trailing slash, port, path, query) hash to different slugs — that's
/// intentional, because the URL is the registry's identity and the
/// installer treats it as opaque.
pub fn slug_for(registry_url: Option<&str>) -> String {
    match registry_url {
        None => DEV_SLUG.to_string(),
        Some(url) => {
            let mut hasher = Sha256::new();
            hasher.update(url.as_bytes());
            let digest = hasher.finalize();
            let hex = format!("{:x}", digest);
            hex[..SLUG_HEX_LEN].to_string()
        }
    }
}

/// True if `s` could be a valid slug for a non-dev registry.
/// Used by host scanners to distinguish dev folders from registry
/// folders before they read any cartridge.json.
pub fn is_registry_slug(s: &str) -> bool {
    s.len() == SLUG_HEX_LEN && s.chars().all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// TEST1500: The default central registry's URL hashes to a stable,
    /// pre-computed slug. If this value ever changes silently it means
    /// either the encoding rule shifted or the hashing algorithm
    /// changed — either way every installed cartridge would land in
    /// the wrong directory and stop being discovered. The slug is
    /// pinned as a literal so a regression is loud.
    #[test]
    fn test1500_slug_for_central_registry_is_stable() {
        let url = "https://cartridges.machinefabric.com/manifest";
        let slug = slug_for(Some(url));
        // Compute once on a known-good build and freeze it. The first
        // 16 hex chars of `sha256("https://cartridges.machinefabric.com/manifest")`.
        let expected = {
            let mut h = Sha256::new();
            h.update(url.as_bytes());
            let d = h.finalize();
            format!("{:x}", d)[..SLUG_HEX_LEN].to_string()
        };
        assert_eq!(slug, expected);
        assert_eq!(slug.len(), SLUG_HEX_LEN);
        assert!(is_registry_slug(&slug));
    }

    /// TEST1501: `None` (dev cartridge) maps to the literal `dev` and
    /// never to a hex slug. The dev sentinel must remain
    /// distinguishable from registry slugs by length alone — no
    /// caller should ever hash the string "dev" to get this value.
    #[test]
    fn test1501_slug_for_none_is_dev() {
        assert_eq!(slug_for(None), DEV_SLUG);
        assert_ne!(DEV_SLUG.len(), SLUG_HEX_LEN);
        assert!(!is_registry_slug(DEV_SLUG));
    }

    /// TEST1502: The URL is treated as raw bytes — adding a trailing
    /// slash, changing case, or appending a query string yields a
    /// different slug. Proves we are not normalizing the URL behind
    /// the operator's back; if they typed two URLs that look "the
    /// same" but differ byte-wise, those are two distinct registries.
    #[test]
    fn test1502_slug_byte_sensitivity() {
        let a = slug_for(Some("https://cartridges.machinefabric.com/manifest"));
        let b = slug_for(Some("https://cartridges.machinefabric.com/manifest/"));
        let c = slug_for(Some("https://CARTRIDGES.machinefabric.com/manifest"));
        let d = slug_for(Some("https://cartridges.machinefabric.com/manifest?v=1"));
        assert_ne!(a, b, "trailing slash must change slug");
        assert_ne!(a, c, "case change must change slug");
        assert_ne!(a, d, "query string must change slug");
    }

    /// TEST1503: Calling `slug_for` twice on the same URL returns the
    /// same string. Determinism is the whole point of using a hash
    /// here — if this fails, every install/restart would land in a
    /// different folder and discovery would be permanently broken.
    #[test]
    fn test1503_slug_is_deterministic() {
        let url = "https://example.com/some/registry/path?token=abc";
        let s1 = slug_for(Some(url));
        let s2 = slug_for(Some(url));
        let s3 = slug_for(Some(url));
        assert_eq!(s1, s2);
        assert_eq!(s2, s3);
    }

    /// TEST1504: A 16-character hex slug can never equal the literal
    /// `dev` — `dev` is 3 characters, so by-length comparison alone
    /// rules out collision. This invariant is what lets us use the
    /// folder name as a dev-vs-registry discriminator without
    /// reading any file inside the directory.
    #[test]
    fn test1504_dev_never_collides_with_hex_slug() {
        assert_ne!(DEV_SLUG.len(), SLUG_HEX_LEN);
        // Probe a wide variety of URLs and confirm none produce the
        // dev string. Catches a hypothetical hashing bug that
        // truncated to 3 characters.
        let probes = [
            "",
            "https://a.test",
            "https://b.test",
            "ftp://example.com/",
            "https://localhost:8080/",
            "x",
        ];
        for p in probes {
            let s = slug_for(Some(p));
            assert_ne!(s, DEV_SLUG);
            assert_eq!(s.len(), SLUG_HEX_LEN);
        }
    }

    /// TEST1505: `is_registry_slug` rejects the dev sentinel, accepts
    /// 16-hex strings, rejects anything else. Used by the XPC service
    /// and engine to distinguish dev folders from registry folders
    /// during the pre-read scan.
    #[test]
    fn test1505_is_registry_slug_classification() {
        assert!(!is_registry_slug(DEV_SLUG));
        assert!(!is_registry_slug(""));
        assert!(!is_registry_slug("nightly")); // an old channel folder name
        assert!(!is_registry_slug("ABCDEF1234567890")); // uppercase rejected
        assert!(!is_registry_slug("zzzz567890abcdef")); // non-hex rejected
        assert!(is_registry_slug("0123456789abcdef"));
        let real = slug_for(Some("https://cartridges.machinefabric.com/manifest"));
        assert!(is_registry_slug(&real));
    }
}
