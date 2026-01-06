//! MediaSpec parsing and handling
//!
//! Parses media_spec values in the format:
//! `content-type: <mime-type>; profile=<url>`
//!
//! Examples:
//! - `content-type: application/json; profile=https://capns.org/schema/document-outline`
//! - `content-type: image/png; profile=https://capns.org/schema/thumbnail-image`
//! - `content-type: text/plain; profile=https://capns.org/schema/utf8-text`

use std::fmt;

/// Parsed MediaSpec structure
#[derive(Debug, Clone, PartialEq)]
pub struct MediaSpec {
    /// The MIME content type (e.g., "application/json", "image/png")
    pub content_type: String,
    /// Optional profile URL
    pub profile: Option<String>,
}

impl MediaSpec {
    /// Parse a media_spec string
    /// Format: `content-type: <mime-type>; profile=<url>`
    pub fn parse(s: &str) -> Result<Self, MediaSpecError> {
        let s = s.trim();

        // Must start with "content-type:" (case-insensitive)
        let lower = s.to_lowercase();
        if !lower.starts_with("content-type:") {
            return Err(MediaSpecError::MissingContentType);
        }

        // Get everything after "content-type:"
        let after_prefix = &s[13..].trim_start();

        // Split by semicolon to separate mime type from parameters
        let parts: Vec<&str> = after_prefix.splitn(2, ';').collect();

        let content_type = parts[0].trim().to_string();
        if content_type.is_empty() {
            return Err(MediaSpecError::EmptyContentType);
        }

        // Parse profile if present
        let profile = if parts.len() > 1 {
            let params = parts[1].trim();
            MediaSpec::parse_profile(params)?
        } else {
            None
        };

        Ok(MediaSpec {
            content_type,
            profile,
        })
    }

    /// Parse profile parameter from params string
    fn parse_profile(params: &str) -> Result<Option<String>, MediaSpecError> {
        // Look for profile= (case-insensitive)
        let lower = params.to_lowercase();
        if let Some(pos) = lower.find("profile=") {
            let after_profile = &params[pos + 8..];

            // Handle quoted value
            let value = if after_profile.starts_with('"') {
                // Find closing quote
                let rest = &after_profile[1..];
                if let Some(end) = rest.find('"') {
                    rest[..end].to_string()
                } else {
                    return Err(MediaSpecError::UnterminatedQuote);
                }
            } else {
                // Unquoted value - take until semicolon or end
                after_profile.split(';').next().unwrap_or("").trim().to_string()
            };

            if value.is_empty() {
                Ok(None)
            } else {
                Ok(Some(value))
            }
        } else {
            Ok(None)
        }
    }

    /// Check if this media spec represents binary output
    pub fn is_binary(&self) -> bool {
        let ct = self.content_type.to_lowercase();

        // Binary content types
        ct.starts_with("image/")
            || ct.starts_with("audio/")
            || ct.starts_with("video/")
            || ct == "application/octet-stream"
            || ct == "application/pdf"
            || ct.starts_with("application/x-")
            || ct.contains("+zip")
            || ct.contains("+gzip")
    }

    /// Check if this media spec represents JSON output
    pub fn is_json(&self) -> bool {
        let ct = self.content_type.to_lowercase();
        ct == "application/json" || ct.ends_with("+json")
    }

    /// Check if this media spec represents text output
    pub fn is_text(&self) -> bool {
        let ct = self.content_type.to_lowercase();
        ct.starts_with("text/") || (!self.is_binary() && !self.is_json())
    }

    /// Get the primary type (e.g., "image" from "image/png")
    pub fn primary_type(&self) -> &str {
        self.content_type.split('/').next().unwrap_or(&self.content_type)
    }

    /// Get the subtype (e.g., "png" from "image/png")
    pub fn subtype(&self) -> Option<&str> {
        self.content_type.split('/').nth(1)
    }
}

impl fmt::Display for MediaSpec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "content-type: {}", self.content_type)?;
        if let Some(ref profile) = self.profile {
            write!(f, "; profile=\"{}\"", profile)?;
        }
        Ok(())
    }
}

/// Errors that can occur when parsing a MediaSpec
#[derive(Debug, Clone, PartialEq)]
pub enum MediaSpecError {
    MissingContentType,
    EmptyContentType,
    UnterminatedQuote,
}

impl fmt::Display for MediaSpecError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MediaSpecError::MissingContentType => {
                write!(f, "media_spec must start with 'content-type:'")
            }
            MediaSpecError::EmptyContentType => {
                write!(f, "content-type value cannot be empty")
            }
            MediaSpecError::UnterminatedQuote => {
                write!(f, "unterminated quote in profile value")
            }
        }
    }
}

impl std::error::Error for MediaSpecError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_json() {
        let spec = MediaSpec::parse("content-type: application/json; profile=\"https://capns.org/schema/outline\"").unwrap();
        assert_eq!(spec.content_type, "application/json");
        assert_eq!(spec.profile, Some("https://capns.org/schema/outline".to_string()));
        assert!(spec.is_json());
        assert!(!spec.is_binary());
    }

    #[test]
    fn test_parse_binary() {
        let spec = MediaSpec::parse("content-type: image/png; profile=\"https://capns.org/schema/thumbnail\"").unwrap();
        assert_eq!(spec.content_type, "image/png");
        assert!(spec.is_binary());
        assert!(!spec.is_json());
    }

    #[test]
    fn test_parse_text() {
        let spec = MediaSpec::parse("content-type: text/plain; profile=https://capns.org/schema/utf8-text").unwrap();
        assert_eq!(spec.content_type, "text/plain");
        assert!(spec.is_text());
        assert!(!spec.is_binary());
        assert!(!spec.is_json());
    }

    #[test]
    fn test_parse_no_profile() {
        let spec = MediaSpec::parse("content-type: text/html").unwrap();
        assert_eq!(spec.content_type, "text/html");
        assert!(spec.profile.is_none());
    }

    #[test]
    fn test_case_insensitive() {
        let spec = MediaSpec::parse("Content-Type: Application/JSON").unwrap();
        assert!(spec.is_json());
    }

    #[test]
    fn test_display() {
        let spec = MediaSpec {
            content_type: "application/json".to_string(),
            profile: Some("https://example.com/schema".to_string()),
        };
        assert_eq!(spec.to_string(), "content-type: application/json; profile=\"https://example.com/schema\"");
    }
}
