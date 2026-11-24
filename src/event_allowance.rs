//! `/event` — read the managed proxy's event allowance and ask for more.
//!
//! The proxy enforces the window and the token tiers; this module only asks it
//! how much is left and turns the answer into a readout the TUI can print. It
//! is deliberately separate from `quota.rs`: that file is this machine's own
//! daily guard, while this is the server-side event cohort.

use serde::Deserialize;
use std::time::Duration;

const TIMEOUT: Duration = Duration::from_secs(10);
const DEFAULT_TIERS: [u64; 3] = [4_000_000, 8_000_000, 10_000_000];

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EventRequest {
    Status,
    RequestMore,
}

#[derive(Debug, Default, Deserialize)]
struct StatusBody {
    #[serde(default)]
    used: u64,
    #[serde(default)]
    cap: u64,
    #[serde(default)]
    tier: u32,
    #[serde(default)]
    extra_tokens: u64,
    #[serde(default)]
    remaining: u64,
    #[serde(default)]
    phase: String,
    #[serde(default)]
    grant_tokens: u64,
    #[serde(default)]
    tiers: Vec<u64>,
    #[serde(default)]
    window: Window,
}

#[derive(Debug, Default, Deserialize)]
struct Window {
    #[serde(default)]
    start: Option<String>,
    #[serde(default)]
    end: Option<String>,
}

#[derive(Debug, Deserialize)]
struct MessageBody {
    #[serde(default)]
    message: Option<String>,
    #[serde(default)]
    error: Option<ApiError>,
}

#[derive(Debug, Deserialize)]
struct ApiError {
    #[serde(default)]
    message: String,
}

/// The proxy serves `/v1/...`; the event instance is mounted at
/// `/event/v1/...`, so every endpoint is `${endpoint}/event/<suffix>`.
fn event_url(endpoint: &str, suffix: &str) -> String {
    format!("{}/event/{suffix}", endpoint.trim_end_matches('/'))
}

pub async fn run(
    request: EventRequest,
    endpoint: &str,
    api_key: &str,
) -> Result<String, String> {
    match request {
        EventRequest::Status => status(endpoint, api_key).await,
        EventRequest::RequestMore => request_more(endpoint, api_key).await,
    }
}

pub async fn status(endpoint: &str, api_key: &str) -> Result<String, String> {
    let url = event_url(endpoint, "status");
    let response = reqwest::Client::new()
        .get(&url)
        .bearer_auth(api_key)
        .timeout(TIMEOUT)
        .send()
        .await
        .map_err(|error| format!("Could not reach the event service: {error}"))?;
    let code = response.status().as_u16();
    let body = response.text().await.unwrap_or_default();
    if !(200..300).contains(&code) {
        return Err(api_error(&body, code));
    }
    let parsed: StatusBody = serde_json::from_str(&body)
        .map_err(|error| format!("The event service returned an unreadable answer: {error}"))?;
    Ok(render_status(&parsed))
}

pub async fn request_more(endpoint: &str, api_key: &str) -> Result<String, String> {
    let url = event_url(endpoint, "request-more");
    let response = reqwest::Client::new()
        .post(&url)
        .bearer_auth(api_key)
        .timeout(TIMEOUT)
        .send()
        .await
        .map_err(|error| format!("Could not reach the event service: {error}"))?;
    let code = response.status().as_u16();
    let body = response.text().await.unwrap_or_default();
    if !(200..300).contains(&code) {
        return Err(api_error(&body, code));
    }
    let parsed: MessageBody = serde_json::from_str(&body).unwrap_or(MessageBody {
        message: None,
        error: None,
    });
    Ok(parsed
        .message
        .filter(|message| !message.trim().is_empty())
        .unwrap_or_else(|| {
            "Request sent. Support has your key prefix and will reply by email.".to_string()
        }))
}

fn render_status(status: &StatusBody) -> String {
    let tiers: Vec<u64> = if status.tiers.is_empty() {
        DEFAULT_TIERS.to_vec()
    } else {
        status.tiers.clone()
    };
    let tier_total = tiers.len().max(1);
    let tier = (status.tier as usize + 1).min(tier_total);
    let phase = match status.phase.as_str() {
        "open" => "open",
        "not_started" => "not started",
        "ended" => "ended",
        other if !other.is_empty() => other,
        _ => "open",
    };
    let window = match (&status.window.start, &status.window.end) {
        (Some(start), Some(end)) => format!("{start} → {end}"),
        _ => "not reported".to_string(),
    };
    let mut text = format!(
        "Covexcode event — {phase}\n\n\
           Tokens:    {} of {}  (tier {tier} of {tier_total})\n\
           Remaining: {}\n\
           Grants:    {}\n\
           Window:    {window} (UTC)\n",
        crate::quota::thousands(status.used),
        crate::quota::thousands(status.cap),
        crate::quota::thousands(status.remaining),
        crate::quota::thousands(status.extra_tokens),
    );
    let grant = if status.grant_tokens > 0 {
        status.grant_tokens
    } else {
        2_000_000
    };
    if status.phase == "ended" {
        text.push_str("\nThe event has ended. Tokens are no longer usable.");
    } else if status.phase == "not_started" {
        text.push_str("\nThe event has not started yet.");
    } else if status.remaining == 0 {
        text.push_str(&format!(
            "\nAllowance used. Type /event request to ask support for {} more.",
            crate::quota::thousands(grant)
        ));
    } else {
        text.push_str(&format!(
            "\nAfter the allowance, /event request asks support for {} more.",
            crate::quota::thousands(grant)
        ));
    }
    text
}

fn api_error(body: &str, code: u16) -> String {
    if let Ok(parsed) = serde_json::from_str::<MessageBody>(body) {
        if let Some(error) = parsed.error {
            if !error.message.trim().is_empty() {
                return error.message;
            }
        }
        if let Some(message) = parsed.message {
            if !message.trim().is_empty() {
                return message;
            }
        }
    }
    let trimmed = body.trim();
    if trimmed.is_empty() {
        format!("The event service answered HTTP {code}.")
    } else {
        format!("The event service answered HTTP {code}: {}", clip(trimmed, 300))
    }
}

fn clip(text: &str, limit: usize) -> String {
    if text.chars().count() <= limit {
        return text.to_string();
    }
    let mut out: String = text.chars().take(limit).collect();
    out.push('…');
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_event_url_keeps_the_mount_prefix() {
        assert_eq!(
            event_url("https://llm.covexcode.com/event/v1", "status"),
            "https://llm.covexcode.com/event/v1/event/status"
        );
        assert_eq!(
            event_url("https://llm.covexcode.com/v1/", "request-more"),
            "https://llm.covexcode.com/v1/event/request-more"
        );
    }

    #[test]
    fn the_readout_names_usage_tier_window_and_the_next_step() {
        let status = StatusBody {
            used: 4_200_000,
            cap: 8_000_000,
            tier: 1,
            extra_tokens: 0,
            remaining: 3_800_000,
            phase: "open".to_string(),
            grant_tokens: 2_000_000,
            tiers: vec![4_000_000, 8_000_000, 10_000_000],
            window: Window {
                start: Some("2026-11-25T04:30:00Z".to_string()),
                end: Some("2026-11-25T11:30:00Z".to_string()),
            },
        };
        let text = render_status(&status);
        assert!(text.contains("4,200,000 of 8,000,000"), "{text}");
        assert!(text.contains("tier 2 of 3"), "{text}");
        assert!(text.contains("3,800,000"), "{text}");
        assert!(text.contains("2026-11-25T04:30:00Z"), "{text}");
        assert!(text.contains("/event request"), "{text}");
    }

    #[test]
    fn an_exhausted_readout_points_at_the_request_command() {
        let status = StatusBody {
            used: 10_000_000,
            cap: 10_000_000,
            tier: 2,
            remaining: 0,
            phase: "open".to_string(),
            grant_tokens: 2_000_000,
            ..Default::default()
        };
        let text = render_status(&status);
        assert!(text.contains("Allowance used"), "{text}");
        assert!(text.contains("2,000,000 more"), "{text}");
    }

    #[test]
    fn api_errors_prefer_the_openai_error_message() {
        assert_eq!(
            api_error(
                r#"{"error":{"message":"event allowance exhausted — 10,000,000 of 10,000,000 tokens used."}}"#,
                429
            ),
            "event allowance exhausted — 10,000,000 of 10,000,000 tokens used."
        );
        assert_eq!(api_error("", 404), "The event service answered HTTP 404.");
        assert!(api_error("not json", 500).contains("HTTP 500"));
    }
}
