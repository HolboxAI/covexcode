//! `/feedback` — the one place user-typed text leaves this machine.
//!
//! The payload is deliberately minimal: the message, the app version, the OS,
//! and the same anonymous per-install id telemetry already uses. Nothing else
//! is sent -- no email, API key, model, conversation, or paths -- and nothing
//! else may be added here. The telemetry opt-out is respected by `is_opted_out`
//! so a user who has already asked for no outbound calls never sends one here.

use std::time::Duration;

/// Where the CLI sends feedback. A constant rather than a config knob on
/// purpose: there is no per-install override to test against, and the one
/// switch that matters (opt-out) is `COVEXCODE_TELEMETRY_URL`, below.
const FEEDBACK_URL: &str = "https://covexcode.com/api/feedback";

/// How long the POST may take. Fire-and-forget, so this is a hard ceiling that
/// must never leave the user wondering -- it is also why nothing here is ever
/// awaited on the input path.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

/// The most a message may contain, in Unicode scalar values. Enforced at the
/// input box in `app.rs`; the counter in `ui.rs` reads the same number so the
/// cap and the display cannot drift.
pub const MAX_CHARS: usize = 2000;

/// The `os` field's value, chosen at compile time so the field stays exactly
/// `macos` / `linux` / `windows` on each of the three platforms this CLI ships
/// for. The `not(...)` arm keeps an unusual target building rather than failing
/// on an undefined constant.
pub fn os_name() -> &'static str {
    #[cfg(target_os = "macos")]
    {
        "macos"
    }
    #[cfg(target_os = "linux")]
    {
        "linux"
    }
    #[cfg(target_os = "windows")]
    {
        "windows"
    }
    #[cfg(not(any(target_os = "macos", target_os = "linux", target_os = "windows")))]
    {
        std::env::consts::OS
    }
}

/// Whether the user has opted out of outbound calls. Blank ("") and "off" (any
/// case) both count as opt-out, matching the convention documented in
/// `telemetry.rs`; an unset variable means the default (not opted out), and any
/// other value -- including a custom telemetry URL -- leaves feedback enabled.
pub fn is_opted_out() -> bool {
    let value = std::env::var("COVEXCODE_TELEMETRY_URL")
        .or_else(|_| std::env::var("BOXCODE_TELEMETRY_URL"));
    opted_out_given(value.ok().as_deref())
}

/// The decision behind `is_opted_out`, taking the env value as a plain
/// `Option<&str>` so it is testable without mutating process-global state.
fn opted_out_given(env_override: Option<&str>) -> bool {
    match env_override {
        Some(v) => {
            let t = v.trim();
            t.is_empty() || t.eq_ignore_ascii_case("off")
        }
        None => false,
    }
}

fn feedback_log_path() -> std::path::PathBuf {
    // `$HOME`-based, matching `login.rs::covexcode_dir`, not the
    // override-aware `Config::config_dir()`: the transcript names the file as
    // `~/.covexcode/feedback.log`, and a `COVEXCODE_CONFIG_DIR` override (set by
    // the IDE) must not move it to a directory that string no longer describes.
    let home = std::env::var_os("HOME")
        .or_else(|| std::env::var_os("USERPROFILE"))
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| std::path::PathBuf::from("."));
    home.join(".covexcode").join("feedback.log")
}

/// Appends the message to `~/.covexcode/feedback.log` *before* anything is
/// sent, so a failed network call cannot lose it. Best-effort by design: a
/// read-only `$HOME` must not turn feedback into an error the user notices.
pub fn log(message: &str) {
    use std::io::Write;
    let path = feedback_log_path();
    if let Some(parent) = path.parent() {
        let _ = std::fs::create_dir_all(parent);
    }
    let stamp = crate::dateutil::now_utc_timestamp();
    if let Ok(mut file) = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
    {
        let _ = writeln!(file, "[{stamp}] {message}");
    }
}

/// The JSON body that goes on the wire, exactly four keys and nothing else.
/// Split out of `send` so the payload shape is testable without a network.
pub fn build_body(message: &str, version: &str) -> serde_json::Value {
    serde_json::json!({
        "message": message,
        "version": version,
        "os": os_name(),
        "anon_id": crate::telemetry::anon_id().unwrap_or_default(),
    })
}

/// POST the message to the feedback endpoint. Fire-and-forget from the caller's
/// point of view: the runtime loop spawns this and never awaits it on the UI
/// path. Returns `Err` with a human reason so the transcript can say what
/// happened (the message is already on disk in `log` either way).
pub async fn send(message: &str, version: &str) -> Result<(), String> {
    let client = reqwest::Client::builder()
        .timeout(REQUEST_TIMEOUT)
        .user_agent(format!("covexcode/{version}"))
        .build()
        .map_err(|e| format!("could not build an HTTP client: {e}"))?;

    let body = build_body(message, version).to_string();

    let response = client
        .post(FEEDBACK_URL)
        .header("content-type", "application/json")
        .body(body)
        .send()
        .await
        .map_err(|e| format!("could not reach the feedback service: {e}"))?;

    let status = response.status();
    if !status.is_success() {
        return Err(format!("the feedback service refused this ({status})"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_char_cap_is_exactly_two_thousand() {
        assert_eq!(MAX_CHARS, 2000);
    }

    #[test]
    fn only_blank_or_off_counts_as_opted_out() {
        assert!(!opted_out_given(None));
        assert!(opted_out_given(Some("")));
        assert!(opted_out_given(Some("off")));
        assert!(opted_out_given(Some("OFF")));
        assert!(opted_out_given(Some("  off  ")));
        assert!(!opted_out_given(Some("https://example.com")));
    }

    #[test]
    fn the_payload_has_exactly_four_keys_and_nothing_else() {
        crate::config::test_support::with_isolated_home(|| {
            let body = build_body("hello", "1.2.3");
            let object = body.as_object().expect("the payload is a JSON object");
            let mut keys: Vec<&str> = object.keys().map(|k| k.as_str()).collect();
            keys.sort();
            assert_eq!(keys, vec!["anon_id", "message", "os", "version"]);
            assert_eq!(object["message"], "hello");
            assert_eq!(object["version"], "1.2.3");
        });
    }
}
