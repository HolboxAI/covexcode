//! `covexcode login` / `covexcode logout` — device auth via covexcode.com.

use crate::config::Config;
use serde::Deserialize;
use std::path::PathBuf;
use std::process::Stdio;
use std::time::Duration;

const DEFAULT_AUTH_BASE: &str = "https://covexcode.com";

/// How long any one request may take.
///
/// Bounded on purpose: the TUI stands down for the whole of `login()` -- raw
/// mode off, nothing drawn, nothing reading the keyboard -- so a request that
/// never answers leaves the user in a terminal that looks hung, with the only
/// way out being to kill the process.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

/// How long the device-code wait may last in total.
///
/// The code expires server-side and says so (see the `GONE` and `expired`
/// answers in `login()`), but a server that goes quiet must not hold the
/// terminal hostage either way.
const MAX_WAIT: Duration = Duration::from_secs(300);

fn http_client() -> reqwest::Result<reqwest::Client> {
    reqwest::Client::builder().timeout(REQUEST_TIMEOUT).build()
}

fn is_promo_proxy(endpoint: &str) -> bool {
    let e = endpoint.to_lowercase();
    e.contains("llm.covexcode.com") || e.contains("llm.boxcode.sh")
}

fn auth_base() -> String {
    std::env::var("COVEXCODE_AUTH_URL")
        .or_else(|_| std::env::var("BOXCODE_AUTH_URL"))
        .or_else(|_| std::env::var("COVEXCODE_SITE_URL"))
        .or_else(|_| std::env::var("BOXCODE_SITE_URL"))
        .unwrap_or_else(|_| DEFAULT_AUTH_BASE.to_string())
        .trim_end_matches('/')
        .to_string()
}

fn covexcode_dir() -> PathBuf {
    let home = std::env::var_os("HOME")
        .or_else(|| std::env::var_os("USERPROFILE"))
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("."));
    home.join(".covexcode")
}

/// Appends a log line to ~/.covexcode/covexcode.log.
pub fn log_to_covexcode(msg: &str) {
    let dir = covexcode_dir();
    let _ = std::fs::create_dir_all(&dir);
    let path = dir.join("covexcode.log");
    if let Ok(mut file) = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
    {
        use std::io::Write;
        let secs = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        let _ = writeln!(file, "[{secs}] {msg}");
    }
}

fn account_token_path() -> PathBuf {
    covexcode_dir().join("account.token")
}

fn account_email_path() -> PathBuf {
    covexcode_dir().join("account.email")
}

/// Snapshot of the local covexcode.com device-login session (no network).
pub struct LoginStatus {
    pub has_session_token: bool,
    pub email: Option<String>,
    pub endpoint: String,
    pub model: String,
    pub key_prefix: String,
    pub via_promo_proxy: bool,
}

impl LoginStatus {
    pub fn load(config: &Config) -> Self {
        let email = std::fs::read_to_string(account_email_path())
            .ok()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty());
        let key = config.llm.api_key.trim();
        let key_prefix = if key.is_empty() {
            String::new()
        } else if key.len() > 12 {
            format!("{}…", &key[..12])
        } else {
            key.to_string()
        };
        let endpoint = config.llm.endpoint.trim().to_string();
        let via_promo_proxy = is_promo_proxy(&endpoint);
        Self {
            has_session_token: account_token_path().is_file(),
            email,
            endpoint,
            model: config.llm.model.clone(),
            key_prefix,
            via_promo_proxy,
        }
    }

    /// True when this machine has a promo key on llm.covexcode.com (or the
    /// legacy llm.boxcode.sh host) and a device session token.
    pub fn is_signed_in(&self) -> bool {
        self.has_session_token && self.via_promo_proxy && !self.key_prefix.is_empty()
    }

    /// Human-readable status for `/login` in the TUI.
    pub fn readout(&self) -> String {
        if self.is_signed_in() {
            let who = self.email.as_deref().unwrap_or("(email on next login)");
            format!(
                "Signed in via covexcode.com\n\n\
                 Account:  {who}\n\
                 Endpoint: {}\n\
                 Model:    {}\n\
                 Key:      {}\n\n\
                 Session:  ~/.covexcode/account.token\n\
                 Proof:    heartbeat hits https://covexcode.com/api/heartbeat\n\n\
                 To re-link: /logout, then /login (or exit and run `covexcode login`).",
                self.endpoint, self.model, self.key_prefix
            )
        } else if self.via_promo_proxy && !self.key_prefix.is_empty() {
            format!(
                "Using the promo LLM proxy, but no device session yet.\n\n\
                 Endpoint: {}\n\
                 Key:      {}\n\n\
                 Run /login to finish linking this machine in the browser.",
                self.endpoint, self.key_prefix
            )
        } else {
            "Not signed in via covexcode.com.\n\n\
             Run /login — the browser opens https://covexcode.com for Google\n\
             sign-in, then this session receives your promo key.\n\
             Same file the IDE reads: ~/.covexcode/config.toml"
                .to_string()
        }
    }
}

fn write_account_email(email: &str) {
    let path = account_email_path();
    if let Some(parent) = path.parent() {
        let _ = std::fs::create_dir_all(parent);
    }
    let _ = std::fs::write(&path, email.trim());
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600));
    }
}

#[derive(Debug, Deserialize)]
struct StartResponse {
    device_code: String,
    user_code: String,
    verification_uri: String,
    verification_uri_complete: Option<String>,
    interval: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct PollResponse {
    status: Option<String>,
    endpoint: Option<String>,
    model: Option<String>,
    api_key: Option<String>,
    session_token: Option<String>,
    email: Option<String>,
    error: Option<String>,
}

/// The command that hands `url` to the desktop's default browser, or `None` on
/// a platform with no such convention.
fn browser_command(url: &str) -> Option<std::process::Command> {
    #[cfg(target_os = "macos")]
    {
        let mut cmd = std::process::Command::new("open");
        cmd.arg(url);
        Some(cmd)
    }
    #[cfg(target_os = "linux")]
    {
        let mut cmd = std::process::Command::new("xdg-open");
        cmd.arg(url);
        Some(cmd)
    }
    #[cfg(target_os = "windows")]
    {
        let mut cmd = std::process::Command::new("cmd");
        cmd.args(["/C", "start", "", url]);
        Some(cmd)
    }
    #[cfg(not(any(target_os = "macos", target_os = "linux", target_os = "windows")))]
    {
        let _ = url;
        None
    }
}

/// Open the browser, without letting it near this terminal.
///
/// Detached, not waited on: `open` blocks until the handler exits on some
/// systems, and this is called from the event loop while the whole UI is stood
/// down. Both streams go to `/dev/null` -- inherited ones would print the
/// browser's own chatter into a terminal this app is about to redraw on, which
/// is half of what made `/login` leave a mess behind.
fn open_browser(url: &str) {
    let Some(mut command) = browser_command(url) else {
        return;
    };
    // Reaped on its own thread so a handler that keeps running does not leave a
    // zombie for the rest of the session.
    if let Ok(mut child) = command
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
    {
        std::thread::spawn(move || {
            let _ = child.wait();
        });
    }
    // A browser that will not open is not worth failing the login over: the URL
    // is printed above, so the worst case is opening it by hand.
}

pub async fn login() -> Result<(), Box<dyn std::error::Error>> {
    let base = auth_base();
    let client = http_client()?;

    let start: StartResponse = client
        .post(format!("{base}/api/auth/device/start"))
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;

    let verify = start
        .verification_uri_complete
        .unwrap_or_else(|| format!("{}?code={}", start.verification_uri, start.user_code));

    log_to_covexcode("covexcode login: opening browser for device authorization");
    open_browser(&verify);

    let interval = Duration::from_secs(start.interval.unwrap_or(2).max(1));
    // Bounded even though the code itself expires: see `MAX_WAIT`.
    let deadline = tokio::time::Instant::now() + MAX_WAIT;
    loop {
        if tokio::time::Instant::now() >= deadline {
            return Err(
                "timed out waiting for the browser to authorize -- run /login again".into(),
            );
        }
        tokio::time::sleep(interval).await;

        let res = client
            .post(format!("{base}/api/auth/device/poll"))
            .json(&serde_json::json!({ "device_code": start.device_code }))
            .send()
            .await?;

        if res.status() == reqwest::StatusCode::GONE {
            return Err("device code expired — run covexcode login again".into());
        }

        let body: PollResponse = res.json().await?;
        if body.api_key.is_some() {
            let mut config = Config::load().unwrap_or_default();
            if let Some(endpoint) = body.endpoint {
                config.llm.endpoint = endpoint.trim_end_matches('/').to_string();
            }
            if let Some(model) = body.model {
                config.llm.model = model;
            }
            if let Some(api_key) = body.api_key {
                config.llm.api_key = api_key;
            }
            config.llm.provider = "deepseek".to_string();
            config.save()?;

            if let Some(token) = body.session_token {
                let path = account_token_path();
                if let Some(parent) = path.parent() {
                    let _ = std::fs::create_dir_all(parent);
                }
                std::fs::write(&path, token.trim())?;
                #[cfg(unix)]
                {
                    use std::os::unix::fs::PermissionsExt;
                    let _ = std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600));
                }
            }
            if let Some(ref email) = body.email {
                write_account_email(email);
            }

            log_to_covexcode(&format!(
                "✓ Signed in as {}. Credentials saved to ~/.covexcode/config.toml",
                body.email.unwrap_or_else(|| "you".into())
            ));
            // Immediate heartbeat so DAU counts this login.
            heartbeat_once().await;
            return Ok(());
        }

        match body.status.as_deref() {
            Some("pending") | None => continue,
            Some("expired") => {
                return Err("device code expired — run covexcode login again".into());
            }
            _ => {
                if let Some(err) = body.error {
                    return Err(err.into());
                }
                continue;
            }
        }
    }
}

pub async fn logout() -> Result<(), Box<dyn std::error::Error>> {
    let msg = logout_local()?;
    println!("{msg}");
    Ok(())
}

/// Clear device session (+ promo key when pointed at the promo LLM proxy).
/// Used by both `covexcode logout` and the `/logout` slash command.
pub fn logout_local() -> Result<String, Box<dyn std::error::Error>> {
    let token_path = account_token_path();
    if token_path.exists() {
        let _ = std::fs::remove_file(&token_path);
    }
    let email_path = account_email_path();
    if email_path.exists() {
        let _ = std::fs::remove_file(&email_path);
    }
    let mut config = Config::load().unwrap_or_default();
    let endpoint = config.llm.endpoint.to_lowercase();
    if is_promo_proxy(&endpoint) {
        config.llm.api_key.clear();
        config.save()?;
        Ok("✓ Cleared promo LLM credentials from ~/.covexcode/config.toml".into())
    } else {
        Ok("✓ Cleared local session token (left custom endpoint/api_key untouched)".into())
    }
}

/// Fire-and-forget heartbeat while the TUI is running.
pub async fn heartbeat_once() {
    let Ok(token) = std::fs::read_to_string(account_token_path()) else {
        return;
    };
    let token = token.trim();
    if token.is_empty() {
        return;
    }
    let base = auth_base();
    let Ok(client) = http_client() else {
        return;
    };
    let _ = client
        .post(format!("{base}/api/heartbeat"))
        .bearer_auth(token)
        .json(&serde_json::json!({ "client": "cli" }))
        .send()
        .await;
}

/// Spawn a background task that heartbeats every five minutes.
pub fn spawn_heartbeat_loop() {
    tokio::spawn(async {
        loop {
            heartbeat_once().await;
            tokio::time::sleep(Duration::from_secs(300)).await;
        }
    });
}
