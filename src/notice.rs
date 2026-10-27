//! Telling one kind of failure from another.
//!
//! Every failure used to render identically: the word `Error:` in red, then a
//! paragraph. "You have used today's allowance" and "the endpoint is
//! unreachable" looked the same, so the reader had to parse prose to work out
//! whether anything was actually broken and whether they could do something
//! about it.
//!
//! The axis that matters is not severity, it is **agency**:
//!
//! - Amber: expected, and there is something you can do -- a spent allowance,
//!   a full context window, a truncated reply.
//! - Red: something is wrong that you did not cause -- a rejected key, an
//!   unreachable endpoint, a provider fault.
//!
//! Classification works off markers this crate controls (see `markers`), rather
//! than guessing from free prose. The tests feed the real strings the app
//! produces through `classify`, so a reworded message that stops matching fails
//! the build instead of silently going grey.

use crate::theme;
use ratatui::style::{Modifier, Style};

/// Substrings that classification keys on. Constants rather than literals at
/// the match site, so the producer and the classifier are edited together.
pub mod markers {
    /// What providers call a conversation that no longer fits.
    pub const CONTEXT_EXCEEDED: &str = "context_length_exceeded";
    pub const CONTEXT_MAXIMUM: &str = "maximum context length";
    pub const CONTEXT_TOO_LONG: &str = "too many tokens";

    /// Produced by this crate.
    pub const LOCAL_LIMIT: &str = "Daily limit reached";
    pub const TRUNCATED: &str = "output cap and was cut off";
    pub const UNREACHABLE: &str = "Could not reach";
    pub const CONTEXT_FULL_LOCAL: &str = "conversation is too long";
    pub const CONTEXT_TOO_LARGE_LOCAL: &str = "conversation is too large";
    pub const OUT_OF_CREDIT: &str = "out of credit";

    /// What the proxy calls a spent account. The type is OpenAI's standard
    /// `insufficient_quota`; the message distinguishes the lifetime cap (this
    /// is the one that means "add credits") from the daily server cap.
    pub const INSUFFICIENT_QUOTA: &str = "insufficient_quota";
    pub const LIFETIME_SPEND_LIMIT: &str = "lifetime spend limit";
    pub const DAILY_SPEND_LIMIT: &str = "daily spend limit";
}

/// What kind of thing went wrong.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind {
    /// This user's own `[quota]` allowance for the day is spent.
    DailyLimit,
    /// The conversation no longer fits in the model's context window.
    ContextFull,
    /// The reply hit the output cap mid-sentence.
    Truncated,
    /// A key or device token was rejected.
    Auth,
    /// The account's prepaid credit (or the server-side daily cap) is spent.
    /// Distinct from `DailyLimit`, which is this machine's own `[quota]`.
    OutOfCredit,
    /// Slow down.
    RateLimited,
    /// The endpoint could not be reached at all.
    Offline,
    /// The provider answered, with a failure.
    Provider,
    /// Anything not recognised. Rendered as a plain error rather than
    /// mislabelled as something specific.
    Other,
}

impl Kind {
    /// Amber for "expected, you can act on it"; red for "something is wrong".
    pub fn color(self) -> ratatui::style::Color {
        match self {
            Kind::DailyLimit
            | Kind::ContextFull
            | Kind::Truncated
            | Kind::OutOfCredit
            | Kind::RateLimited => theme::p().warning,
            Kind::Auth | Kind::Offline | Kind::Provider | Kind::Other => theme::p().danger,
        }
    }

    pub fn style(self) -> Style {
        Style::default()
            .fg(self.color())
            .add_modifier(Modifier::BOLD)
    }

    /// A glyph, so the kind is legible before any text is read.
    pub fn icon(self) -> &'static str {
        match self {
            Kind::DailyLimit => "◔",
            Kind::ContextFull => "▣",
            Kind::Truncated => "✂",
            Kind::Auth => "⚿",
            Kind::OutOfCredit => "◈",
            Kind::RateLimited => "⏳",
            Kind::Offline => "⊘",
            Kind::Provider => "⚠",
            Kind::Other => "✗",
        }
    }

    /// The headline, replacing the undifferentiated `Error:`.
    pub fn headline(self) -> &'static str {
        match self {
            Kind::DailyLimit => "Daily limit reached",
            Kind::ContextFull => "Conversation too long",
            Kind::Truncated => "Reply cut off",
            Kind::Auth => "Not authorised",
            Kind::OutOfCredit => "Out of credit",
            Kind::RateLimited => "Rate limited",
            Kind::Offline => "Endpoint unreachable",
            Kind::Provider => "Provider error",
            Kind::Other => "Error",
        }
    }

    /// One line on what to do, shown under the detail.
    ///
    /// Only where there is a genuine next step. A hint that says nothing is
    /// worse than none, because it trains people to skip the line that
    /// sometimes matters.
    pub fn hint(self) -> Option<&'static str> {
        match self {
            Kind::DailyLimit => Some(
                "Resets at UTC midnight · /quota to see the numbers · /quota override to keep going",
            ),
            Kind::ContextFull => Some("/new starts a fresh conversation and clears the history"),
            Kind::Truncated => Some("Raise max_tokens under [llm], or ask for smaller pieces"),
            Kind::Auth => Some("/provider to set a working key"),
            // The detail already carries the account URL, in the same text the
            // IDE receives; repeating it here would print it twice in the TUI.
            Kind::OutOfCredit => None,
            Kind::RateLimited => Some("Wait a moment and send again"),
            Kind::Offline => Some("Check your connection, then /provider to confirm the endpoint"),
            Kind::Provider | Kind::Other => None,
        }
    }
}

/// Work out what a failure message is about.
///
/// Ordered most specific first: several of these bodies contain the words the
/// looser rules below match on, so a rule that fires early would file a
/// specific failure under a generic heading and offer the wrong remedy.
pub fn classify(text: &str) -> Kind {
    use markers as m;
    let lower = text.to_lowercase();

    // Before the generic 429/rate-limit rule: the proxy uses 429 for a spent
    // account as well, and telling someone to "wait a moment" when they need
    // to add credit is the failure this kind exists to prevent.
    if lower.contains(m::OUT_OF_CREDIT)
        || lower.contains(m::INSUFFICIENT_QUOTA)
        || lower.contains(m::LIFETIME_SPEND_LIMIT)
        || lower.contains(m::DAILY_SPEND_LIMIT)
    {
        return Kind::OutOfCredit;
    }
    if text.contains(m::LOCAL_LIMIT) {
        return Kind::DailyLimit;
    }
    if text.contains(m::CONTEXT_EXCEEDED)
        || lower.contains(m::CONTEXT_MAXIMUM)
        || lower.contains(m::CONTEXT_TOO_LONG)
        || lower.contains(m::CONTEXT_FULL_LOCAL)
        || lower.contains(m::CONTEXT_TOO_LARGE_LOCAL)
    {
        return Kind::ContextFull;
    }
    if lower.contains(m::TRUNCATED) {
        return Kind::Truncated;
    }
    if lower.contains("invalid api key") || lower.contains("http 401") || lower.contains("http 403")
    {
        return Kind::Auth;
    }
    if lower.contains("http 429")
        || lower.contains("rate limit")
        || lower.contains("rate_limit")
    {
        return Kind::RateLimited;
    }
    if text.contains(m::UNREACHABLE) || lower.contains("connection") || lower.contains("dns") {
        return Kind::Offline;
    }
    if lower.contains("http ") || lower.contains("provider returned") {
        return Kind::Provider;
    }
    Kind::Other
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The messages this crate actually produces, verbatim. If one is reworded
    /// so it stops classifying, this fails rather than the notice silently
    /// degrading to a grey "Error".
    #[test]
    fn the_real_messages_this_app_produces_are_classified() {
        let cases: &[(&str, Kind)] = &[
            // quota.rs
            (
                "Daily limit reached — requests 5 of 5. Resets in 3h 2m (UTC midnight).\n\
                 Type /quota override to keep going today",
                Kind::DailyLimit,
            ),
            // llm.rs truncation notice
            (
                "The reply hit the 16384-token output cap and was cut off. Raise `max_tokens`.",
                Kind::Truncated,
            ),
            // llm.rs, a key the provider rejected
            (
                "HTTP 401 Unauthorized from https://api.deepseek.com/v1/chat/completions",
                Kind::Auth,
            ),
            // llm.rs, provider rate limit
            (
                "Too many requests.\n\nThis is a rate limit, not a fault -- wait a moment and try again.",
                Kind::RateLimited,
            ),
            // llm.rs connectivity
            (
                "Could not reach http://localhost:8000/v1/chat/completions: error trying to connect",
                Kind::Offline,
            ),
            // llm.rs, the proxy's lifetime cap. The real body is JSON, but the
            // classifier also has to recognise the rendered message.
            (
                "Out of credit.\n\nlifetime spend limit reached\n\n\
                 Add credits at https://covexcode.com/account, then send the message again.",
                Kind::OutOfCredit,
            ),
        ];
        for (text, expected) in cases {
            assert_eq!(classify(text), *expected, "misclassified: {text}");
        }
    }

    /// The proxy answers a spent account with HTTP 429, the same status as a
    /// real rate limit. The body is the only thing that tells them apart, and
    /// the remedies are opposites: wait vs add credit.
    #[test]
    fn an_insufficient_quota_429_is_not_filed_as_a_rate_limit() {
        for body in [
            r#"{"error":{"message":"lifetime spend limit reached","type":"insufficient_quota"}}"#,
            r#"{"error":{"message":"daily spend limit reached","type":"insufficient_quota"}}"#,
            "Daily spend limit reached",
        ] {
            assert_eq!(classify(body), Kind::OutOfCredit, "{body}");
        }
        assert_eq!(
            classify(r#"{"error":{"message":"slow down","type":"rate_limit_exceeded"}}"#),
            Kind::RateLimited
        );
    }

    #[test]
    fn context_length_is_recognised_in_the_shapes_providers_use() {
        for text in [
            r#"{"error":{"code":"context_length_exceeded","message":"..."}}"#,
            "This model's maximum context length is 65536 tokens",
            "the request contains too many tokens",
            "This conversation is too long for the model's context window.",
            "This conversation is too large for deepseek-v4-flash's 131072-token context \
             window (estimated ~131494 tokens including the 32768-token reply budget). \
             Start a new chat or run /compact.",
        ] {
            assert_eq!(classify(text), Kind::ContextFull, "{text}");
        }
    }

    #[test]
    fn something_unrecognised_stays_a_plain_error_rather_than_being_mislabelled() {
        assert_eq!(classify("something went sideways"), Kind::Other);
        assert_eq!(Kind::Other.headline(), "Error");
        assert!(Kind::Other.hint().is_none());
    }

    /// Amber means "you can act on this"; red means "something is wrong".
    /// Getting these the wrong way round is what makes a wall of red
    /// unreadable.
    #[test]
    fn actionable_kinds_are_amber_and_faults_are_red() {
        for k in [
            Kind::DailyLimit,
            Kind::ContextFull,
            Kind::Truncated,
            Kind::OutOfCredit,
            Kind::RateLimited,
        ] {
            assert_eq!(
                k.color(),
                theme::p().warning,
                "{k:?} is expected, not a fault"
            );
        }
        for k in [Kind::Auth, Kind::Offline, Kind::Provider, Kind::Other] {
            assert_eq!(k.color(), theme::p().danger, "{k:?} is a fault");
        }
    }

    #[test]
    fn every_kind_has_a_distinct_icon_and_headline() {
        let all = [
            Kind::DailyLimit,
            Kind::ContextFull,
            Kind::Truncated,
            Kind::Auth,
            Kind::OutOfCredit,
            Kind::RateLimited,
            Kind::Offline,
            Kind::Provider,
            Kind::Other,
        ];
        let icons: std::collections::HashSet<_> = all.iter().map(|k| k.icon()).collect();
        let heads: std::collections::HashSet<_> = all.iter().map(|k| k.headline()).collect();
        assert_eq!(icons.len(), all.len(), "icons must be distinguishable");
        assert_eq!(heads.len(), all.len(), "headlines must be distinguishable");
    }

    /// A hint that says nothing is worse than none: it trains people to skip
    /// the line that sometimes matters.
    #[test]
    fn hints_name_a_real_next_step() {
        for k in [
            Kind::DailyLimit,
            Kind::ContextFull,
            Kind::Truncated,
            Kind::Auth,
        ] {
            let hint = k.hint().expect("should have a hint");
            assert!(
                hint.contains('/') || hint.contains("max_tokens") || hint.contains("Wait"),
                "{k:?} hint names no action: {hint}"
            );
        }
    }
}
