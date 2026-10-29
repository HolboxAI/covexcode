/// Static registry backing the `/provider` and `/model` overlays. A "Custom
/// endpoint..." escape hatch lives alongside this list in app.rs so the tool's
/// "any OpenAI-compatible endpoint" generality is never limited to this table.
///
/// `Serialize` backs `covexcode --providers-json` (see main.rs), so an ACP
/// client (boxcode-ide) can build its own provider picker from this same
/// registry instead of hand-copying it into TypeScript, where it would rot
/// exactly like the old `deepseek-chat` placeholder already did once.
#[derive(serde::Serialize)]
pub struct Provider {
    pub id: &'static str,
    pub label: &'static str,
    pub endpoint: &'static str,
    pub models: &'static [&'static str],
    /// Whether this provider's models accept image attachments (OpenAI vision
    /// content blocks). DeepSeek's current lineup is text-only; sending an
    /// image to it is not "unsupported but harmless" -- the endpoint accepts
    /// the request and the model answers as though the image were text it
    /// cannot read, which surfaces to the user as a garbled line.
    pub vision: bool,
}

pub const PROVIDERS: &[Provider] = &[
    Provider {
        id: "deepseek",
        label: "DeepSeek",
        endpoint: "https://api.deepseek.com",
        // deepseek-chat / deepseek-reasoner were retired 2026-07-24; current
        // lineup is v4-pro (flagship) / v4-flash (faster, cheaper).
        // v4-flash supports vision; model-level gating is handled in supports_images.
        models: &["deepseek-v4-pro", "deepseek-v4-flash"],
        vision: true,
    },
    Provider {
        id: "openai",
        label: "OpenAI",
        endpoint: "https://api.openai.com",
        // gpt-4o / gpt-4-turbo / gpt-3.5-turbo are deprecated; current lineup
        // is the GPT-5.6 family: sol (frontier), terra (balanced), luna (cost).
        models: &["gpt-5.6-sol", "gpt-5.6-terra", "gpt-5.6-luna"],
        vision: true,
    },
];

pub fn find_provider(id: &str) -> Option<&'static Provider> {
    PROVIDERS.iter().find(|p| p.id == id)
}

/// Whether the active provider and model can read images.
/// DeepSeek's v4-flash supports vision inputs, whereas v4-pro is text-only.
/// Unknown/custom endpoints and OpenAI models are assumed capable.
pub fn supports_images(provider_id: &str, model: &str) -> bool {
    if provider_id == "deepseek" {
        return model.contains("flash");
    }
    find_provider(provider_id).map_or(true, |p| p.vision)
}

/// The conventional env var a user is likely to already have exported for this
/// provider, e.g. "deepseek" -> "DEEPSEEK_API_KEY".
pub fn env_var_name(provider_id: &str) -> String {
    format!("{}_API_KEY", provider_id.to_uppercase())
}

/// The temperature a provider gets when `config.toml` has not set one
/// explicitly (see `LlmConfig::effective_temperature`).
///
/// `None` for every provider except DeepSeek: leaving the field off the
/// request entirely is the safe, unobtrusive default, since it lets each
/// endpoint's own default stand rather than second-guessing providers this
/// tool has no specific reason to disagree with. DeepSeek is the one
/// exception on record -- its models are used here mainly for agentic coding
/// work, where a wandering, non-zero default sampling temperature measurably
/// hurts tool-call reliability, so `0.0` (deterministic) is sent explicitly
/// rather than left to chance.
pub fn default_temperature(provider_id: &str) -> Option<f32> {
    match provider_id {
        "deepseek" => Some(0.0),
        _ => None,
    }
}

/// The model context window, in tokens, reported to the IDE as
/// `usage_update.size` so the cost/usage meter can show occupancy ("N of M").
/// `0` means "unknown" -- the client then shows a bare token count without the
/// "of M" half rather than asserting a limit covexcode cannot verify.
///
/// Only DeepSeek is on record: its current lineup exposes a 128k window, the
/// same assumption `CompactConfig::auto_at_tokens`'s default already bakes in.
/// Every other provider -- including a custom endpoint -- gets `0` rather than
/// a guessed number, which would make a "truthful" meter untruthful.
pub fn default_context_window(provider_id: &str) -> u64 {
    match provider_id {
        "deepseek" => 131_072,
        _ => 0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_provider_has_at_least_one_model() {
        for p in PROVIDERS {
            assert!(!p.models.is_empty(), "{} has no models", p.id);
        }
    }

    #[test]
    fn find_provider_looks_up_by_id() {
        assert_eq!(find_provider("deepseek").unwrap().label, "DeepSeek");
        assert!(find_provider("nonexistent").is_none());
    }

    #[test]
    fn env_var_name_uppercases_the_id() {
        assert_eq!(env_var_name("deepseek"), "DEEPSEEK_API_KEY");
        assert_eq!(env_var_name("openai"), "OPENAI_API_KEY");
    }

    /// `covexcode --providers-json`'s whole reason to exist: an ACP client reads
    /// this instead of hand-copying the table, so it must actually round-trip
    /// as valid, structured JSON with the fields a picker needs.
    #[test]
    fn the_registry_serializes_to_valid_parseable_json() {
        let json = serde_json::to_string(PROVIDERS).expect("PROVIDERS must serialize");
        let parsed: serde_json::Value = serde_json::from_str(&json).expect("must be valid JSON");
        let arr = parsed.as_array().expect("top level must be an array");
        assert_eq!(arr.len(), PROVIDERS.len());

        let deepseek = arr
            .iter()
            .find(|p| p["id"] == "deepseek")
            .expect("deepseek must be present");
        assert_eq!(deepseek["label"], "DeepSeek");
        assert_eq!(deepseek["endpoint"], "https://api.deepseek.com");
        assert!(deepseek["models"]
            .as_array()
            .unwrap()
            .contains(&serde_json::json!("deepseek-v4-pro")));
    }

    /// DeepSeek is the one provider with a specific, on-record reason to
    /// override the endpoint's own default: agentic tool-call reliability.
    /// Everyone else, including an unrecognised or custom endpoint, gets
    /// `None` so the field is simply not sent.
    #[test]
    fn only_deepseek_gets_a_built_in_temperature_default() {
        assert_eq!(default_temperature("deepseek"), Some(0.0));
        assert_eq!(default_temperature("openai"), None);
        assert_eq!(default_temperature(""), None);
        assert_eq!(default_temperature("some-custom-endpoint"), None);
    }

    #[test]
    fn deepseek_flash_reads_images_and_pro_is_text_only() {
        assert!(!supports_images("deepseek", "deepseek-v4-pro"));
        assert!(supports_images("deepseek", "deepseek-v4-flash"));
        assert!(supports_images("openai", "gpt-5.6-sol"));
        // Unknown/custom endpoints are assumed capable rather than silently
        // stripped of a possibly-valid image.
        assert!(supports_images("", "any-model"));
        assert!(supports_images("some-custom-endpoint", "my-vision-model"));
    }
}
