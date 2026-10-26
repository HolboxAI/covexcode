//! Compaction prompt and summary framing shared by the TUI (`App`) and the
//! headless ACP session. Keeping the exact wording in one place means a
//! compaction in either front end preserves the same facts and the same
//! on-the-wire framing.

/// What `/compact` asks the model for.
///
/// Written as instructions to itself rather than as a request for a report:
/// the reply *becomes* the conversation, so anything it leaves out is gone for
/// good. Hence the emphasis on specifics -- a summary that says "fixed the
/// bug" instead of naming the file has thrown away the only copy.
pub const COMPACT_INSTRUCTION: &str = "\
Summarise the conversation above so that it can replace it entirely as your context.

Keep: what the user is trying to achieve, decisions taken and the reasoning \
behind them, every file path touched and what changed in it, commands run and \
what they produced, and anything still unfinished or agreed as a next step.

Be specific. Names, paths, numbers and exact error text survive only if you \
write them down -- the messages above will not be sent again, so anything you \
leave out is lost. Drop pleasantries, restatements, and anything since \
superseded.

Before the summary, in a section of its own headed `Proposed COVEXCODE.md \
updates:`, list any durable, non-obvious project facts from this conversation \
that are worth keeping past this summary -- a build or test quirk, a wrong \
assumption you were corrected on, a convention you were told -- and are not \
already written down in the project notes. One line each, or none at all if \
nothing in this conversation rises to that bar; do not strain to fill it. \
No tools are available to you in this request, so only list them here -- do \
not attempt to call one. They stay in view in the summary that replaces this \
conversation, so once tools are available again, propose the actual edit to \
COVEXCODE.md through the normal approval, the same as any other file change.

Write it as notes to yourself, not as a reply to the user, and do not comment \
on the act of summarising.";

/// Prefixed to the summary on the wire so the model reads it as context it
/// already has rather than as something it is being asked to act on. The user
/// never sees this line -- the transcript shows `display`, which is the
/// summary alone.
pub const SUMMARY_PREAMBLE: &str =
    "Summary of the conversation so far. The messages it covers have been \
removed to save context, so this is all that remains of them:";

/// The message that replaces a conversation after compaction. `system`, not
/// `assistant`: it is context the model already has, not something it said.
pub fn summary_message(summary: &str) -> crate::llm::ChatMessage {
    crate::llm::ChatMessage::text(
        "system",
        format!("{SUMMARY_PREAMBLE}\n\n{summary}"),
    )
}
