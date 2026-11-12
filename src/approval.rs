//! The typed seam between "the model wants to do something" and "the user
//! decided" -- step one of extracting the agent loop from `App` (see
//! `upgrade-plan.md`, Phase 3).
//!
//! Today these two types travel a very short distance: `App::advance_approvals`
//! builds an [`ApprovalRequest`], the approval popup renders it out of
//! `Overlay::ToolApproval`, and the keypress becomes a [`Decision`] consumed by
//! `App::decide`. That is deliberate. The point of this module is not what it
//! does now but what it pins down: every approval flows through exactly one
//! request type and one decision function, no matter where the question came
//! from. When the agent loop becomes its own task, these become the messages
//! on the channel between it and the UI -- and nothing about what the user
//! sees or answers has to change, because the popup already speaks this type.
//!
//! Nothing in here weakens or reroutes the safety decisions themselves:
//! blocked commands are refused before a request is ever built, plan mode
//! filters before it, and `[tools] approval` is judged before it. A request existing means "the user must answer this",
//! and there is no constructor that skips the queue.

use crate::llm::ToolCall;
use crate::tools::Action;
#[cfg(feature = "cli")]
use crossterm::event::{KeyCode, KeyModifiers};

/// One thing the model wants to do, waiting for the user's answer.
#[derive(Clone, Debug, PartialEq)]
pub struct ApprovalRequest {
    /// The call exactly as the model made it -- what executes if allowed.
    pub call: ToolCall,
    /// What that call *means*, already interpreted -- what the popup shows.
    /// Carried alongside `call` rather than re-derived so the thing displayed
    /// and the thing decided can never drift apart.
    pub action: Action,
    /// How many more calls are queued behind this one.
    pub remaining: usize,
    /// For a `Write` or an `Edit`, what the file looks like before and after
    /// -- the diff the popup draws instead of describing the change in prose.
    ///
    /// It lives on the request rather than inside [`Action`] because it is not
    /// part of what the call *means*; it is what the question looks like when
    /// asked. `Action` stays a pure interpretation of the tool call, which is
    /// what lets the same value be compared, logged and matched on without
    /// dragging a snapshot of the filesystem along with it.
    ///
    /// Computed once, when the request is built, against the file as it is at
    /// that moment. `None` when there is nothing to show -- see
    /// [`crate::tools::preview_change`] -- and the popup falls back to its
    /// plain rendering.
    pub preview: Option<crate::diff::FileDiff>,
}

/// The user's answer to one [`ApprovalRequest`].
///
/// Three cases, not two, because "no" has always carried more information
/// than the binary could hold: the user knows *why* it was no, and the model
/// did not. `DenyWithFeedback` is that reason travelling to the model instead
/// of being thrown away -- see [`Feedback`] for the prompt that collects it.
///
/// Deliberately not `Copy` any more: the third case owns a `String`. Every
/// consumer already either matched on it or passed it straight through, so
/// the only change at a call site is that `is_allowed` now takes `&self`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Decision {
    /// Run it. For a plan, adopt it; for a deployment, hand over to the
    /// deploy flow.
    Allow,
    /// Don't. The model is told the call was declined and carries on without
    /// it; nothing is executed. Worded exactly as it was before feedback
    /// existed, so a plain refusal is not a behaviour change.
    Deny,
    /// Don't, and here is why. The reason goes to the model verbatim.
    ///
    /// Unused in the ACP-only binary: `protocol::Decision::from` is the RPC
    /// surface's equivalent, and a permission response there can only say
    /// allow or reject -- there is no field to carry a reason in. So the whole
    /// feedback half of this module is TUI-only by construction, not by
    /// omission, and that binary is the one place it is dead weight.
    #[allow(dead_code)]
    DenyWithFeedback(String),
}

#[allow(dead_code)]
impl Decision {
    pub fn is_allowed(&self) -> bool {
        matches!(self, Decision::Allow)
    }

    /// The reason the user gave, when they gave one.
    pub fn reason(&self) -> Option<&str> {
        match self {
            Decision::DenyWithFeedback(reason) => Some(reason.as_str()),
            _ => None,
        }
    }
}

/// The one-line "why?" prompt that follows a `no`.
///
/// The whole point of this type is that there is exactly one of it. Every
/// place in the app that can be told "no" by a person offers the same chance
/// to say why, collects it the same way, and turns it into the same
/// [`Decision`] -- so no call site can drift into its own slightly different
/// idea of what pressing Esc means.
///
/// It holds only the typed text. The decision is not made until the user
/// says it is: Enter sends, Esc abandons, and **neither is required** -- an
/// empty Enter is a plain `Deny`, so pressing `n` and then Enter quickly is
/// still the two-keystroke refusal it always was.
#[allow(dead_code)]
pub struct Feedback {
    text: String,
}

#[allow(dead_code)]
impl Default for Feedback {
    fn default() -> Self {
        Self::new()
    }
}

#[allow(dead_code)]
impl Feedback {
    pub fn new() -> Self {
        Self {
            text: String::new(),
        }
    }

    pub fn text(&self) -> &str {
        &self.text
    }

    /// Whether the user has typed nothing but whitespace, which is the case
    /// that has to behave exactly like a plain `no`.
    pub fn is_blank(&self) -> bool {
        self.text.trim().is_empty()
    }

    /// Feed one keypress to the prompt.
    ///
    /// `Some(decision)` means the prompt is finished and the caller should
    /// apply it; `None` means the key was text editing and the prompt stays
    /// up. Every key that is not Backspace, Enter, Esc or a printable
    /// character is swallowed here rather than falling through to the rest of
    /// the app -- while this prompt is open, nothing else may act on a
    /// keystroke.
    #[cfg(feature = "cli")]
    pub fn handle_key(&mut self, code: KeyCode, modifiers: KeyModifiers) -> Option<Decision> {
        match code {
            // Esc skips the offer. Identical to an empty Enter, and identical
            // to what Esc meant before this prompt existed -- which is why it
            // is a bare `Deny` and not `self.decision()`: whatever had been
            // typed must be thrown away, not sent.
            KeyCode::Esc => Some(Decision::Deny),
            KeyCode::Enter => Some(self.decision()),
            KeyCode::Backspace => {
                self.text.pop();
                None
            }
            KeyCode::Char(c) if !modifiers.contains(KeyModifiers::CONTROL) => {
                self.text.push(c);
                None
            }
            _ => None,
        }
    }

    /// Turn what has been typed so far into the answer it stands for.
    ///
    /// Trimmed, so a reason of only spaces is not sent as a reason -- the
    /// model would otherwise be handed an empty string and told it was
    /// feedback.
    pub fn decision(&self) -> Decision {
        let reason = self.text.trim();
        if reason.is_empty() {
            Decision::Deny
        } else {
            Decision::DenyWithFeedback(reason.to_string())
        }
    }
}


/// What to do with one queued call -- the pure decision `App::advance_approvals`
/// makes as it walks `pending_tools`, extracted so a headless (RPC-driven)
/// session can make the exact same decision without needing any of `App`'s
/// other state.
///
/// Confirmed narrow before this was written, not assumed: an independent
/// review traced every predicate `advance_approvals` calls and found the
/// whole six-way ordering depends on exactly the call, the workspace it
/// would run in, plan mode, and the configured approval policy -- nothing
/// else out of `App`'s ~60 fields. That review also found this ordering
/// changed 15 times in `needs_approval` alone across three weeks of this
/// project's history, several of those changes closing real safety gaps
/// ("Put the guardrail above approval, where no setting can reach it") --
/// which is why this is a single shared function a headless session calls
/// into, rather than a second implementation a headless session maintains
/// on its own. Two independently-maintained copies of *this specific*
/// logic drifting apart is not a cosmetic bug; it is the RPC surface
/// running something the TUI would have refused.
///
/// `App::advance_approvals` and any headless equivalent match on this and
/// apply their own effects for each variant (push a message, mutate a
/// plan, show a prompt, ...) -- this function decides, callers act.
#[derive(Debug, Clone, PartialEq)]
pub enum Verdict {
    /// Refused outright, unconditionally. No approval mode or setting
    /// reaches this -- see `danger::Risk::Blocked` and its own callers for
    /// why that has to stay true.
    Blocked(String),
    /// Refused because plan mode is on and this call is not read-only.
    PlanRefused(String),
    /// `plan_progress` bookkeeping against a plan the user already
    /// approved -- always auto-resolved, never asked, because there is
    /// nothing here for a prompt to protect.
    Progress {
        step: usize,
        done: bool,
        note: Option<String>,
    },
    /// `update_todos` bookkeeping -- always auto-resolved, same reasoning
    /// as `Progress`.
    Todos(Vec<crate::tools::TodoItem>),
    /// Nothing here needs asking about -- either the configured policy
    /// auto-approves it, or there was nothing coherent to interpret in the
    /// first place, in which case the runner is better placed to explain
    /// the malformed arguments back to the model than a prompt would be.
    AutoApprove,
    /// Stop and ask. Carries the interpreted action so a caller can render
    /// or wire it into an `ApprovalRequest` without re-deriving it.
    Ask(crate::tools::Action),
}

/// The pure decision itself -- see [`Verdict`]'s docs for why this exists
/// as a free function taking exactly these four things, not a method on
/// `App`. Mirrors `App::advance_approvals`'s ordering exactly: verified
/// against it line by line, not reproduced from memory. Change the
/// ordering here, not in two places -- that is the entire point.
pub fn verdict_for(
    call: &crate::llm::ToolCall,
    workspace_root: &std::path::Path,
    mode: crate::tools::Mode,
    approval_mode: crate::config::ApprovalMode,
) -> Verdict {
    let action = crate::tools::describe_action(call);

    // 1. Blocked outright -- ranked above plan mode so a catastrophic
    // command is reported as blocked rather than merely out of scope.
    let risk = match &action {
        Some(a) => crate::tools::action_risk(a, workspace_root),
        None => crate::danger::Risk::Normal,
    };
    if let crate::danger::Risk::Blocked(reason) = risk {
        return Verdict::Blocked(reason);
    }

    // 2. Plan mode outranks every approval setting.
    if mode.is_plan() {
        if let Some(a) = &action {
            if let Some(reason) = crate::tools::plan_mode_block(a) {
                return Verdict::PlanRefused(reason);
            }
        }
    }

    // 3 & 4. Pure in-memory bookkeeping against state the user already
    // agreed to -- never prompted, same reasoning for both: there is
    // nothing here for a prompt to protect.
    match &action {
        Some(crate::tools::Action::Progress { step, done, note }) => {
            return Verdict::Progress {
                step: *step,
                done: *done,
                note: note.clone(),
            };
        }
        Some(crate::tools::Action::Todos(items)) => {
            return Verdict::Todos(items.clone());
        }
        _ => {}
    }

    // 5. Whether the configured policy needs to ask at all.
    let needs_approval = match action {
        Some(ref a) => {
            risk.is_dangerous()
                || matches!(a, crate::tools::Action::Plan(_))
                || match approval_mode {
                    crate::config::ApprovalMode::Destructive => false,
                    crate::config::ApprovalMode::Always => {
                        !crate::tools::is_read_only_action(a)
                    }
                }
        }
        // Nothing coherent to show, so nothing to approve -- see the
        // `AutoApprove` variant's docs.
        None => false,
    };
    if !needs_approval {
        return Verdict::AutoApprove;
    }

    // 6. Everything else stops and asks. `action` is `Some` here: the
    // `None` branch above always sets `needs_approval` false.
    Verdict::Ask(action.expect("needs_approval only true when action is Some"))
}


#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(feature = "cli")]
    fn typed(text: &str) -> Feedback {
        let mut feedback = Feedback::new();
        for c in text.chars() {
            assert_eq!(feedback.handle_key(KeyCode::Char(c), KeyModifiers::NONE), None);
        }
        feedback
    }

    /// The core contract: what the user typed is what the model is told,
    /// character for character.
    #[cfg(feature = "cli")]
    #[test]
    fn a_typed_reason_becomes_deny_with_feedback_verbatim() {
        let mut feedback = typed("use the existing helper instead");
        assert_eq!(
            feedback.handle_key(KeyCode::Enter, KeyModifiers::NONE),
            Some(Decision::DenyWithFeedback(
                "use the existing helper instead".to_string()
            ))
        );
    }

    /// Pressing `n` and immediately Enter has to be a plain refusal -- feedback
    /// is offered, never required.
    #[cfg(feature = "cli")]
    #[test]
    fn an_empty_enter_is_a_plain_deny() {
        let mut feedback = Feedback::new();
        assert_eq!(
            feedback.handle_key(KeyCode::Enter, KeyModifiers::NONE),
            Some(Decision::Deny)
        );
    }

    #[cfg(feature = "cli")]
    #[test]
    fn esc_is_a_plain_deny() {
        let mut feedback = Feedback::new();
        assert_eq!(
            feedback.handle_key(KeyCode::Esc, KeyModifiers::NONE),
            Some(Decision::Deny)
        );
    }

    /// Esc means "forget it", not "send what I had half-typed".
    #[cfg(feature = "cli")]
    #[test]
    fn esc_after_typing_still_sends_nothing() {
        let mut feedback = typed("half a thou");
        assert_eq!(
            feedback.handle_key(KeyCode::Esc, KeyModifiers::NONE),
            Some(Decision::Deny)
        );
    }

    /// Whitespace is not a reason: the model would otherwise be handed an empty
    /// string and told it was feedback.
    #[cfg(feature = "cli")]
    #[test]
    fn a_blank_reason_is_a_plain_deny_not_an_empty_feedback() {
        let mut feedback = typed("   ");
        assert!(feedback.is_blank());
        assert_eq!(
            feedback.handle_key(KeyCode::Enter, KeyModifiers::NONE),
            Some(Decision::Deny)
        );
    }

    /// A reason is trimmed at the edges but never rewritten inside.
    #[cfg(feature = "cli")]
    #[test]
    fn a_reason_keeps_its_inner_punctuation_and_is_only_trimmed() {
        let mut feedback = typed("  don't touch app.rs: it's mid-edit  ");
        let decision = feedback
            .handle_key(KeyCode::Enter, KeyModifiers::NONE)
            .unwrap();
        assert_eq!(
            decision,
            Decision::DenyWithFeedback("don't touch app.rs: it's mid-edit".to_string())
        );
    }

    #[cfg(feature = "cli")]
    #[test]
    fn backspace_edits_the_reason() {
        let mut feedback = typed("wrong");
        feedback.handle_key(KeyCode::Backspace, KeyModifiers::NONE);
        assert_eq!(feedback.text(), "wron");
    }

    /// While this prompt is open, `y` and `n` are letters like any other. If
    /// they were still the approval keys, a reason containing either would
    /// answer the prompt by accident.
    #[cfg(feature = "cli")]
    #[test]
    fn y_and_n_are_ordinary_characters_inside_the_prompt() {
        let mut feedback = typed("ny");
        assert_eq!(
            feedback.handle_key(KeyCode::Enter, KeyModifiers::NONE),
            Some(Decision::DenyWithFeedback("ny".to_string()))
        );
    }

    /// A control chord is not text: Ctrl-C has to stay the interrupt/exit it is,
    /// and must not land a literal character in the reason.
    #[cfg(feature = "cli")]
    #[test]
    fn control_chords_are_swallowed_rather_than_typed() {
        let mut feedback = Feedback::new();
        assert_eq!(
            feedback.handle_key(KeyCode::Char('c'), KeyModifiers::CONTROL),
            None
        );
        assert_eq!(feedback.text(), "");
    }

    #[test]
    fn is_allowed_is_only_true_for_allow() {
        assert!(Decision::Allow.is_allowed());
        assert!(!Decision::Deny.is_allowed());
        assert!(!Decision::DenyWithFeedback("no".to_string()).is_allowed());
    }

    #[test]
    fn reason_is_only_some_for_feedback() {
        assert_eq!(Decision::Allow.reason(), None);
        assert_eq!(Decision::Deny.reason(), None);
        assert_eq!(Decision::DenyWithFeedback("no".to_string()).reason(), Some("no"));
    }
}
