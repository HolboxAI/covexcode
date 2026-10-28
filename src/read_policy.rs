/*---------------------------------------------------------------------------------------------
 *  Copyright (c) HolboxAI. Licensed under the MIT License.
 *--------------------------------------------------------------------------------------------*/

//! The read-only classification shared by the agent loop and the ACP
//! transport. Kept out of `agent.rs` so the headless agent binary does not
//! need the TUI's `App` state just to answer "may these calls run together?".

use crate::llm::ToolCall;
use crate::tools;

pub(crate) fn is_pure_read_call(call: &ToolCall) -> bool {
	match call.function.name.as_str() {
		tools::READ_FILE | tools::LIST_DIR | tools::GLOB | tools::GREP_SEARCH => true,
		tools::RUN_COMMAND => matches!(
			tools::describe_action(call),
			Some(tools::Action::Command { command, .. }) if tools::is_read_only(&command)
		),
		_ => false,
	}
}
