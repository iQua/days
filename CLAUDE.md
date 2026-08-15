## Coding is delegated to Codex

All coding — implementation, tests, refactors, mechanical migrations — goes to
Codex via the `codex exec` CLI (already installed and authenticated), using the
`gpt-5.6-sol` model. Claude models do not write code directly; they plan,
orchestrate, and review.

Canonical invocation (non-interactive, from the repo root):

```bash
codex exec --sandbox=danger-full-access -c sandbox_workspace_write.network_access=true -m gpt-5.6-sol -c model_reasoning_effort=xhigh "Implement <task>. <constraints, files, acceptance criteria>"
```

Always use exactly these flags for coding runs: `--sandbox=danger-full-access
-c sandbox_workspace_write.network_access=true -m gpt-5.6-sol -c model_reasoning_effort=xhigh`.
`danger-full-access` (plus network access) is required so the run can reach the local network.

`codex exec` is already non-interactive with `approval: never`, and its worker enables web search by
default, so the top-level interactive flags `--search` and `--ask-for-approval` are NOT
passed here — `codex exec` rejects them (they belong to `codex <flags>` with no
subcommand). To force web search for exec explicitly, use `-c` config, not `--search`.

- Give Codex a complete, self-contained brief: the task, the exact files, the
  acceptance criteria, and the tests to run. It does not see this conversation.
- Continue a session with `codex exec resume <session-id-or---last> -m gpt-5.6-sol -c model_reasoning_effort=xhigh "<follow-up>"`.
  Exec-level flags (`--sandbox`, `-C`, `-o`) must come BEFORE the `resume` subcommand.
- For scripting: `-o <file>` writes the final message to a file; `--json`
  streams JSONL events; `-C <dir>` sets the working root.
- After every Codex run, verify the result yourself: read the diff, run the
  relevant tests, and check `pnpm test:boundaries` when imports changed. If a
  run hangs or produces no edits, kill it and re-brief with a narrower task.

## Live-steerable Codex lanes via herdr (preferred for long/multi-commit work)

For implementations that benefit from mid-flight steering, launch Codex
interactively in a herdr pane instead of `codex exec`. The pane runs the
user's interactive shell, whose `codex` alias already injects the sandbox
flags — pass ONLY the model args, or the duplicated flags error out:

```bash
herdr workspace create --cwd /Users/bli/Playground/days --label <lane-name>
# note the root pane_id in the JSON reply, e.g. w4T:p1
herdr agent start <lane-name> --kind codex --pane <pane_id> -- -m gpt-5.6-sol -c model_reasoning_effort=xhigh
herdr agent prompt <lane-name> "$(cat brief.txt)" --wait --until working --timeout 15000
```

- Briefs still go through a quoted-heredoc file and `"$(cat brief.txt)"` —
  `agent prompt` takes positional TEXT only (there is no `--file` option),
  and inline backticks in a raw string get shell-executed.
- Steer with further `herdr agent prompt <lane-name> "<message>"` calls;
  inspect with `herdr agent read <lane-name>` and `herdr workspace list`
  (agent_status: working/idle/blocked).
- `--wait --until <status> --timeout <ms>` confirms the submission landed;
  without `--timeout` the wait is indefinite.
- Choose the route by need: `codex exec` (detached, `-o` final-message
  file) for fire-and-forget batch runs; a herdr lane when you may need to
  redirect the worker mid-task. The 20-minute watchdog rule applies to
  both.
