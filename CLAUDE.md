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
