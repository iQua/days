## Coding is delegated to Codex

All coding — implementation, tests, refactors, mechanical migrations,
measurement campaigns — goes to Codex (`gpt-5.6-sol`, reasoning effort
`xhigh`), launched as an interactive lane in a herdr pane. Claude models do
not write code directly; they plan, orchestrate, verify, and review.

- Give Codex a complete, self-contained brief: the task, the exact files, the
  acceptance criteria, the gates to run, and where to write its report. It
  does not see this conversation. Write the brief to a file with a quoted
  heredoc; never paste a brief inline (backticks in a raw string get
  shell-executed).
- Tell the lane to stop and report on any contradiction between its brief
  and the code or evidence rather than choosing a resolution itself; this
  has caught real design errors.
- After every lane finishes, verify the result yourself: read the diff, run
  the relevant tests and gates, recompute headline numbers from raw
  artifacts. If a lane hangs or produces no edits, stop it and re-brief with
  a narrower task. Never merge on a projection; merge on a passed verdict.

## Codex lanes via herdr

The pane runs the user's interactive shell, whose `codex` alias already
injects the sandbox flags — pass ONLY the model args, or the duplicated
flags error out:

```bash
herdr workspace create --cwd /Users/bli/Playground/days --label <lane-name>
# note the root pane_id in the JSON reply, e.g. w4T:p1
herdr agent start <lane-name> --kind codex --pane <pane_id> -- -m gpt-5.6-sol -c model_reasoning_effort=xhigh
herdr agent prompt <lane-name> "$(cat brief.txt)" --wait --until working --timeout 15000
```

- `agent prompt` takes positional TEXT only (there is no `--file` option);
  pass the brief as `"$(cat brief.txt)"`.
- Steer with further `herdr agent prompt <lane-name> "<message>"` calls
  (they queue until the lane's next turn boundary); inspect with
  `herdr agent read <lane-name>`, `herdr agent get <lane-name>`, and
  `herdr workspace list` (agent_status: working/idle/blocked/done).
- `--wait --until <status> --timeout <ms>` confirms the submission landed;
  without `--timeout` the wait is indefinite.
- One lane per worktree: create it with `git worktree add` and point
  `--cwd` at it, so parallel lanes never share a checkout. Lanes commit on
  their own branches and never push, merge, or rebase; the orchestrator
  merges after the gates.
- Parallel lanes may share a GPU host for builds and conformance tests, but
  only ONE lane may take a timing clock on a host at a time; timing waits
  for every other lane's remote work to finish.

### React the moment a lane finishes

Arm a blocking wait as a background task immediately after submitting the
brief:

```bash
herdr agent prompt <lane-name> "$(cat brief.txt)" --wait --until working --timeout 15000
# then, as a run-in-background Bash task:
herdr agent wait <lane-name>
```

`herdr agent wait` blocks until the agent settles (idle, done, or
blocked; indefinite without `--timeout`). Run in the background, it
exits the instant Codex finishes — the harness re-invokes the
orchestrator with a task notification, and review starts immediately.
`blocked` matching also surfaces a Codex question the moment it is asked.

- The wait does not track turns: it matches the NEXT settled state, so
  re-arm it after every prompt on multi-prompt lanes.
- The background wait task is sometimes stopped externally (it shows as
  "killed" without the lane having settled). Always keep one fallback
  wakeup armed at 20–30 minutes while any lane is running; on a kill, check
  the lane's status and re-arm the wait. The fallback also covers work the
  harness cannot track (remote campaigns, GitHub Actions runs).
