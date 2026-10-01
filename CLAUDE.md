## Work is delegated to Claude lanes

The orchestrating session plans, orchestrates, verifies, and merges. Work
goes to Claude lanes launched in herdr panes, by kind:

| Work | Model and effort | Agent definition |
|---|---|---|
| Coding: implementation, tests, refactors, mechanical migrations | Opus 5.5, high | `opus-implementer` |
| Review loops | Opus 5.5, high | `opus-reviewer` |
| Writing: docs, READMEs, PR bodies, release notes, paper prose | Fable 5.1 | `fable-writer` |
| Measurement: A/B campaigns, probes, profiling, remote suite runs | Sonnet 5.5, high | `opus-measurer` |
| Exploration and fact-gathering | Sonnet 5.5, medium | `opus-explorer` |

The agent definitions live in `~/.claude/agents/` (user level: `.claude/` is
gitignored here, so worktrees would not see project-level copies).

- Give each lane a complete, self-contained brief: the task, the exact files,
  the acceptance criteria, the gates to run, and where to write its report. It
  does not see the orchestrator's conversation. Write the brief to a file with
  a quoted heredoc; never paste a brief inline (backticks in a raw string get
  shell-executed).
- Tell the lane to stop and report on any contradiction between its brief and
  the code or evidence rather than choosing a resolution itself; this has
  caught real design errors.
- After every lane finishes, verify the result yourself: read the diff, run
  the relevant tests and gates, recompute headline numbers from raw artifacts.
  If a lane hangs or produces no edits, stop it and re-brief with a narrower
  task. Never merge on a projection; merge on a passed verdict.
- Remote builds and tests on boston, sim, and madrid
  (`bli@<host>.csl.toronto.edu`) happen only inside `~/Playground/days` on
  that host, never directly in the home directory.

## Lanes via herdr

The pane runs the user's interactive shell, whose `claude` alias already adds
`--dangerously-skip-permissions`; pass only the model, effort, and agent:

```bash
herdr workspace create --cwd /Users/bli/Playground/days --label <lane-name> --no-focus
# note the root pane_id in the JSON reply, e.g. w4T:p1
herdr agent start <lane-name> --kind claude --pane <pane_id> --timeout 90000 -- \
  --model claude-opus-5-5 --effort high --agent opus-implementer
herdr agent prompt <lane-name> "$(cat brief.md)" --wait --until working --timeout 15000
```

- Writer lanes use `--model claude-fable-5-1 --agent fable-writer`; measurer
  lanes use `--model claude-sonnet-5-5 --effort high --agent opus-measurer`;
  explorer lanes use `--model claude-sonnet-5-5 --effort medium --agent
  opus-explorer`. The `opus-` agent names are historical; the model comes
  from `--model` and the definition's frontmatter.
- `agent prompt` takes positional TEXT only (there is no `--file` option);
  pass the brief as `"$(cat brief.md)"`. If the lane stays `idle` with the
  brief typed but not submitted, send `herdr agent send-keys <lane-name> enter`.
- Steer with further `herdr agent prompt <lane-name> "<message>"` calls (they
  queue until the lane's next turn boundary); inspect with
  `herdr agent read <lane-name>`, `herdr agent get <lane-name>`, and
  `herdr workspace list` (agent_status: working/idle/blocked/done).
- One lane per worktree: create it with `git worktree add` and point `--cwd`
  at it, so parallel lanes never share a checkout. Lanes commit on their own
  branches and never push, merge, or rebase; the orchestrator merges after
  the gates.
- Parallel lanes may share a GPU host for builds and conformance tests, but
  only ONE lane may take a timing clock on a host at a time; timing waits for
  every other lane's remote work to finish.

### Branches and pull requests

One branch and one PR per phase, merged into `main` with a merge commit (so
commit hashes cited by the days-gpu evidence stay reachable). With parallel
lanes, each lane works on `<phase>/<lane>` in its own worktree; the
orchestrator merges each lane into the phase branch with a merge commit after
its gate, then opens one PR from the phase branch to `main`.

### React the moment a lane finishes

Arm a blocking wait as a background task immediately after submitting the
brief:

```bash
herdr agent prompt <lane-name> "$(cat brief.md)" --wait --until working --timeout 15000
# then, as a run-in-background Bash task:
herdr agent wait <lane-name> --until idle --until done --until blocked
```

`herdr agent wait` blocks until the agent settles (idle, done, or blocked;
indefinite without `--timeout`). Run in the background, it exits the instant
the lane finishes: the harness re-invokes the orchestrator with a task
notification, and review starts immediately. `blocked` matching also surfaces
a lane's question the moment it is asked.

- The wait does not track turns: it matches the NEXT settled state, so re-arm
  it after every prompt on multi-prompt lanes.
- The background wait task is sometimes stopped externally (it shows as
  "killed" without the lane having settled). Always keep one fallback wakeup
  armed at 20–30 minutes while any lane is running; on a kill, check the
  lane's status and re-arm the wait. The fallback also covers work the harness
  cannot track (remote campaigns, GitHub Actions runs).
