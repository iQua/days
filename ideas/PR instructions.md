The user likes the state of the code.

There are 19 uncommitted changes.
The current branch is baochunli/track-name-select.
The target branch is origin/main.

There is no upstream branch yet.
The user requested a PR.

Follow these **exact steps** to create a PR:

- Run `git diff` to review uncommitted changes
- Commit them. Follow any instructions the user gave you about writing commit messages.
- Push to origin.
- Use `git diff origin/main...` to review the PR diff
- Use `gh pr create --base main` to create a PR onto the target branch. Keep the title under 160 characters, but write a detailed description to show a summary, as well as a bulleted list of features implemented or bugs fixed (without referring to the files and line numbers). Use the past tense to start a sentence, as in the PR description below:

---
This PR added support for drag-and-drop reordering for author cards with grab handle, stabilized author identity during edits and saves, and ensured author order persists when saving submissions.

## Changes

- Introduced reorder support in GridList with native drag/drop and visual cues.

- Added client-side author IDs for safe edit/remove/reorder actions.

- Reused `arrayMove` helper and stripped client-only fields before save.
---
