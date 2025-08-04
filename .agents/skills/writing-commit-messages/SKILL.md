---
name: writing-commit-messages
description: >-
  Writes Git commit messages. Activates when the user asks to write
  a commit message, draft a commit message, or similar.
licence: Copyright (c) 2024 Mitchell Hashimoto, Ghostty contributors
---

# Writing Commit Messages

Write commit messages that follow commit style guidelines for the project.

## Format

```
<subsystem>: <summary>

<reference issues/PRs/etc.>

<long form description>
```

## Rules

### Subject line

- **Subsystem prefix**: Use a short, lowercase identifier for the area of code
  changed (more on this later). Determine this from the file paths in the diff. 
- **Summary**: Lowercase start (not capitalized), imperative mood,
  no trailing period. Keep it concise—ideally under 60 characters
  total for the whole subject line.

Well known areas:

 - `cp`: control plane related changes. Use `cp/core` and `cp/sql` for the core
   and sql subsystems, respectively.
 - `ingestor`: the ingestor. Use `ingestor/core` and `ingestor/http` for the
   core and http subsystems.
 - `build`: changes related to the build system, including nix.
 - all other crates: use the name of the crate without the `wings_` prefix. Use
   the hypenated `-` name.
 
### References

- If the change relates to a GitHub issue, PR, or discussion, list
  the relevant numbers on their own lines after the subject, separated
  by a blank line. E.g. `#1234`
- If there are no references, omit this section entirely (no blank
  line).

### Long form description

- Describe **what changed**, **what the previous behavior was**,
  and **how the new behavior works** at a high level.
- Use plain prose, not bullet points. Wrap lines at ~72 characters.
- Focus on the _why_ and _how_ rather than restating the diff.
- Keep the tone direct and technical without no filler phrases.
- Don't exceed a handful of paragraphs; less is more.

## Workflow

- Run a diff to see what changes are present since the last commit.
- Identify the subsystem and crates from the changed file paths.
- Identify any referenced issues/PRs from the diff context or
  branch name.
- Draft the commit message following the format above.
- Apply the commit
- Don't push the commit; leave that to the user.
