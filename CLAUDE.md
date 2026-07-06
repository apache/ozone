@AGENTS.md

## Claude Code
- For builds, tests, and CI checks, delegate to the `ozone-builder` subagent (keeps Maven
  logs out of the main context).
- Module/path lookup auto-surfaces via the `ozone-map` skill.
- For code/test review, prefer the pr-review-toolkit subagents over ad-hoc review.
- Use plan mode for protobuf/RPC and RocksDB layout/upgrade changes.
