---
title: "AI agent setup"
description: "Configure AI coding agents like Claude Code and Codex to work with mz-deploy projects."
menu:
  main:
    parent: manage-mz-deploy
    weight: 44
    identifier: "mz-deploy-agent-setup"
    name: "AI agent setup"
---

`mz-deploy` was built with AI coding agents in mind. Every new project ships
with agent-readable documentation, the CLI provides agent-optimized help, and
the language server gives agents real-time feedback on SQL correctness.

## Project skill

When you run `mz-deploy new`, the scaffolded project includes an agent skill
at `.agents/skills/mz-deploy/SKILL.md`. Agents that support skills (Claude
Code, Codex) load this automatically when working in the project.

The skill teaches the agent your project's conventions: one object per file,
file paths map to qualified names, how the deployment lifecycle works, unit
test syntax, and how to get detailed help with `mz-deploy help <command>`.
You don't need to explain these things — the agent already knows them.

## Agent-optimized help

```bash
mz-deploy help <command>    # Detailed guide for a single command
mz-deploy help --all        # All command guides concatenated
```

Unlike `--help` (which prints brief CLI usage), `help` returns full guides
with behavior notes, examples, error recovery steps, and related commands.

## Language server

The mz-deploy language server gives agents the same benefits it gives human
editors: parse error diagnostics on every file change, go-to-definition across
your project, and column-aware completions scoped to actual dependencies.

For agents, this means fewer incorrect SQL suggestions — the agent sees real
column names and types from your `types.lock` rather than guessing.

### Configuring for Claude Code

Add to your project's `.claude/settings.json`:

```json
{
  "lsp": {
    "mz-deploy": {
      "command": "mz-deploy",
      "args": ["lsp", "-d", "."],
      "filePatterns": ["*.sql"]
    }
  }
}
```

### Configuring for Codex

Add to your project's agent configuration:

```json
{
  "lsp": {
    "sql": {
      "command": "mz-deploy",
      "args": ["lsp", "-d", "."]
    }
  }
}
```
