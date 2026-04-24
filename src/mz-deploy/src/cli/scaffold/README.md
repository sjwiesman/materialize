# {{name}}

A [Materialize](https://materialize.com) project managed by mz-deploy.

## Project structure

- `models/` — SQL model definitions organized by database and schema
- `clusters/` — Cluster definitions
- `roles/` — Role definitions
- `project.toml` — Project configuration

## Agent skills

Agent skills from [MaterializeInc/agent-skills](https://github.com/MaterializeInc/agent-skills) help AI agents work with Materialize.

Install them with:

```sh
npx -y skills add MaterializeInc/agent-skills -a universal -a claude-code --project
```

Later, update to the latest version with:

```sh
npx -y skills update
```
