
## Local pre-push hook

This repo ships a `.githooks/pre-push` that runs the same checks GitHub Actions runs (ruff, mypy, pytest). Wire it up once per clone:

```bash
git config core.hooksPath .githooks
```

Integration tests (against a local `surrealdb/surrealdb:v3.0.5` container) are opt-in:

```bash
export SURQL_PRE_PUSH_INTEGRATION=1
docker run -d -p 8000:8000 --name surrealdb surrealdb/surrealdb:v3.0.5 start --user root --pass root memory
```

Bypass (rarely, only with authorisation):

```bash
git push --no-verify
```
