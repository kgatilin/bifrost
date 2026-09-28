# Fork of maximhq/bifrost

Branch `fork` is an upstream release tag plus:

- `transports/bifrost-http/natslog`: built-in plugin publishing every LLM
  request to NATS JetStream (tokens, cost, latency, `x-bf-lh-*` headers), wired
  in `server/plugins.go` and the built-in list in `lib/config.go`.
- `.github/workflows/fork-release.yml`: builds the upstream Dockerfile for
  linux/arm64 and pushes `ghcr.io/kgatilin/bifrost:<version>-<n>`.

The fork follows release tags, not `dev`: `dev`'s transports module requires
core/framework code that is only published when upstream tags a release.

## Taking a new upstream release

```
git fetch upstream --tags
git merge transports/vX.Y.Z          # conflicts, if any: plugins.go, config.go, go.mod/go.sum
GOWORK=off go -C transports mod tidy
GOWORK=off go -C transports test ./bifrost-http/natslog/
git push origin fork
git tag fork/vX.Y.Z-1 && git push origin fork/vX.Y.Z-1   # -> image X.Y.Z-1
```

## natslog config

```json
{"name": "natslog", "enabled": true,
 "config": {"url": "", "subject": "evt.llm.call", "header_prefix": "x-bf-lh-"}}
```

`url` empty reads `NATS_URL`. A publish that fails is logged and dropped; the
request itself never fails on it.
