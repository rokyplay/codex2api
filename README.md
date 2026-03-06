<div align="center">

# codex2api

**A production-focused API gateway that converts OpenAI Chat/Responses, Anthropic Messages, and Gemini requests into Codex Responses API.**

**Built with Node.js (ESM), JSON-only storage, account pool orchestration, streaming, abuse protection, and admin tooling.**

[![License: AGPL-3.0](https://img.shields.io/badge/License-AGPL--3.0-blue.svg?style=for-the-badge)](./LICENSE)
[![Node.js ESM](https://img.shields.io/badge/Node.js-ESM-339933?style=for-the-badge&logo=node.js&logoColor=white)](https://nodejs.org/)
[![GitHub Stars](https://img.shields.io/github/stars/rokyplay/codex2api?style=for-the-badge&logo=github)](https://github.com/rokyplay/codex2api/stargazers)

</div>

## Features

- 🔄 **Multi-format API conversion**: OpenAI Chat/Responses, Anthropic Messages, and Gemini requests are normalized to Codex Responses.
- 🧭 **Multi-account pool management**: automatic rotation, cooldown handling, and account lifecycle control.
- 🔐 **Automatic token refresh**: JWT expiry is detected and refreshed automatically.
- ⚡ **SSE streaming relay**: real-time streaming with reasoning/thinking event support.
- 🧠 **Prompt caching**: `session_id` + `prompt_cache_key` reduce latency and repeated context cost.
- 🛡️ **Abuse detection system**: 7-rule risk scoring with auto throttle/ban and 10-minute auto recovery.
- 🚦 **Rate limiter**: global and per-user RPM/TPM controls.
- 🖥️ **Admin panel**: account management, hot config updates, metrics, and registration controls.
- 👤 **User portal**: Discord OAuth2 login, API key lifecycle management, and usage analytics.
- 🔎 **Web Search tool support**: compatible `annotations` output format.
- 🧩 **Dynamic model discovery**: pulls available models from upstream and updates runtime mapping.
- 🔑 **TOTP 2FA for admin**: stronger operator authentication for the management surface.

## Supported Models

`gpt-5` · `gpt-5-codex` · `gpt-5-codex-mini` · `gpt-5.1` · `gpt-5.1-codex` · `gpt-5.1-codex-mini` · `gpt-5.1-codex-max` · `gpt-5.2` · `gpt-5.2-codex` · `gpt-5.3-codex` · `gpt-5.4`

Reasoning effort is supported using `model(low|medium|high|xhigh)` (for example: `gpt-5.3-codex(high)`).

## API Endpoints

| Method | Endpoint | Format |
|---|---|---|
| `POST` | `/v1/chat/completions` | OpenAI Chat Completions |
| `POST` | `/v1/responses` | OpenAI Responses |
| `POST` | `/v1/messages` | Anthropic Messages |
| `POST` | `/v1beta/models/*/generateContent` | Gemini Generate Content |
| `POST` | `/backend-api/codex/responses` | Codex Native |

## Quick Start

1. Clone the repository and install dependencies.

```bash
git clone https://github.com/rokyplay/codex2api.git
cd codex2api
npm install
```

2. Copy the server configuration template.

```bash
cp config-server.example.json config-server.json
```

3. Start the service.

```bash
node server.mjs
```

## Configuration

Main runtime config lives in `config-server.json`.

- `server`: host/port, admin credentials, API keys, and TOTP settings.
- `upstream`: Codex upstream base URL and timeout controls.
- `models`: default model, aliases, and dynamic-discovery-ready model map.
- `accounts_source` + `scheduler`: account pool source and rotation strategy.
- `credentials`: auto-refresh/relogin policy for expiring sessions and JWTs.
- `prompt_cache`: `session_id` strategy and cache retention behavior.
- `abuse_detection`: rule set, thresholds, and automated enforcement/recovery.
- `rate_limits`: global/per-user RPM/TPM quotas and overrides.
- `discord_auth`: OAuth2-based user portal authentication and registration gating.

All persistent data is stored in local JSON files (no external database required).

## Architecture

```text
Client
  → Parse
  → Universal Format
  → Codex Responses
  → Upstream
  → Parse SSE
  → Convert
  → Client
```

## License

Licensed under **AGPL-3.0**. See [LICENSE](./LICENSE) for details.

## Contributing

Contributions are welcome. Open an issue or submit a PR with a clear problem statement, implementation notes, and verification steps.
