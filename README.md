# codex2api

`codex2api` is an API proxy server for the Codex Responses API.
It accepts multiple client API formats and forwards requests through a unified Codex-compatible backend.

## Supported Formats

- OpenAI Chat Completions
- OpenAI Responses
- Anthropic Messages
- Gemini

## Features

- Account pool management
- Automatic token refresh
- SSE streaming support
- Abuse detection pipeline
- Admin panel

## Quick Start

1. Copy `config-server.example.json` to `config-server.json`.
2. Fill in your real credentials and runtime settings.
3. Start the server:

```bash
node server.mjs
```

## License

AGPL-3.0
