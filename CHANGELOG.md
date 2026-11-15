# Changelog

## 1.0.0 — 2026-11-15

- Long polling (`GET /updates`) with a persisted marker and per-update dedup journal.
- Outbound text with MAX HTML rendering and source-level 4000-character splitting.
- Inbound/outbound media: images, video, audio and documents via `POST /uploads`.
- Group chats with mention gating and chat+sender allowlists.
- Inline-keyboard exec approvals, clarify prompts and choice pickers via `POST /answers`.
- Typing indicators, reconnect backoff/watchdog and partial-send continuation.
- Standalone sender for cron / `hermes send`.
