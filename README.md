# hermes-phabricator

A Hermes gateway platform plugin that connects Phabricator Maniphest to a Hermes agent.

A Phabricator webhook wakes the agent when a task enters a column of a board, or when someone writes `@bot` in a comment.
The agent's final reply is posted back as a Maniphest comment.
One task is one chat, so follow-up comments continue the same session.

## What it does

- Verifies `X-Phabricator-Webhook-Signature` (HMAC-SHA256 of the raw body) itself.
- Asks Conduit what changed, because the webhook only carries transaction PHIDs.
- Mention: a user other than the bot writes `@handle` (case-insensitive, not `@handlebot`) in a new comment on a task.
- New task: a task was created in, or moved into, the configured column, is open, and the bot has not commented on it yet.
- Ignores the bot's own comments, tasks off the configured board, removed comments, test deliveries and non-task objects.
- Posts the agent's final reply with `maniphest.edit` (one comment).
- Optional poller (off by default) that catches new tasks whose webhook was missed.

## Install

```
hermes plugins install <git url> --enable
```

Third-party plugins load only when listed in `plugins.enabled`, and `--enable` does that.
A manual install is a clone (or symlink) at `<HERMES_HOME>/plugins/phabricator`.

With `gateway.multiplex_profiles`, install it into the profile that serves Phabricator (`hermes -p <profile> plugins install ...`) and also into the default profile.
The default profile never starts the adapter (it has no `PHABRICATOR_*` settings), but the gateway reads every profile's saved sessions at startup before the profile's plugins are discovered.
Without the plugin in the default profile, those sessions are skipped as "not a valid Platform", and each task starts a fresh chat after a gateway restart.

## Minimal setup

Profile `.env`:

```
PHABRICATOR_URL=https://phabricator.example.com
PHABRICATOR_API_TOKEN=api-xxxxxxxx
PHABRICATOR_WEBHOOK_SECRET=xxxxxxxx
```

Profile `config.yaml`:

```yaml
plugins:
  enabled: [phabricator]
```

With only that, the plugin enables itself, finds its own identity with `user.whoami`, answers `@<its username>` on any task the webhook sends, and listens on `127.0.0.1:8646/phabricator/webhook`.
Nobody is authorized until you add an allowlist or allow-all (see below).

Typical full setup:

```
PHABRICATOR_BOARD=Support Board
PHABRICATOR_NEW_COLUMN=New
PHABRICATOR_ALLOW_ALL_USERS=true
PHABRICATOR_PORT=8646
```

```yaml
plugins:
  enabled: [phabricator]
platform_toolsets:
  phabricator: [skills]        # whatever the bot may use, same as for Slack
display:
  busy_input_mode: queue       # a second comment waits instead of interrupting the running one
```

## Settings

Every setting is an env var in the profile `.env`.
The non-secret ones can instead live under `platforms.phabricator.extra` in `config.yaml` (env wins).

| Env var | `extra` key | Meaning |
| --- | --- | --- |
| `PHABRICATOR_URL` | `url` | Base URL (required). |
| `PHABRICATOR_API_TOKEN` | - | Conduit token of the bot user (required, env only). |
| `PHABRICATOR_WEBHOOK_SECRET` | - | HMAC key of the webhook (required, env only). |
| `PHABRICATOR_BOT_HANDLE` | `bot_handle` | Mention handle. Default: the bot's username. |
| `PHABRICATOR_BOARD` | `board` | Only react to tasks tagged with this project: PHID, `#slug` or exact name. Empty: any task. |
| `PHABRICATOR_NEW_COLUMN` | `new_column` | Column PHID or name on that board. Enables the new-task trigger. Empty: mentions only. |
| `PHABRICATOR_ALLOWED_USERS` | `allowed_users` | Usernames allowed to mention the bot. |
| `PHABRICATOR_ALLOW_ALL_USERS` | `allow_all_users` | Any Phabricator user may mention the bot. |
| `PHABRICATOR_SKILL` | `skill` | Skill preloaded into every session this platform starts. |
| `PHABRICATOR_HOST` / `PHABRICATOR_PORT` | `host` / `port` | Listener bind. Default `127.0.0.1:8646`. Use a distinct port per profile. |
| `PHABRICATOR_WEBHOOK_PATH` | `webhook_path` | Listener path. Default `/phabricator/webhook`. Set it to the public path (for example `/p/<profile>/webhooks/phab`) to avoid a proxy rewrite. |
| `PHABRICATOR_POLL_INTERVAL` | `poll_interval` | Seconds between New-column polls. `0` (default) is off. |
| `PHABRICATOR_POLL_LOOKBACK` | `poll_lookback` | Only tasks modified within this many seconds are polled. Default 3600. |

Names are resolved through Conduit when the adapter connects.
A name that matches zero or several projects or columns fails the connect with a clear message, so use the PHID then.

## Phabricator admin steps

1. Create a bot user (Phabricator Bot flag is fine) and give it a Conduit API token.
   It needs permission to view and comment on the tasks.
2. Webhooks app: create a webhook with URI `https://<public host>/<path to the listener>`, status Active, and copy its HMAC key.
   Either use a Herald rule that calls the webhook on Maniphest tasks (for example "Tags include <board>"), or firehose mode.
   Firehose is fine: non-task events are ignored after the signature check.
3. Put a reverse proxy in front of the listener if Phabricator cannot reach it directly.
   The path is `/phabricator/webhook` on the plugin side.
4. Use the webhook Test button and check the gateway log.

## Behavior notes

- Only the agent's final reply becomes a comment.
  Busy acknowledgements, the "no home channel" nudge and progress lines also pass through the adapter and are dropped.
- If a run fails (provider down, crash), the task gets one fixed line, "Could not investigate this task: the agent run failed. Mention me again to retry.", and never the exception text.
- Task text is untrusted, so it can never run gateway commands such as `/new`.
- New-task events are an admin choice (the configured column), so they skip the user allowlist.
  Mentions follow the normal gateway allowlist, and unauthorized mentions are ignored silently.
- Redelivered webhooks do not rerun a task: transactions are deduplicated for an hour.
- Comments render as Remarkup.
  The platform hint tells the agent to use `**bold**`, `-` bullets, backticks and bare URLs.

## Design notes

Decisions taken while building the plugin, with the evidence behind them.

**Listener and profiles.**
Hermes can serve secondary profiles on the default profile's single listener (`/p/<profile>/...`), but that shared-listener mode is keyed on a hardcoded list of core platforms (`PORT_BINDING_PLATFORM_VALUES` in `gateway/config.py`), so a plugin cannot use it.
The plugin therefore binds its own `host:port`, one per profile, and the reverse proxy maps a path to that port.
The adapter does not need to know its profile: under `gateway.multiplex_profiles` each profile's adapter is constructed inside that profile's secret scope, so `PHABRICATOR_*` secrets and the allowlist come from that profile's `.env`.
Verified: a secondary profile connected on its own port next to a default profile, with the API token read from the profile `.env`.

**Who posts the reply.**
`send()` posts it, from the agent's final text, so there is one write path and the agent needs no comment tool.
Remove any "add comment" tool from the profile's tool list.
`send()` only posts sends that carry the base adapter's final-reply marker (`metadata["notify"]`), the same contract the A2A plugin uses.
Without that filter the first live test posted the "no home channel" nudge and the busy acknowledgement as task comments.

**Session per task.**
The chat id is the task PHID, chat type `group`, and the thread id is the same PHID (the name shown to the agent is `T123: title`).
The PHID rather than `T123` keeps sessions from colliding when a profile is later pointed at a different or rebuilt Phabricator.
Group sessions are normally split per participant, and a thread id is what keeps every commenter in the task's one session.
Verified: three comments on one task produced user turn counts 1, 2, 3 in one session, a new-task run and later mentions share it, and an unauthorized comment does not enter it.

**Failed runs.**
The base adapter's turn-error notice carries exception text and has no final-reply marker, so it is dropped like other notices.
The adapter overrides `_notify_turn_error` to post the one fixed line above instead.

**Busy input.**
The gateway default interrupts a running turn when another message arrives for the same session.
For a comment thread, `display.busy_input_mode: queue` is the better fit: both comments are answered in order.

**Poller.**
Built, behind `PHABRICATOR_POLL_INTERVAL` (default off).
It lists open tasks in the New column modified within the lookback window and starts the same run a webhook would, with the same dedup, so a webhook plus a poll never run a task twice.
It costs one `maniphest.search` per interval plus one `transaction.search` per candidate.
A missed mention is not recovered (it needs the transaction history), only missed new tasks.

**Skill.**
The plugin ships no skill.
Triage playbooks are specific to a team, so the deployment repo stays the single source of truth and `PHABRICATOR_SKILL` preloads it.

**Toolsets.**
`platform_toolsets.phabricator` works like it does for Slack: an explicit list restricts the tools.
Verified: `[skills]` gave the model 3 tools instead of the default 17.
MCP toolset names are passed through by the same mechanism; that was not exercised with real MCP servers.

**Phabricator facts verified against a real instance** (Phabricator pinned at 7ec88f3b4ed0).
- The webhook `action` is an object (`{"test": false, "silent": false, "secure": false, "epoch": N}`), not a string.
- Moving a task into a column is a transaction with `type: null`, and the column is not in `transaction.search`.
  The plugin asks `maniphest.search` with the `columnPHIDs` constraint instead.
- The signature header is lowercase hex HMAC-SHA256 of the raw body.
- Transaction order inside a payload is not chronological, so nothing depends on it.

## Known limits

- Deduplication is in memory (one hour), so a restart can rerun a transaction that Phabricator redelivers.
- The poller recovers missed new tasks only, not missed mentions.
- Tested against firehose webhooks only, not a Herald rule.
- Tested against a stub model, not a real LLM, and not with real MCP toolsets.

## Tests

```
HERMES_SRC=/path/to/hermes-agent  <python of that install>  tests/test_adapter.py
```

The suite needs a Hermes source tree and its Python environment.
It runs the adapter against an in-memory fake Conduit (`tests/fake_conduit.py`) and covers signature verification, the mention pattern, loop prevention, board and column gating, redelivery, session keys, `send()`, the poller and the HTTP listener.
`tests/fake_conduit.py` also runs as a script (with control endpoints) for manual end-to-end tests.
