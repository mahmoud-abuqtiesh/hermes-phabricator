"""Phabricator Maniphest platform adapter for Hermes.

Inbound: a Phabricator webhook (HMAC-verified) names a task and transaction PHIDs only, so the adapter asks
Conduit what changed and starts a run for
  - a mention: a non-bot user wrote ``@handle`` in a new comment on a (board) task
  - a new task: a (board) task was created in, or moved into, the configured column and the bot has not
    commented on it yet
Outbound: ``send()`` posts the agent's final reply as a Maniphest comment.
One task is one chat (``T123``), so follow-up comments continue the same session.
"""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import hmac
import json
import logging
import re
import time
from dataclasses import dataclass
from typing import Any, Optional

from gateway.config import Platform
from gateway.platforms._shared import (
    apply_yaml_bridge as _apply_yaml_bridge,
    env_is_connected as _env_is_connected,
    extra_or_secret as _extra_or_secret,
    get_scoped_secret as _get_scoped_secret,
    seed_extra_from_env as _seed_extra_from_env,
    send_error,
)
from gateway.platforms.base import BasePlatformAdapter, SendResult
from gateway.platforms.event import MessageEvent
from gateway.platforms.helpers import MessageDeduplicator

logger = logging.getLogger(__name__)

DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 8646
DEFAULT_PATH = "/phabricator/webhook"
DEFAULT_POLL_LOOKBACK = 3600
WEBHOOK_BODY_MAX_BYTES = 1024 * 1024
MAX_TEXT_CHARS = 8000
# Moving a task into a column is an internal transaction that transaction.search reports with type null.
NEW_TRIGGER_TYPES = frozenset({"create", "projects", None})
SIGNATURE_HEADER = "X-Phabricator-Webhook-Signature"
TURN_ERROR_COMMENT = "Could not investigate this task: the agent run failed. Mention me again to retry."


def verify_signature(body: bytes, signature: str, secret: str) -> bool:
    if not secret or not signature:
        return False
    expected = hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()
    return hmac.compare_digest(expected, signature.strip().lower())


def mention_pattern(handle: str) -> re.Pattern:
    # Phabricator usernames may contain "-", "_" and "." (not trailing), so "@bot-two" and "@bot.two" are other users.
    return re.compile(r"(?<![\w@])@" + re.escape(handle) + r"(?![\w-])(?!\.\w)", re.IGNORECASE)


def flatten(value: Any, prefix: str = "") -> list[tuple[str, str]]:
    """Conduit form encoding: nested dicts and lists become bracket keys (``constraints[phids][0]``)."""
    if isinstance(value, dict):
        items = value.items()
    elif isinstance(value, (list, tuple)):
        items = enumerate(value)
    else:
        return [(prefix, str(value))]
    out: list[tuple[str, str]] = []
    for key, item in items:
        out += flatten(item, f"{prefix}[{key}]" if prefix else str(key))
    return out


def _is_test(payload: dict) -> bool:
    action = payload.get("action")
    if isinstance(action, dict):
        return bool(action.get("test"))
    return action == "test"


class ConduitError(RuntimeError):
    pass


class Conduit:
    def __init__(self, base_url: str, token: str, timeout: float = 20.0) -> None:
        self.base_url = base_url.rstrip("/")
        self._token = token
        self._timeout = timeout
        self._session = None

    async def call(self, method: str, **params: Any) -> Any:
        import aiohttp
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=self._timeout))
        form = flatten({"api.token": self._token, **params})
        async with self._session.post(f"{self.base_url}/api/{method}", data=form) as response:
            body = await response.json(content_type=None)
        if body.get("error_code"):
            raise ConduitError(f"{method}: {body['error_code']}: {body.get('error_info')}")
        return body["result"]

    async def close(self) -> None:
        if self._session is not None:
            await self._session.close()
            self._session = None


@dataclass
class Trigger:
    mode: str  # "mention" or "new"
    task_id: int
    task_phid: str
    title: str
    author: str
    text: str
    message_id: str


class PhabricatorAdapter(BasePlatformAdapter):
    MAX_MESSAGE_LENGTH = 60000

    def __init__(self, config, **kwargs):
        super().__init__(config=config, platform=Platform("phabricator"))
        extra = getattr(config, "extra", {}) or {}

        def setting(key: str, env: str, default: Any = "") -> Any:
            return _extra_or_secret(extra, key, env, default)

        self.url = str(setting("url", "PHABRICATOR_URL")).strip().rstrip("/")
        self.token = str(_get_scoped_secret("PHABRICATOR_API_TOKEN", "") or "").strip()
        self.webhook_secret = str(_get_scoped_secret("PHABRICATOR_WEBHOOK_SECRET", "") or "").strip()
        self.handle = str(setting("bot_handle", "PHABRICATOR_BOT_HANDLE")).strip().lstrip("@")
        self.board_ref = str(setting("board", "PHABRICATOR_BOARD")).strip()
        self.column_ref = str(setting("new_column", "PHABRICATOR_NEW_COLUMN")).strip()
        self.skill = str(setting("skill", "PHABRICATOR_SKILL")).strip()
        self.host = str(setting("host", "PHABRICATOR_HOST", DEFAULT_HOST))
        self.port = _int(setting("port", "PHABRICATOR_PORT", DEFAULT_PORT), DEFAULT_PORT)
        self.path = str(setting("webhook_path", "PHABRICATOR_WEBHOOK_PATH", DEFAULT_PATH))
        self.poll_interval = _int(setting("poll_interval", "PHABRICATOR_POLL_INTERVAL", 0), 0)
        self.poll_lookback = _int(setting("poll_lookback", "PHABRICATOR_POLL_LOOKBACK", DEFAULT_POLL_LOOKBACK), DEFAULT_POLL_LOOKBACK)

        self.conduit: Optional[Conduit] = None
        self.bot_phid = ""
        self.board_phid = ""
        self.column_phid = ""
        self._mention: Optional[re.Pattern] = None
        self._users: dict[str, str] = {}
        self._seen = MessageDeduplicator(max_size=4000, ttl_seconds=3600)
        self._tasks: set[asyncio.Task] = set()
        self._poller: Optional[asyncio.Task] = None
        self._app = self._runner = None

    def _fail(self, code: str, detail: str, *, retryable: bool = False) -> bool:
        self._set_fatal_error(code, detail, retryable=retryable)
        return False

    # -- lifecycle ---------------------------------------------------------------------------------

    async def connect(self, *, is_reconnect: bool = False) -> bool:
        missing = [name for name, value in (
            ("PHABRICATOR_URL", self.url), ("PHABRICATOR_API_TOKEN", self.token),
            ("PHABRICATOR_WEBHOOK_SECRET", self.webhook_secret)) if not value]
        if missing:
            return self._fail("config_missing", f"{', '.join(missing)} must be set")
        if self.column_ref and not self.board_ref:
            return self._fail("config_invalid", "PHABRICATOR_NEW_COLUMN needs PHABRICATOR_BOARD")
        identity = hashlib.sha256(f"{self.url}|{self.token}".encode()).hexdigest()[:16]
        if not self._acquire_platform_lock("phabricator", identity, "Phabricator bot token"):
            return False
        self.conduit = Conduit(self.url, self.token)
        try:
            await self._resolve_identity()
            from aiohttp import web
            self._app = self._build_app(web)
            self._wire_plugin_handlers(self._app)
            from gateway.platforms.shared_ingress import bind_listener
            self._runner = await bind_listener(self, self._app, self.host, self.port, self.path)
        except Exception as exc:
            await self.conduit.close()
            self._release_platform_lock()
            return self._fail("connect_failed", f"Phabricator connect failed: {exc}", retryable=True)
        self._mark_connected()
        if self.poll_interval > 0 and self.column_phid:
            self._poller = asyncio.create_task(self._poll_loop())
        logger.info("Phabricator: bot @%s (%s), webhook on %s:%s%s, board=%s column=%s poll=%ss",
                    self.handle, self.bot_phid, self.host, self.port, self.path,
                    self.board_phid or "-", self.column_phid or "-", self.poll_interval)
        return True

    async def disconnect(self) -> None:
        self._mark_disconnected()
        for task in [self._poller, *self._tasks]:
            if task is not None:
                task.cancel()
        self._poller = None
        self._tasks.clear()
        if self._runner is not None:
            with contextlib.suppress(Exception):
                await self._runner.cleanup()
            self._runner = None
        if self.conduit is not None:
            with contextlib.suppress(Exception):
                await self.conduit.close()
        with contextlib.suppress(Exception):
            self._release_platform_lock()

    def _build_app(self, web):
        app = web.Application(client_max_size=WEBHOOK_BODY_MAX_BYTES)
        app.router.add_post(self.path, self._handle_webhook)
        app.router.add_get(f"{self.path}/health", self._handle_health)
        return app

    async def _resolve_identity(self) -> None:
        me = await self.conduit.call("user.whoami")
        self.bot_phid = me["phid"]
        self.handle = self.handle or me["userName"]
        self._mention = mention_pattern(self.handle)
        if self.board_ref:
            self.board_phid = await self._resolve_project(self.board_ref)
        if self.column_ref:
            self.column_phid = await self._resolve_column(self.board_phid, self.column_ref)

    async def _resolve_project(self, ref: str) -> str:
        if ref.startswith("PHID-PROJ-"):
            return ref
        if ref.startswith("#"):
            constraints = {"slugs": [ref[1:]]}
        else:
            constraints = {"query": ref}
        found = (await self.conduit.call("project.search", constraints=constraints))["data"]
        wanted = ref.lstrip("#").casefold()
        exact = [p for p in found if wanted in (p["fields"]["name"].casefold(), (p["fields"].get("slug") or "").casefold())]
        if len(exact) != 1:
            raise RuntimeError(f"board {ref!r} matched {len(exact)} projects (use its PHID or #slug)")
        return exact[0]["phid"]

    async def _resolve_column(self, project_phid: str, ref: str) -> str:
        if ref.startswith("PHID-PCOL-"):
            return ref
        found = (await self.conduit.call("project.column.search", constraints={"projects": [project_phid]}))["data"]
        exact = [c for c in found if c["fields"]["name"].casefold() == ref.casefold()]
        if len(exact) != 1:
            raise RuntimeError(f"column {ref!r} matched {len(exact)} columns on the board (use its PHID)")
        return exact[0]["phid"]

    # -- inbound -----------------------------------------------------------------------------------

    async def _handle_health(self, request):
        from aiohttp import web
        return web.json_response({"status": "ok", "platform": "phabricator"})

    async def _handle_webhook(self, request):
        from aiohttp import web
        try:
            body = await request.read()
        except Exception:
            return web.Response(status=400, text="bad request")
        if len(body) > WEBHOOK_BODY_MAX_BYTES:
            return web.Response(status=413, text="payload too large")
        if not verify_signature(body, request.headers.get(SIGNATURE_HEADER, ""), self.webhook_secret):
            return web.Response(status=401, text="invalid signature")
        try:
            payload = json.loads(body.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            return web.Response(status=400, text="bad json")
        # Phabricator waits for the response and counts slow or failed deliveries against the webhook.
        task = asyncio.create_task(self._process(payload))
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)
        return web.Response(status=200, text="ok")

    async def _process(self, payload: dict) -> None:
        try:
            triggers = await self.decide(payload)
            for trigger in triggers:
                await self._dispatch(trigger)
        except Exception:
            logger.exception("Phabricator: webhook processing failed")

    async def _search_task(self, task_phid: str, **constraints: Any) -> Optional[dict]:
        found = await self.conduit.call("maniphest.search", constraints={"phids": [task_phid], **constraints})
        return found["data"][0] if found["data"] else None

    async def _transactions(self, task_phid: str) -> list[dict]:
        after, found = None, []
        while True:
            params = {"objectIdentifier": task_phid, **({"after": after} if after else {})}
            page = await self.conduit.call("transaction.search", **params)
            found += page["data"]
            after = page["cursor"]["after"]
            if not after:
                return found

    async def _username(self, phid: str) -> str:
        if phid not in self._users:
            users = (await self.conduit.call("user.search", constraints={"phids": [phid]}))["data"]
            self._users[phid] = users[0]["fields"]["username"] if users else phid
        return self._users[phid]

    def _bot_commented(self, transactions: list[dict]) -> bool:
        return any(t["authorPHID"] == self.bot_phid and t.get("comments") for t in transactions)

    async def decide(self, payload: dict) -> list[Trigger]:
        """Triggers a webhook payload warrants (empty for anything the bot should ignore)."""
        obj = payload.get("object") or {}
        if obj.get("type") != "TASK" or _is_test(payload) or not obj.get("phid"):
            return []
        task_phid = obj["phid"]
        board = {"projects": [self.board_phid]} if self.board_phid else {}
        task = await self._search_task(task_phid, **board)
        if not task:
            return []
        transactions = await self._transactions(task_phid)
        fired = {t["phid"] for t in payload.get("transactions") or []}
        fresh = [t for t in transactions if t["phid"] in fired]

        found = []
        for t in fresh:
            comment = next((c for c in t.get("comments", []) if not c.get("removed")), None)
            if not comment or t["authorPHID"] == self.bot_phid:
                continue
            raw = comment["content"]["raw"]
            if self._mention.search(raw) and not self._seen.is_duplicate(f"tx:{t['phid']}"):
                found.append(await self._trigger("mention", task, t["authorPHID"], raw, t["phid"]))
        if found:
            return found

        if self.column_phid and any(t["type"] in NEW_TRIGGER_TYPES for t in fresh):
            trigger = await self._new_trigger(task, transactions)
            if trigger:
                return [trigger]
        return []

    async def _new_trigger(self, task: dict, transactions: list[dict]) -> Optional[Trigger]:
        if self._bot_commented(transactions):
            return None
        if not await self._search_task(task["phid"], columnPHIDs=[self.column_phid], statuses=["open"]):
            return None
        if self._seen.is_duplicate(f"new:{task['phid']}"):
            return None
        description = (task["fields"].get("description") or {}).get("raw") or ""
        return await self._trigger("new", task, task["fields"]["authorPHID"], description, f"new:{task['phid']}")

    async def _trigger(self, mode: str, task: dict, author_phid: str, body: str, message_id: str) -> Trigger:
        author = await self._username(author_phid)
        title = task["fields"]["name"]
        monogram = f"T{task['id']}"
        body = body.strip()[:MAX_TEXT_CHARS]
        if mode == "mention":
            text = f"{monogram} \"{title}\"\nComment by @{author}:\n{body}"
        else:
            text = f"New task {monogram} \"{title}\" by @{author}.\n\nDescription:\n{body or '(empty)'}"
        return Trigger(mode, task["id"], task["phid"], title, author, text, message_id)

    async def _dispatch(self, trigger: Trigger) -> None:
        # The PHID (not T123) is the chat id so sessions cannot collide across Phabricator instances sharing a
        # profile. thread_id keeps every commenter in the task's one session (group sessions are otherwise per user).
        chat_id = trigger.task_phid
        source = self.build_source(
            chat_id=chat_id, chat_name=f"T{trigger.task_id}: {trigger.title}", chat_type="group",
            user_id=trigger.author, user_name=trigger.author, thread_id=chat_id, message_id=trigger.message_id)
        await self.handle_message(MessageEvent(
            text=trigger.text, source=source, message_id=trigger.message_id,
            raw_message={"mode": trigger.mode, "task_phid": trigger.task_phid},
            auto_skill=self.skill or None,
            # Task text is untrusted: it must never be read as a gateway command.
            allow_gateway_control=False, reply_expected=True,
            # A board-column trigger is an admin choice, not a user action, so it skips the user allowlist.
            internal=trigger.mode == "new"))

    # -- poller ------------------------------------------------------------------------------------

    async def _poll_loop(self) -> None:
        while self._running:
            await asyncio.sleep(self.poll_interval)
            try:
                await self.poll_once()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.warning("Phabricator: poll failed", exc_info=True)

    async def poll_once(self) -> int:
        """Dispatch unanswered open tasks in the New column modified within the lookback window."""
        constraints = {"projects": [self.board_phid], "columnPHIDs": [self.column_phid], "statuses": ["open"],
                       "modifiedStart": int(time.time()) - self.poll_lookback}
        tasks = (await self.conduit.call("maniphest.search", constraints=constraints, limit=50))["data"]
        count = 0
        for task in tasks:
            trigger = await self._new_trigger(task, await self._transactions(task["phid"]))
            if trigger:
                await self._dispatch(trigger)
                count += 1
        return count

    # -- outbound ----------------------------------------------------------------------------------

    async def send(self, chat_id: str, content: str, reply_to: Optional[str] = None,
                   metadata: Optional[dict[str, Any]] = None) -> SendResult:
        # Only the base adapter's final-reply marker becomes a comment. Busy acks, the "no home channel" nudge,
        # progress lines and gateway error notices all pass through send() too, and none of them belongs on a task.
        if not (metadata or {}).get("notify") or (metadata or {}).get("_interim_send"):
            logger.debug("Phabricator: not posting non-final send to %s: %.80s", chat_id, content)
            return SendResult(success=True)
        if self.conduit is None:
            return SendResult(success=False, error="Phabricator adapter not connected")
        if not (content or "").strip():
            return SendResult(success=True)
        try:
            result = await self.conduit.call(
                "maniphest.edit", objectIdentifier=chat_id, transactions=[{"type": "comment", "value": content}])
        except ConduitError as exc:
            return SendResult(success=False, error=str(exc))
        except Exception as exc:
            return SendResult(success=False, error=str(exc), retryable=True)
        return SendResult(success=True, message_id=(result.get("transactions") or [{}])[0].get("phid"))

    async def _notify_turn_error(self, event: MessageEvent, e: BaseException) -> Optional[dict]:
        # The base notice carries exception text and is not a final reply; post one fixed line instead.
        logger.error("Phabricator: agent run failed for %s: %s", event.source.chat_name, e)
        await self.send(event.source.chat_id, TURN_ERROR_COMMENT, metadata={"notify": True})
        return None

    async def send_typing(self, chat_id: str, metadata=None) -> None:
        return None

    async def get_chat_info(self, chat_id: str) -> dict[str, Any]:
        return {"name": chat_id, "type": "group"}


def _int(value: Any, default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


# -- registration --------------------------------------------------------------------------------

_ENV_REQUIRED = ("PHABRICATOR_URL", "PHABRICATOR_API_TOKEN", "PHABRICATOR_WEBHOOK_SECRET")
_ENV_SEED = (
    ("PHABRICATOR_URL", "url", None), ("PHABRICATOR_BOT_HANDLE", "bot_handle", None),
    ("PHABRICATOR_BOARD", "board", None), ("PHABRICATOR_NEW_COLUMN", "new_column", None),
    ("PHABRICATOR_SKILL", "skill", None), ("PHABRICATOR_HOST", "host", None),
    ("PHABRICATOR_PORT", "port", int), ("PHABRICATOR_POLL_INTERVAL", "poll_interval", int),
    ("PHABRICATOR_WEBHOOK_PATH", "webhook_path", None),
)
_YAML_BRIDGE = (
    ("url", "PHABRICATOR_URL", "str"), ("bot_handle", "PHABRICATOR_BOT_HANDLE", "str"),
    ("board", "PHABRICATOR_BOARD", "str"), ("new_column", "PHABRICATOR_NEW_COLUMN", "str"),
    ("skill", "PHABRICATOR_SKILL", "str"), ("host", "PHABRICATOR_HOST", "str"),
    ("port", "PHABRICATOR_PORT", "str"), ("poll_interval", "PHABRICATOR_POLL_INTERVAL", "str"),
    ("webhook_path", "PHABRICATOR_WEBHOOK_PATH", "str"),
    ("allowed_users", "PHABRICATOR_ALLOWED_USERS", "csv"), ("allow_all_users", "PHABRICATOR_ALLOW_ALL_USERS", "lower"),
)


def _configured() -> bool:
    return all(str(_get_scoped_secret(name, "") or "").strip() for name in _ENV_REQUIRED)


def check_requirements() -> bool:
    if not _configured():
        return False
    try:
        import aiohttp  # noqa: F401
        return True
    except ImportError:
        return False


def validate_config(config) -> bool:
    return _configured()


def _env_enablement() -> Optional[dict]:
    if not _configured():
        return None
    return _seed_extra_from_env(_ENV_SEED)


def _apply_yaml_config(yaml_cfg: dict, platform_cfg: dict) -> Optional[dict]:
    extra = platform_cfg.get("extra", platform_cfg) or {}
    return _apply_yaml_bridge(extra, _YAML_BRIDGE) if isinstance(extra, dict) else None


async def _standalone_send(pconfig, chat_id: str, message: str, *, thread_id=None, media_files=None,
                           force_document: bool = False) -> dict:
    extra = getattr(pconfig, "extra", {}) or {}
    url = str(_extra_or_secret(extra, "url", "PHABRICATOR_URL")).strip()
    token = str(_get_scoped_secret("PHABRICATOR_API_TOKEN", "") or "").strip()
    if not (url and token and chat_id):
        return send_error("Phabricator standalone send: missing URL, token or task")
    conduit = Conduit(url, token)
    try:
        await conduit.call(
            "maniphest.edit", objectIdentifier=chat_id, transactions=[{"type": "comment", "value": message}])
        return {"success": True, "message_id": None}
    except Exception as exc:
        return send_error(str(exc))
    finally:
        await conduit.close()


def register(ctx) -> None:
    ctx.register_platform(
        name="phabricator", label="Phabricator", adapter_factory=lambda cfg: PhabricatorAdapter(cfg),
        check_fn=check_requirements, validate_config=validate_config,
        is_connected=_env_is_connected(*_ENV_REQUIRED), required_env=list(_ENV_REQUIRED),
        install_hint="pip install aiohttp   # already a Hermes dependency",
        env_enablement_fn=_env_enablement, apply_yaml_config_fn=_apply_yaml_config,
        standalone_sender_fn=_standalone_send,
        allowed_users_env="PHABRICATOR_ALLOWED_USERS", allow_all_env="PHABRICATOR_ALLOW_ALL_USERS",
        max_message_length=PhabricatorAdapter.MAX_MESSAGE_LENGTH, emoji="🦅", pii_safe=False,
        platform_hint=(
            "You are replying in a Phabricator Maniphest task. Your final message is posted as ONE task comment, "
            "so do not post comments any other way. Comments render Remarkup: use **bold**, `-` bullets, "
            "`backticks` and bare URLs; do not use # headings or [text](url) links. Keep it short."))
