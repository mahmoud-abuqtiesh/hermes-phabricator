#!/usr/bin/env python3
"""Run: HERMES_SRC=/path/to/hermes-agent <hermes venv python> tests/test_adapter.py

Needs a Hermes source tree on HERMES_SRC (default ~/.hermes/hermes-agent) and its Python environment.
"""
import asyncio
import contextlib
import hashlib
import hmac
import json
import os
import socket
import sys
import tempfile
from pathlib import Path

HERE = Path(__file__).resolve().parent
os.environ.setdefault("HERMES_HOME", tempfile.mkdtemp(prefix="phab-plugin-test-"))
sys.path.insert(0, os.environ.get("HERMES_SRC", str(Path.home() / ".hermes/hermes-agent")))
sys.path.insert(0, str(HERE.parent))
sys.path.insert(0, str(HERE))

from aiohttp import ClientSession  # noqa: E402
from aiohttp.test_utils import TestServer  # noqa: E402
from gateway.config import PlatformConfig  # noqa: E402

import adapter as phab  # noqa: E402
import fake_conduit as fc  # noqa: E402

SECRET = "webhook-dummy"


class _Ctx:
    """Stands in for the plugin manager's context: builds the same PlatformEntry (validates register()'s kwargs)."""

    def register_platform(self, name, label, adapter_factory, check_fn, validate_config=None, required_env=None,
                          install_hint="", **kwargs):
        from gateway.platform_registry import PlatformEntry, platform_registry
        platform_registry.register(PlatformEntry(
            name=name, label=label, adapter_factory=adapter_factory, check_fn=check_fn,
            validate_config=validate_config, required_env=required_env or [], install_hint=install_hint,
            source="plugin", **kwargs))


phab.register(_Ctx())


def sign(body: bytes, secret: str = SECRET) -> str:
    return hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()


def event(task, *tx_phids, type_="TASK", action=None):
    action = action if action is not None else {"test": False, "silent": False, "secure": False, "epoch": 1}
    return {"object": {"type": type_, "phid": task}, "triggers": [], "action": action,
            "transactions": [{"phid": p} for p in tx_phids]}


@contextlib.asynccontextmanager
async def harness(**extra):
    fake = fc.FakeConduit()
    server = TestServer(fake.app())
    await server.start_server()
    os.environ.update(PHABRICATOR_API_TOKEN=fc.TOKEN, PHABRICATOR_WEBHOOK_SECRET=SECRET,
                      PHABRICATOR_URL=str(server.make_url("")).rstrip("/"))
    cfg = {"board": "Support Board", "new_column": "New", **extra}
    adapter = phab.PhabricatorAdapter(PlatformConfig(enabled=True, extra={k: v for k, v in cfg.items() if v}))
    adapter.conduit = phab.Conduit(adapter.url, adapter.token)
    try:
        await adapter._resolve_identity()
    except Exception:
        await adapter.conduit.close()
        await server.close()
        raise
    adapter.events = []

    async def collect(ev):
        adapter.events.append(ev)

    adapter.handle_message = collect
    try:
        yield adapter, fake
    finally:
        await adapter.conduit.close()
        await server.close()


def run(coro):
    asyncio.run(coro)


async def modes(adapter, payload):
    return [t.mode for t in await adapter.decide(payload)]


def test_helpers():
    pattern = phab.mention_pattern("triagebot")
    for text in ("@triagebot why?", "hey @TriageBot.", "(@triagebot)", "x\n@triagebot"):
        assert pattern.search(text), text
    for text in ("@triagebotty", "@triagebot-two", "@triagebot.two", "mail@triagebot", "@@triagebot", "triagebot"):
        assert not pattern.search(text), text
    assert phab.verify_signature(b"x", sign(b"x"), SECRET)
    assert phab.verify_signature(b"x", sign(b"x").upper(), SECRET)
    assert not phab.verify_signature(b"x", sign(b"x", "other"), SECRET)
    assert not phab.verify_signature(b"x", "", SECRET)
    assert not phab.verify_signature(b"x", sign(b"x"), "")
    assert phab.flatten({"constraints": {"phids": ["a", "b"]}, "limit": 5}) == [
        ("constraints[phids][0]", "a"), ("constraints[phids][1]", "b"), ("limit", "5")]


async def _identity():
    async with harness() as (adapter, fake):
        assert adapter.bot_phid == fc.BOT and adapter.handle == "triagebot"
        assert adapter.board_phid == fc.BOARD and adapter.column_phid == fc.NEW_COLUMN
    async with harness(board="#support_board", new_column=fc.NEW_COLUMN, bot_handle="@helper") as (adapter, fake):
        assert adapter.board_phid == fc.BOARD and adapter.column_phid == fc.NEW_COLUMN and adapter.handle == "helper"
    async with harness(board=fc.OTHER_BOARD, new_column="") as (adapter, fake):
        assert adapter.board_phid == fc.OTHER_BOARD and adapter.column_phid == ""
    try:
        async with harness(board="Nonexistent"):
            raise AssertionError("unknown board must fail")
    except RuntimeError as exc:
        assert "matched 0" in str(exc)


async def _decisions():
    async with harness() as (adapter, fake):
        task = fake.add_task(7)
        created = fake.add_transaction(task, "create")
        assert await modes(adapter, event(task, created)) == ["new"]
        # same event again: already in flight, no second run
        assert await modes(adapter, event(task, created)) == []

    async with harness() as (adapter, fake):
        task = fake.add_task(7)
        moved = fake.add_transaction(task, None)
        assert await modes(adapter, event(task, moved)) == ["new"], "a column move has transaction type null"

    async with harness() as (adapter, fake):
        task = fake.add_task(7, column=fc.DONE_COLUMN)
        assert await modes(adapter, event(task, fake.add_transaction(task, "create"))) == []

    async with harness() as (adapter, fake):
        task = fake.add_task(7, projects=(fc.OTHER_BOARD,))
        assert await modes(adapter, event(task, fake.add_transaction(task, "create"))) == []
        mention = fake.add_transaction(task, comment="@triagebot look")
        assert await modes(adapter, event(task, mention)) == [], "off-board mention must be ignored"

    async with harness() as (adapter, fake):
        task = fake.add_task(7, status="resolved")
        assert await modes(adapter, event(task, fake.add_transaction(task, "create"))) == []

    async with harness() as (adapter, fake):
        task = fake.add_task(7)
        fake.add_transaction(task, "comment", fc.BOT, "first pass")
        assert await modes(adapter, event(task, fake.add_transaction(task, "projects"))) == []

    async with harness() as (adapter, fake):
        task = fake.add_task(7)
        for kind in ("title", "priority", "comment"):
            assert await modes(adapter, event(task, fake.add_transaction(task, kind))) == []
        created = fake.add_transaction(task, "create")
        assert await modes(adapter, event(task, created, type_="DREV")) == []
        assert await modes(adapter, event(task, created, action={"test": True})) == []
        assert await modes(adapter, event(task, created, action="test")) == []
        assert await modes(adapter, event("PHID-TASK-404", created)) == []

    async with harness() as (adapter, fake):
        task = fake.add_task(7)
        ask = fake.add_transaction(task, comment="@TriageBot why does this fail?")
        triggers = await adapter.decide(event(task, ask))
        assert [t.mode for t in triggers] == ["mention"]
        assert triggers[0].author == "alice" and triggers[0].task_id == 7
        assert "why does this fail?" in triggers[0].text and "T7" in triggers[0].text
        assert await modes(adapter, event(task, ask)) == [], "redelivered transaction must not rerun"
        assert await modes(adapter, event(task, fake.add_transaction(task, comment="cc @triagebotty"))) == []
        assert await modes(adapter, event(task, fake.add_transaction(task, comment="@triagebot hi", author=fc.BOT))) == [], \
            "the bot's own comment must be ignored"
        removed = fake.add_transaction(task, comment="@triagebot gone")
        fake.transactions[task][-1]["comments"][0]["removed"] = True
        assert await modes(adapter, event(task, removed)) == []
        fake.add_transaction(task, comment="@triagebot not in this payload")
        assert await modes(adapter, event(task)) == []

    async with harness(new_column="") as (adapter, fake):
        task = fake.add_task(7)
        assert await modes(adapter, event(task, fake.add_transaction(task, "create"))) == [], "mentions-only without a column"
        assert await modes(adapter, event(task, fake.add_transaction(task, comment="@triagebot go"))) == ["mention"]

    async with harness(board="", new_column="") as (adapter, fake):
        task = fake.add_task(7, projects=())
        assert await modes(adapter, event(task, fake.add_transaction(task, comment="@triagebot go"))) == ["mention"], \
            "no board configured means any task"


async def _dispatch_and_session_key():
    async with harness(skill="triage") as (adapter, fake):
        task = fake.add_task(7)
        first = fake.add_transaction(task, comment="@triagebot one")
        second = fake.add_transaction(task, comment="@triagebot two", author=fc.HUMAN)
        await adapter._process(event(task, first))
        await adapter._process(event(task, second))
        created = fake.add_task(8)
        await adapter._process(event(created, fake.add_transaction(created, "create")))
        a, b, c = adapter.events
        assert a.source.chat_id == b.source.chat_id == "T7" and c.source.chat_id == "T8"
        assert a.source.thread_id == "T7" and a.source.chat_type == "group"
        assert a.source.user_id == "alice" and a.auto_skill == "triage"
        assert a.allow_gateway_control is False and a.internal is False and c.internal is True
        assert "Calls drop after 5s" in c.text and "T8" in c.text
        from gateway.session import build_session_key
        assert build_session_key(a.source) == build_session_key(b.source) != build_session_key(c.source)
        other = adapter.build_source(chat_id="T7", chat_type="group", user_id="bob", thread_id="T7")
        assert build_session_key(other) == build_session_key(a.source), "another commenter shares the task session"


async def _send():
    async with harness() as (adapter, fake):
        task = fake.add_task(7)
        final = {"notify": True}
        result = await adapter.send("T7", "**Verdict**\n- one", metadata=final)
        assert result.success and result.message_id
        assert fake.comments_by(task, fc.BOT) == ["**Verdict**\n- one"]
        assert (await adapter.send("T7", "progress", metadata={"notify": True, "_interim_send": True})).success
        assert (await adapter.send("T7", "busy ack", metadata={"thread_id": "T7"})).success
        assert (await adapter.send("T7", "no home channel")).success
        assert (await adapter.send("T7", "   ", metadata=final)).success
        assert len(fake.comments_by(task, fc.BOT)) == 1, "interim, unmarked and blank sends post nothing"
        missing = await adapter.send("T999", "x", metadata=final)
        assert not missing.success and not missing.retryable and "ERR-INVALID-PARAMETER" in missing.error
        adapter.conduit._token = "wrong"
        assert not (await adapter.send("T7", "x", metadata=final)).success
        adapter.conduit._token = fc.TOKEN
        adapter.conduit.base_url = "http://127.0.0.1:1"
        down = await adapter.send("T7", "x", metadata=final)
        assert not down.success and down.retryable


async def _poller():
    async with harness() as (adapter, fake):
        fresh = fake.add_task(7)
        answered = fake.add_task(8)
        fake.add_transaction(answered, "comment", fc.BOT, "done already")
        stale = fake.add_task(9)
        fake.tasks[stale]["fields"]["dateModified"] = 1
        other_column = fake.add_task(10, column=fc.DONE_COLUMN)
        assert await adapter.poll_once() == 1
        assert [e.source.chat_id for e in adapter.events] == ["T7"]
        assert await adapter.poll_once() == 0, "polling twice must not rerun a task"
        created = fake.add_transaction(fresh, "create")
        assert await modes(adapter, event(fresh, created)) == [], "webhook after poll must not rerun"
        del other_column


async def _http_end_to_end():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]
    async with harness(port=port) as (adapter, fake):
        task = fake.add_task(7)
        ask = fake.add_transaction(task, comment="@triagebot status?")
        from aiohttp import web
        adapter._app = adapter._build_app(web)
        from gateway.platforms.shared_ingress import bind_listener
        adapter._runner = await bind_listener(adapter, adapter._app, "127.0.0.1", port, adapter.path)
        adapter._running = True
        url = f"http://127.0.0.1:{port}{phab.DEFAULT_PATH}"
        body = json.dumps(event(task, ask)).encode()
        async with ClientSession() as http:
            async def post(data, signature):
                async with http.post(url, data=data, headers={phab.SIGNATURE_HEADER: signature}) as r:
                    return r.status
            assert await post(body, "bad") == 401
            assert await post(body, "") == 401
            assert await post(body, sign(body, "other")) == 401
            assert await post(b"{", sign(b"{")) == 400
            await asyncio.sleep(0.2)
            assert adapter.events == [], "unsigned requests must not reach the agent"
            assert await post(body, sign(body)) == 200
            for _ in range(50):
                if adapter.events:
                    break
                await asyncio.sleep(0.05)
            assert [e.source.chat_id for e in adapter.events] == ["T7"]
            async with http.get(url + "/health") as r:
                assert r.status == 200
        await adapter._runner.cleanup()


async def _check_requirements_and_enablement():
    saved = {k: os.environ.pop(k, None) for k in phab._ENV_REQUIRED}
    try:
        assert not phab.check_requirements() and phab._env_enablement() is None
        os.environ.update(PHABRICATOR_URL="https://p.example", PHABRICATOR_API_TOKEN="t",
                          PHABRICATOR_WEBHOOK_SECRET="s", PHABRICATOR_PORT="9001")
        assert phab.check_requirements()
        seeded = phab._env_enablement()
        assert seeded["url"] == "https://p.example" and seeded["port"] == 9001
        os.environ.pop("PHABRICATOR_BOARD", None)
        bridged = phab._apply_yaml_config({}, {"extra": {"board": "Support", "allowed_users": ["a", "b"]}})
        assert bridged == {"board": "Support", "allowed_users": ["a", "b"]}
        assert os.environ.pop("PHABRICATOR_BOARD") == "Support" and os.environ.pop("PHABRICATOR_ALLOWED_USERS") == "a,b"
    finally:
        for key, value in saved.items():
            os.environ.pop(key, None)
            if value is not None:
                os.environ[key] = value


def test_identity():
    run(_identity())


def test_decisions():
    run(_decisions())


def test_dispatch_and_session_key():
    run(_dispatch_and_session_key())


def test_send():
    run(_send())


def test_poller():
    run(_poller())


def test_http_end_to_end():
    run(_http_end_to_end())


def test_requirements_and_enablement():
    run(_check_requirements_and_enablement())


if __name__ == "__main__":
    for name, fn in list(globals().items()):
        if name.startswith("test_"):
            fn()
            print("ok", name)
