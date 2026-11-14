"""In-memory fake of the Conduit methods the adapter uses (also runnable as a script for manual tests)."""
import itertools
import time

from aiohttp import web

BOT = "PHID-USER-bot"
HUMAN = "PHID-USER-human"
BOARD = "PHID-PROJ-board"
OTHER_BOARD = "PHID-PROJ-other"
NEW_COLUMN = "PHID-PCOL-new"
DONE_COLUMN = "PHID-PCOL-done"
TOKEN = "api-dummy"

USERS = {BOT: "triagebot", HUMAN: "alice"}


def parse(form) -> dict:
    """Undo Conduit bracket keys: a[b][0]=x -> {"a": {"b": ["x"]}}."""
    root: dict = {}
    for key, value in form.items():
        parts = key.replace("]", "").split("[")
        node = root
        for part in parts[:-1]:
            node = node.setdefault(part, {})
        node[parts[-1]] = value

    def listify(node):
        if not isinstance(node, dict):
            return node
        node = {k: listify(v) for k, v in node.items()}
        if node and all(k.isdigit() for k in node):
            return [node[k] for k in sorted(node, key=int)]
        return node

    return listify(root)


class FakeConduit:
    def __init__(self):
        self.tasks: dict[str, dict] = {}
        self.transactions: dict[str, list[dict]] = {}
        self.calls: list[tuple[str, dict]] = []
        self._ids = itertools.count(1)

    def add_task(self, id_, title="Calls fail", projects=(BOARD,), column=NEW_COLUMN, status="open",
                 author=HUMAN, description="Calls drop after 5s"):
        phid = f"PHID-TASK-{id_}"
        self.tasks[phid] = {"id": id_, "phid": phid, "projects": list(projects), "column": column, "status": status,
                            "fields": {"name": title, "authorPHID": author, "description": {"raw": description},
                                       "dateModified": int(time.time())}}
        self.transactions[phid] = []
        return phid

    def add_transaction(self, task_phid, type_="comment", author=HUMAN, comment=None):
        phid = f"PHID-XACT-{next(self._ids)}"
        comments = [{"removed": False, "content": {"raw": comment}}] if comment is not None else []
        self.transactions[task_phid].append({"phid": phid, "type": type_, "authorPHID": author, "comments": comments})
        return phid

    def comments_by(self, task_phid, author):
        return [c["content"]["raw"] for t in self.transactions[task_phid] if t["authorPHID"] == author
                for c in t["comments"]]

    def _task_by_ref(self, ref):
        for task in self.tasks.values():
            if ref in (task["phid"], f"T{task['id']}"):
                return task

    def result(self, method, p):
        if method == "user.whoami":
            return {"phid": BOT, "userName": USERS[BOT]}
        if method == "user.search":
            return {"data": [{"phid": x, "fields": {"username": USERS[x]}} for x in p["constraints"]["phids"] if x in USERS]}
        if method == "project.search":
            c = p["constraints"]
            projects = [{"phid": BOARD, "fields": {"name": "Support Board", "slug": "support_board"}},
                        {"phid": OTHER_BOARD, "fields": {"name": "Other", "slug": "other"}}]
            return {"data": [x for x in projects if x["fields"]["slug"] in c.get("slugs", [x["fields"]["slug"]])
                             and c.get("query", x["fields"]["name"]).lower() in x["fields"]["name"].lower()]}
        if method == "project.column.search":
            return {"data": [{"phid": NEW_COLUMN, "fields": {"name": "New"}}, {"phid": DONE_COLUMN, "fields": {"name": "Done"}}]}
        if method == "maniphest.search":
            c = p["constraints"]
            data = []
            for task in self.tasks.values():
                if "phids" in c and task["phid"] not in c["phids"]:
                    continue
                if "projects" in c and not set(c["projects"]) & set(task["projects"]):
                    continue
                if "columnPHIDs" in c and task["column"] not in c["columnPHIDs"]:
                    continue
                if "statuses" in c and task["status"] not in c["statuses"]:
                    continue
                if "modifiedStart" in c and task["fields"]["dateModified"] < int(c["modifiedStart"]):
                    continue
                data.append({"id": task["id"], "phid": task["phid"], "fields": {**task["fields"], "status": {"value": task["status"]}}})
            return {"data": data}
        if method == "transaction.search":
            return {"data": self.transactions[p["objectIdentifier"]], "cursor": {"after": None}}
        if method == "maniphest.edit":
            task = self._task_by_ref(p["objectIdentifier"])
            if task is None:
                raise KeyError("ERR-INVALID-PARAMETER")
            tx = p["transactions"][0]
            assert tx["type"] == "comment"
            phid = self.add_transaction(task["phid"], author=BOT, comment=tx["value"])
            return {"object": {"phid": task["phid"]}, "transactions": [{"phid": phid}]}
        raise AssertionError(f"unexpected method {method}")

    async def handle(self, request):
        form = parse(await request.post())
        method = request.match_info["method"]
        self.calls.append((method, form))
        if form.get("api.token") != TOKEN:
            return web.json_response({"result": None, "error_code": "ERR-INVALID-AUTH", "error_info": "bad token"})
        try:
            return web.json_response({"result": self.result(method, form), "error_code": None, "error_info": None})
        except KeyError as exc:
            return web.json_response({"result": None, "error_code": str(exc.args[0]), "error_info": "nope"})

    def app(self):
        app = web.Application()
        app.router.add_post("/api/{method}", self.handle)
        return app


if __name__ == "__main__":
    # Manual mode: POST /ctl/task {"id":7,"projects":[...]} and /ctl/tx {"task":"PHID-TASK-7","type":"comment",
    # "author":"PHID-USER-human","comment":"@triagebot hi"}; GET /ctl/state shows every transaction.
    import sys
    fake = FakeConduit()

    async def ctl_task(request):
        return web.json_response({"phid": fake.add_task(**(await request.json()))})

    async def ctl_tx(request):
        data = await request.json()
        return web.json_response({"phid": fake.add_transaction(data.pop("task"), **data)})

    async def ctl_state(request):
        return web.json_response(fake.transactions)

    app = fake.app()
    app.router.add_post("/ctl/task", ctl_task)
    app.router.add_post("/ctl/tx", ctl_tx)
    app.router.add_get("/ctl/state", ctl_state)
    web.run_app(app, host="127.0.0.1", port=int(sys.argv[1]) if len(sys.argv) > 1 else 18999)
