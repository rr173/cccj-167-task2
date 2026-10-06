"""HTTP API for the migration controller (stdlib only).

Plan API, cluster simulation endpoints, consumer commit endpoint, the
reconciliation page (位点对账) and the cutover proof endpoints.
"""
from __future__ import annotations

import json
import re
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from .common import MigrationError
from .controller import MigrationController
from .cluster import Cluster
from .reconcile import render_html
from .store import Store


class _Resp:
    def __init__(self, payload, status=200, content_type="application/json"):
        self.payload, self.status, self.content_type = payload, status, content_type


def _json(payload, status=200):
    return _Resp(payload, status)


def _html(markup, status=200):
    return _Resp(markup, status, "text/html; charset=utf-8")


def _routes(ctl, clusters):
    def body(h):
        return h._body

    return [
        ("GET", re.compile(r"^/$"),
         lambda h, m: _json({
             "service": "logical-slot-migration",
             "links": {"plans": "/plans", "clusters": "/clusters/{name}"}})),

        ("GET", re.compile(r"^/plans$"),
         lambda h, m: _json({"plans": ctl.list_plans()})),
        ("POST", re.compile(r"^/plans$"),
         lambda h, m: _json(ctl.create_plan(body(h)), 201)),
        ("GET", re.compile(r"^/plans/(?P<pid>[^/]+)$"),
         lambda h, m: _json(ctl.get_plan(m.group("pid")))),
        ("POST", re.compile(r"^/plans/(?P<pid>[^/]+)/freeze$"),
         lambda h, m: _json(ctl.freeze(m.group("pid")))),
        ("POST", re.compile(r"^/plans/(?P<pid>[^/]+)/start$"),
         lambda h, m: _json(ctl.start(m.group("pid")))),
        ("POST", re.compile(r"^/plans/(?P<pid>[^/]+)/catch-up$"),
         lambda h, m: _json(ctl.catch_up(m.group("pid")))),
        ("POST", re.compile(r"^/plans/(?P<pid>[^/]+)/verify$"),
         lambda h, m: _json(ctl.verify(m.group("pid")))),
        ("POST", re.compile(r"^/plans/(?P<pid>[^/]+)/cutover$"),
         lambda h, m: _json(ctl.cutover(m.group("pid")))),
        ("POST", re.compile(r"^/plans/(?P<pid>[^/]+)/retry$"),
         lambda h, m: _json(ctl.retry(m.group("pid")))),
        ("POST", re.compile(r"^/plans/(?P<pid>[^/]+)/abort$"),
         lambda h, m: _json(ctl.abort(m.group("pid")))),
        ("POST", re.compile(r"^/plans/(?P<pid>[^/]+)/release-source$"),
         lambda h, m: _json(ctl.release_source(m.group("pid")))),
        ("POST", re.compile(r"^/plans/(?P<pid>[^/]+)/revoke$"),
         lambda h, m: _json(ctl.revoke_handoff(m.group("pid")))),
        ("GET", re.compile(r"^/plans/(?P<pid>[^/]+)/revocation$"),
         lambda h, m: _json(ctl.get_revocation(m.group("pid")))),
        ("GET", re.compile(r"^/plans/(?P<pid>[^/]+)/reconcile$"),
         lambda h, m: _json(ctl.reconcile(m.group("pid")))),
        ("GET", re.compile(r"^/plans/(?P<pid>[^/]+)/reconcile\.html$"),
         lambda h, m: _html(render_html(ctl.reconcile(m.group("pid"))))),
        ("GET", re.compile(r"^/plans/(?P<pid>[^/]+)/proof$"),
         lambda h, m: _json(ctl.get_proof(m.group("pid")))),

        ("POST", re.compile(r"^/proofs/verify$"),
         lambda h, m: _json(ctl.verify_proof(body(h).get("proof", body(h))))),

        ("GET", re.compile(r"^/clusters/(?P<name>[^/]+)$"),
         lambda h, m: _json(clusters[m.group("name")].status()
                            if m.group("name") in clusters else
                            (_ for _ in ()).throw(MigrationError(
                                "NOT_FOUND", "unknown cluster", http_status=404)))),
        ("POST", re.compile(r"^/clusters/(?P<name>[^/]+)/write$"),
         lambda h, m: _json({"lsn": clusters[m.group("name")].write(
             body(h)["table"], body(h)["key"], body(h)["value"])})),
        ("POST", re.compile(r"^/clusters/(?P<name>[^/]+)/delete$"),
         lambda h, m: _json({"lsn": clusters[m.group("name")].delete(
             body(h)["table"], body(h)["key"])})),
        ("POST", re.compile(r"^/clusters/(?P<name>[^/]+)/schema-change$"),
         lambda h, m: _json({"lsn": clusters[m.group("name")].schema_change(
             body(h)["table"], body(h).get("note", ""))})),
        ("POST", re.compile(r"^/clusters/(?P<name>[^/]+)/slots$"),
         lambda h, m: _json(clusters[m.group("name")].create_slot(body(h)["name"])
                            or {"created": body(h)["name"]}, 201)),
        ("GET", re.compile(r"^/clusters/(?P<name>[^/]+)/slots/(?P<slot>[^/]+)/read$"),
         lambda h, m: _json({"entries": [
             e.to_dict() for e in clusters[m.group("name")].read_slot(
                 m.group("slot"),
                 int(h._query.get("after", ["0"])[0]),
                 int(h._query.get("limit", ["1000"])[0]))]})),

        ("POST", re.compile(r"^/groups$"),
         lambda h, m: _json(ctl.ensure_group(body(h)["group_id"], body(h)["cluster"],
                                             body(h)["slot"]), 201)),
        ("GET", re.compile(r"^/groups/(?P<gid>[^/]+)$"),
         lambda h, m: _json(ctl.get_group(m.group("gid")))),
        ("POST", re.compile(r"^/groups/(?P<gid>[^/]+)/commit$"),
         lambda h, m: _json(ctl.commit(m.group("gid"), body(h)["cluster"],
                                       body(h)["slot"], int(body(h)["lsn"]),
                                       int(body(h)["epoch"])))),
    ]


def make_handler(ctl, clusters):
    routes = _routes(ctl, clusters)

    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        # -- helpers -------------------------------------------------
        @property
        def _query(self):
            from urllib.parse import urlparse, parse_qs
            return parse_qs(urlparse(self.path).query)

        @property
        def _body(self):
            # read once per request — re-reading rfile would block forever
            if not hasattr(self, "_cached_body"):
                length = int(self.headers.get("Content-Length") or 0)
                if not length:
                    self._cached_body = {}
                else:
                    raw = self.rfile.read(length)
                    try:
                        self._cached_body = json.loads(raw.decode("utf-8"))
                    except json.JSONDecodeError:
                        raise MigrationError("BAD_REQUEST",
                                             "request body is not valid JSON")
            return self._cached_body

        def _send(self, resp: _Resp):
            if resp.content_type.startswith("application/json"):
                data = json.dumps(resp.payload, ensure_ascii=False).encode("utf-8")
            else:
                data = resp.payload.encode("utf-8")
            self.send_response(resp.status)
            self.send_header("Content-Type", resp.content_type)
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def _dispatch(self, method):
            from urllib.parse import urlparse
            path = urlparse(self.path).path
            try:
                for m, pattern, fn in routes:
                    if m != method:
                        continue
                    match = pattern.match(path)
                    if match:
                        self._send(fn(self, match))
                        return
                self._send(_json({"error": "NOT_FOUND",
                                  "message": f"no route: {method} {path}"}, 404))
            except MigrationError as exc:
                self._send(_json(exc.to_dict(), exc.http_status))
            except (KeyError, ValueError) as exc:
                self._send(_json({"error": "BAD_REQUEST", "message": str(exc)}, 400))
            except Exception as exc:  # pragma: no cover - defensive
                self._send(_json({"error": "INTERNAL", "message": str(exc)}, 500))

        def do_GET(self):
            self._dispatch("GET")

        def do_POST(self):
            self._dispatch("POST")

        def log_message(self, *args):  # keep test output clean
            pass

    return Handler


def create_server(controller, clusters, host="127.0.0.1", port=8080):
    return ThreadingHTTPServer((host, port), make_handler(controller, clusters))


def build_demo_controller(db_path=":memory:", seed=True):
    """Clusters + controller used by the CLI and the demo."""
    source, target = Cluster("source"), Cluster("target")
    clusters = {"source": source, "target": target}
    store = Store(db_path)
    ctl = MigrationController(clusters, store)
    if seed:
        source.create_slot("app_slot")
        for i in range(200):
            source.write("orders", f"ord-{i:04d}",
                         {"amount": 100 + i, "currency": "CNY"})
        for i in range(50):
            source.write("customers", f"cust-{i:03d}",
                         {"name": f"customer-{i}", "tier": i % 3})
    return ctl, clusters, store


def main(argv=None):
    import argparse
    parser = argparse.ArgumentParser(description="logical slot migration API")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8080)
    parser.add_argument("--db", default=":memory:", help="controller state DB path")
    parser.add_argument("--no-seed", action="store_true")
    args = parser.parse_args(argv)

    ctl, clusters, _ = build_demo_controller(args.db, seed=not args.no_seed)
    server = create_server(ctl, clusters, args.host, args.port)
    print(f"API listening on http://{args.host}:{args.port}")
    print("try:  curl -s localhost:%d/plans | python3 -m json.tool" % args.port)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
