from __future__ import annotations

import json
import os
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


def build_health_payload() -> dict[str, str]:
    """Return a lightweight health payload for the pipeline-worker container."""

    database_url = os.getenv("DATABASE_URL", "")
    backend = "sqlite"
    if database_url.lower().startswith(("postgres://", "postgresql://")):
        backend = "postgres"
    return {"status": "ok", "service": "pipeline-worker", "backend": backend}


class _HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:  # noqa: N802 - stdlib handler signature
        if self.path != "/health":
            self.send_error(HTTPStatus.NOT_FOUND)
            return

        payload = json.dumps(build_health_payload()).encode("utf-8")
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def log_message(self, format: str, *args) -> None:  # noqa: A003 - stdlib handler signature
        return


def main() -> int:
    port = int(os.getenv("WORKER_HEALTH_PORT", "8090"))
    server = ThreadingHTTPServer(("0.0.0.0", port), _HealthHandler)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
