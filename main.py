from __future__ import annotations

import os

from server import app


def main() -> None:
    """Viewer-only entrypoint."""
    host = os.getenv("BNF_VIEWER_HOST", "0.0.0.0")
    port = int(os.getenv("BNF_VIEWER_PORT", "5002"))
    app.run(host=host, port=port)


if __name__ == "__main__":
    main()
