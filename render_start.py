diff --git a/render_start.py b/render_start.py
new file mode 100644
index 0000000000000000000000000000000000000000..69ea7b71d91794db4727aee6a753ca3977b785fd
--- /dev/null
+++ b/render_start.py
@@ -0,0 +1,27 @@
+"""Render-compatible entrypoint to serve the Flask app via Waitress.
+
+This keeps the start command simple (`python render_start.py`) so even if the
+Render service was originally configured with a different WSGI module path, we
+can bootstrap the exact same application that `app_flask.py` exposes.
+"""
+from __future__ import annotations
+
+import os
+import logging
+
+from waitress import serve
+
+from app_flask import app
+
+log = logging.getLogger("render")
+
+
+def main() -> None:
+    port = int(os.getenv("PORT", "5000"))
+    listen = f"0.0.0.0:{port}"
+    log.info("Starting Waitress on %s", listen)
+    serve(app, listen=listen)
+
+
+if __name__ == "__main__":
+    main()
