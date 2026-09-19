import os

os.environ["DRA_API_KEY"] = ""
from web.backend.api import router

print("router.routes:", [getattr(r, "path", r) for r in router.routes])

import web.server

app = web.server.app
print("\napp.routes:")
for r in app.routes:
    print("  ", type(r).__name__, getattr(r, "path", None))
