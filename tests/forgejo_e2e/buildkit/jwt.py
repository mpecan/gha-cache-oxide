# Unsigned-verification JWT shaped like Forgejo's runtime token: go-actions-cache
# only ParseUnverified's it and needs `ac`, `exp`, `nbf`.
import base64, json, time, hmac, hashlib
b = lambda d: base64.urlsafe_b64encode(json.dumps(d, separators=(",", ":")).encode()).rstrip(b"=")
now = int(time.time())
h = b({"alg": "HS256", "typ": "JWT"})
p = b({"ac": json.dumps([{"Scope": "", "Permission": 3}]), "exp": now + 3600, "nbf": now - 60, "scp": "Actions.Results:1:1"})
sig = base64.urlsafe_b64encode(hmac.new(b"x", h + b"." + p, hashlib.sha256).digest()).rstrip(b"=")
print((h + b"." + p + b"." + sig).decode())
