VALID_DEVICES = ["mobile", "desktop", "tablet"]
LOOK_SITE = [
    "/blog",
    "/contacto",
]
PIPELINE_MAKE_PURCHASE = {"/": "/productos",
                          "/productos": "/carrito", "/carrito": "/checkout"}
VALID_REFERRERS = ["direct", "google", "facebook"]
VALID_USERS = [f"u{idx:03d}" for idx in range(1, 15)]
MAX_SIZE_KB = 100
MAX_SIZE_BYTES = MAX_SIZE_KB * 1024
RATE_MAKE_PURCHASE = 30
RATE_NEW_SESSION = 30
BAD_DEVICES = ["toaster", "phon3", "desk-top", ""]
BAD_REFERRERS = [None, "(not set)", "   ", "file://local", "http://malformed"]
BAD_PATHS = ["productos", "checkout", "//double-slash", ""]
