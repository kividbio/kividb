"""
Root conftest — imports fixtures so pytest discovers them across all modules.
"""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__)))

import redis

from utils.helpers import active_protocol  # noqa: E402

# This suite runs against BOTH RESP2 and RESP3 (see the `resp_protocol` fixture
# in utils/helpers.py). Set KIVI_RESP=2 or KIVI_RESP=3 to restrict a run to one.
#
# It previously pinned every client to RESP2 unconditionally, because kividb
# implemented RESP2 only while redis-py's default had already moved to RESP3 —
# so an ordinary `pip install` picking up a newer redis-py silently negotiated a
# protocol the server couldn't speak and broke the suite at handshake. kividb
# now implements RESP3, so the hard pin became a parametrization: RESP2 must
# keep behaving exactly as it did, and the only way to know that is to keep
# running the suite against it too.
#
# The default is still patched in here rather than left to redis-py, so that a
# bare `redis.Redis(...)` constructed inside a test body follows the protocol
# under test instead of whichever default the installed redis-py happens to
# have. Callers passing `protocol=` explicitly are unaffected.
_original_redis_init = redis.Redis.__init__


def _protocol_aware_init(self, *args, **kwargs):
    kwargs.setdefault("protocol", active_protocol())
    _original_redis_init(self, *args, **kwargs)


redis.Redis.__init__ = _protocol_aware_init

from utils.helpers import (  # noqa: F401,E402  (re-export as fixtures)
    kivi_server,
    r,
    rb,
    r_pool,
    resp_protocol,
)
