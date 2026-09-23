"""Bounded HTTP/1.1 transport used only for scheduled measurements."""
import httpx
import httpcore
from collections import defaultdict, deque
from types import MethodType


def _origin_key(origin):
    return origin.scheme, origin.host, origin.port


def _assign_http1(pool):
    # A connection can still report idle between assignment and starting I/O.
    # Reserve it until the owning response leaves the pool, including across
    # separate calls here. This transport never enables HTTP/2 multiplexing.
    reserved={r.connection for r in pool._requests if r.connection is not None}
    retained=[];idle=deque();available=defaultdict(deque);closing=[]
    for connection in pool._connections:
        if connection.is_closed():
            continue
        if connection.has_expired():
            closing.append(connection)
            continue
        unclaimed=connection not in reserved
        if unclaimed and connection.is_idle():
            if len(idle)>=pool._max_keepalive_connections:
                closing.append(connection)
                continue
            idle.append(connection)
        retained.append(connection)
        if unclaimed and connection.is_available():
            available[_origin_key(connection._origin)].append(connection)
    live=set(retained)
    for request in pool._requests:
        if not request.is_queued():
            continue
        origin=request.request.url.origin
        candidates=available[_origin_key(origin)]
        while candidates and (candidates[0] not in live or candidates[0] in reserved):
            candidates.popleft()
        if candidates:
            connection=candidates.popleft()
        else:
            if len(live)>=pool._max_connections:
                while idle and (idle[0] not in live or idle[0] in reserved):
                    idle.popleft()
                if not idle:
                    continue
                obsolete=idle.popleft()
                live.remove(obsolete);closing.append(obsolete)
            connection=pool.create_connection(origin)
            retained.append(connection);live.add(connection)
        reserved.add(connection)
        request.assign_to_connection(connection)
    pool._connections=[c for c in retained if c in live]
    return closing


def measured_transport(limits):
    # Version-bounded, per-instance adapter for the measured hot path. No global
    # monkeypatch or replacement of TLS, network policy, I/O, or error handling.
    # Background: https://github.com/encode/httpcore/pull/1035
    if httpx.__version__!='0.28.1' or httpcore.__version__!='1.0.9':
        raise RuntimeError('Measured transport requires httpx 0.28.1 / httpcore 1.0.9')
    transport=httpx.AsyncHTTPTransport(trust_env=False, limits=limits, http2=False, retries=0)
    transport._pool._assign_requests_to_connections=MethodType(_assign_http1,transport._pool)
    return transport
