import contextlib
import socket
import threading
import time
import pytest
import uvicorn


@pytest.fixture
def asgi_server():
    servers = []
    def start(app):
        sock = socket.socket()
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
        server = uvicorn.Server(uvicorn.Config(app, log_level="error", access_log=False))
        thread = threading.Thread(target=server.run, kwargs={"sockets": [sock]}, daemon=True)
        thread.start()
        for _ in range(200):
            if server.started:
                break
            time.sleep(.01)
        else:
            raise RuntimeError("Test HTTP server failed to start")
        servers.append((server, thread, sock))
        return f"http://127.0.0.1:{port}"
    yield start
    for server, thread, sock in reversed(servers):
        server.should_exit = True
        thread.join(timeout=10)
        sock.close()


def pytest_collection_modifyitems(items):
    for item in items:
        if not any(item.get_closest_marker(m) for m in ("gpu", "live_provider")):
            item.add_marker(pytest.mark.cpu)
