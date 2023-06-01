import socket


def findFreePort() -> int:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM, 0)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind(("localhost", 0))  # with port=0 the OS will find a random free port
    port = sock.getsockname()[1]
    sock.close()
    return port
