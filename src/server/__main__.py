import asyncio

from common.config import GRPC_PORT
from server import grpc_server

if __name__ == "__main__":
    asyncio.run(grpc_server.serve(GRPC_PORT))
