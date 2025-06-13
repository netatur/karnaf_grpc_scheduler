import asyncio

from common.config import GRPC_PORT
from micro_services.grpc_server import grpc_server

if __name__ == "__main__":
    asyncio.run(grpc_server.serve(GRPC_PORT))
