from server import grpc_server
from common.config import GRPC_PORT

if __name__ == "__main__":
    grpc_server.serve(GRPC_PORT)
