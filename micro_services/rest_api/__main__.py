import uvicorn

from common.config import API_HOST, API_PORT
from micro_services.rest_api.register_api import app

if __name__ == "__main__":
    uvicorn.run(app, host=API_HOST, port=API_PORT)
