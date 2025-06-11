from fastapi.testclient import TestClient
from src.api_service.register_api import app

client = TestClient(app)

def test_register_callback():
    response = client.post("/register", json={
        "id": "123",
        "url_callback": "http://karnaf/callback",
        "time": 10
    })
    assert response.status_code == 200

