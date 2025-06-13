import requests

url = "http://localhost:8081/register"

data = {"id": "12345", "url_callback": "http://karnaf/callback", "time": 200}

response = requests.post(url, json=data)

print(f"Status Code: {response.status_code}")
print(f"Response: {response.text}")
