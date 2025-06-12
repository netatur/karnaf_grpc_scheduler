import requests

# Define the URL of the FastAPI server
url = "http://localhost:8081/register"

# Data that matches the CallbackRequest schema
data = {
    "id": "12345",
    "url_callback": "http://example.com/callback",
    "time": 100
}

# Send the POST request
response = requests.post(url, json=data)

# Print the status code and the response content
print(f"Status Code: {response.status_code}")
print(f"Response: {response.text}")
