import requests
import json

url = "http://127.0.0.1:10399/api/v1/scan_data"

payload = json.dumps([
  {
    "a": "b"
  },
  {
    "a": "c"
  }
])
headers = {
  'Content-Type': 'application/json'
}

response = requests.request("POST", url, headers=headers, data=payload)

print(response.text)
