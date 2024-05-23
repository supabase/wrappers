#!/usr/bin/env python3

import http.server
import socketserver
import json

class MockServerHandler(http.server.SimpleHTTPRequestHandler):
    def do_GET(self):
        response_data = {"start":0,"limit":50,"length":1, "users": [{
            "email": "john@doe.com",
            "email_verified": True,
            "user_id": "auth0|1234567890abcdef",
            "username": "userexample",
            "phone_number": "123-456-7890",
            "phone_verified": True,
            "created_at": "2023-05-16T07:41:08.028Z",
            "updated_at": "2023-05-16T08:41:08.028Z",
            "identities": [{
                "connection": "Username-Password-Authentication",
                "user_id": "1234567890abcdef",
                "provider": "auth0",
                "isSocial": False
            }],
            "app_metadata": {},
            "user_metadata": {},
            "picture": "https://example.com/avatar.jpg",
            "name": "John Doe",
            "nickname": "Johnny",
            "multifactor": [],
            "last_ip": "192.168.1.1",
            "last_login": "2023-05-16T08:41:08.028Z",
            "logins_count": 1,
            "blocked": False,
            "given_name": "John",
            "family_name": "Doe"
        }]}

        # Set response code and headers
        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.end_headers()

        # Send the response data
        self.wfile.write(json.dumps(response_data).encode())



if __name__ == "__main__":
    # Define server address and port
    port = 3796
    server_address = ('', port)
    # Create an HTTP server instance
    httpd = socketserver.TCPServer(server_address, MockServerHandler)
    print(f"Auth0 mock Server running on port {port}...")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    httpd.server_close()
    print("Server stopped")
