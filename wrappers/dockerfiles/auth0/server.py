#!/usr/bin/env python3

import http.server
import socketserver
import json

class MockServerHandler(http.server.SimpleHTTPRequestHandler):
    def do_GET(self):
        # TODO: Update this server data with relevant info
        response_data = {
            "email": "john@gmail.com",
            "access_token": "<some_access_token>"
        }

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
