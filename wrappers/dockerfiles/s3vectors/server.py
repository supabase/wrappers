from http.server import BaseHTTPRequestHandler, HTTPServer
import datetime, json, logging
from urllib.parse import urlparse, parse_qs

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

hostName = "0.0.0.0"
serverPort = 4444

# mock API server for S3 Vectors FDW testing
#
# Note: this mock server is for smoke testing purpose so it provides hard coded
#       response only.
class MockServer(BaseHTTPRequestHandler):
    def log_request_details(self, method, body=''):
        # Log detailed request information
        parsed_url = urlparse(self.path)
        query_params = parse_qs(parsed_url.query)

        logging.info(f"=== {method} Request Details ===")
        logging.info(f"Path: {self.path}")
        logging.info(f"Parsed URL: {parsed_url}")
        logging.info(f"Query Parameters: {query_params}")
        logging.info(f"Headers: {dict(self.headers)}")
        if body:
            logging.info(f"Request Body: {body}")
        logging.info(f"Client Address: {self.client_address}")
        logging.info("=" * 30)

    def response(self, body):
        self.send_response(200)
        self.send_header("Content-type", "application/json")
        self.end_headers()
        self.wfile.write(bytes(body, "utf-8"))

    def do_GET(self):
        self.response("All good")
        return

    def do_POST(self):
        # Get request body first before logging
        content_length = int(self.headers.get('Content-Length', 0))
        req_body = ''
        if content_length > 0:
            req_body = self.rfile.read(content_length).decode('utf-8')

        self.log_request_details("POST", req_body)

        if self.path == "/ListIndexes":
            body = """
{
   "indexes": [
      {
         "creationTime": 1757771241,
         "indexArn": "arn:aws:s3vectors:us-east-1:213505683208:bucket/my-vector-bucket/index/my-vector-index",
         "indexName": "my-vector-index",
         "vectorBucketName": "my-vector-bucket"
      }
   ]
}
            """
        elif self.path == "/ListVectors":
            if "nextToken" in req_body:
                body = """
{
   "vectors": [
      {
         "data": { "float32": [ 222.33, 44.55, 6.678 ] },
         "key": "test-key3",
         "metadata": { "model": "test", "dimension": 3 }
      }
   ]
}
                """
            else:
                body = """
{
   "nextToken": "xxx",
   "vectors": [
      {
         "data": { "float32": [ 123.45, 67.89, 0.0 ] },
         "key": "test-key1",
         "metadata": { "model": "test", "dimension": 3 }
      },
      {
         "data": { "float32": [ 987.65, 43.21, -4.2 ] },
         "key": "test-key2",
         "metadata": { "model": "test", "dimension": 3 }
      }
   ]
}
                """
        elif self.path == "/QueryVectors":
            body = """
{
   "vectors": [
      {
         "data": { "float32": [ 123.45, 67.89, 0.0 ] },
         "distance": 0.84733,
         "key": "test-key1",
         "metadata": { "model": "test", "dimension": 3 }
      },
      {
         "data": { "float32": [ 987.65, 43.21, -4.2 ] },
         "distance": 0.32804,
         "key": "test-key2",
         "metadata": { "model": "test", "dimension": 3 }
      }
   ]
}
            """
        elif self.path == "/GetVectors":
            body = """
{
   "vectors": [
      {
         "data": { "float32": [ 123.45, 67.89, 0.0 ] },
         "distance": 0.84733,
         "key": "test-key1",
         "metadata": { "model": "test", "dimension": 3 }
      }
   ]
}
            """
        elif self.path == "/PutVectors":
            body = """
            """
        elif self.path == "/DeleteVectors":
            body = """
            """
        else:
            self.send_response(404)
            return

        self.response(body)
        return


if __name__ == "__main__":
    # Create web server
    webServer = HTTPServer((hostName, serverPort), MockServer)
    print("S3 Vectors Mock Server started at http://%s:%s" % (hostName, serverPort))

    try:
        webServer.serve_forever()
    except KeyboardInterrupt:
        pass

    webServer.server_close()
    print("Server stopped.")

