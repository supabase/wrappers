from http.server import BaseHTTPRequestHandler, HTTPServer
import datetime, json
from urllib.parse import urlparse
import airtablemock

hostName = "0.0.0.0"
serverPort = 8086

# This is a client for the base "baseID", it will not access the real
# Airtable service but only the mock one which keeps data in RAM.
client = airtablemock.Airtable('baseID', 'apiKey')
test_table = 'table-foo'

class AirtableMockServer(BaseHTTPRequestHandler):
    def do_GET(self):
        path = urlparse(self.path)
        [_, base_id, table_id] = path.path.split('/')

        records = client.get(table_id)
        if records is None:
            self.send_response(404)
            return

        self.send_response(200)
        self.send_header("Content-type", "application/json")
        self.end_headers()

        for rec in records['records']:
            rec['createdTime'] = datetime.datetime.now().isoformat()

        self.wfile.write(bytes(json.dumps(records), "utf-8"))
        return


if __name__ == "__main__":
    # Populate a test table
    client.create(test_table, {'field1': 1, 'field2': 'two', 'field3': '2023-07-19T06:39:15.000Z'})

    # Create web server
    webServer = HTTPServer((hostName, serverPort), AirtableMockServer)
    print("Airtable Mock Server started at http://%s:%s" % (hostName, serverPort))

    try:
        webServer.serve_forever()
    except KeyboardInterrupt:
        pass

    webServer.server_close()
    print("Server stopped.")
