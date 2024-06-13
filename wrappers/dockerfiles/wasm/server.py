from http.server import BaseHTTPRequestHandler, HTTPServer
import datetime, json
from urllib.parse import urlparse, parse_qs

hostName = "0.0.0.0"
serverPort = 8096
test_table = 'table-foo'

# mock API server for WASM FDW testing
#
# Note: this mock server is for smoke testing purpose so it provides hard coded
#       response only.
class MockServer(BaseHTTPRequestHandler):
    def get_fdw_req_path(self):
        fdw = self.path.split("/")[1]
        req_path = self.path[self.path.find("/", 1):]
        return (fdw, req_path)


    def response(self, body):
        self.send_response(200)
        self.send_header("Content-type", "application/json")
        self.end_headers()
        self.wfile.write(bytes(body, "utf-8"))


    def do_GET(self):
        (fdw, req_path) = self.get_fdw_req_path()
        print(fdw, req_path)

        if fdw == "paddle":
            body = '''
{
    "data": [{
        "id": "ctm_01hytsesv6f8wqzw1eyctqw6qm",
        "status": "active",
        "custom_data": {
            "xx": "yy2"
        },
        "name": "Test2",
        "email": "test2@test.com",
        "marketing_consent": false,
        "locale": "en",
        "created_at": "2024-05-26T15:49:09.606Z",
        "updated_at": "2024-06-07T11:40:15.469033Z",
        "import_meta": null
    }, {
        "id": "ctm_01hymwgpkx639a6mkvg99563sp",
        "status": "active",
        "custom_data": null,
        "name": "Test Customer",
        "email": "test@test.com",
        "marketing_consent": false,
        "locale": "en",
        "created_at": "2024-05-24T08:47:10.973Z",
        "updated_at": "2024-05-24T08:47:10.973Z",
        "import_meta": null
    }],
    "meta": {
        "request_id": "58fedbdf-b3ef-4477-bed1-f245e95114b3",
        "pagination": {
            "per_page": 200,
            "next": "https://sandbox-api.paddle.com/customers?after=ctm_01hymwgpkx639a6mkvg99563sp&per_page=200",
            "has_more": false,
            "estimated_total": 2
        }
    }
}
            '''
        else:
            self.send_response(404)
            return

        self.response(body)

        return


    def do_POST(self):
        (fdw, req_path) = self.get_fdw_req_path()

        if fdw == "snowflake":
            body = '''
{
    "resultSetMetaData": {
        "numRows": 2,
        "format": "jsonv2",
        "partitionInfo": [{
            "rowCount": 2,
            "uncompressedSize": 118
        }],
        "rowType": [{
            "name": "ID",
            "database": "MYDATABASE",
            "schema": "PUBLIC",
            "table": "MYTABLE",
            "nullable": true,
            "byteLength": null,
            "precision": 38,
            "scale": 0,
            "collation": null,
            "type": "fixed",
            "length": null
        }, {
            "name": "NAME",
            "database": "MYDATABASE",
            "schema": "PUBLIC",
            "table": "MYTABLE",
            "nullable": true,
            "byteLength": 16777216,
            "precision": null,
            "scale": null,
            "collation": null,
            "type": "text",
            "length": 16777216
        }, {
            "name": "NUM",
            "database": "MYDATABASE",
            "schema": "PUBLIC",
            "table": "MYTABLE",
            "nullable": true,
            "byteLength": null,
            "precision": 38,
            "scale": 6,
            "collation": null,
            "type": "fixed",
            "length": null
        }, {
            "name": "DT",
            "database": "MYDATABASE",
            "schema": "PUBLIC",
            "table": "MYTABLE",
            "nullable": true,
            "byteLength": null,
            "precision": null,
            "scale": null,
            "collation": null,
            "type": "date",
            "length": null
        }, {
            "name": "TS",
            "database": "MYDATABASE",
            "schema": "PUBLIC",
            "table": "MYTABLE",
            "nullable": true,
            "byteLength": null,
            "precision": 0,
            "scale": 9,
            "collation": null,
            "type": "timestamp_ntz",
            "length": null
        }]
    },
    "data": [
        ["42", "foo", null, "19723", "1704231836.000000000"],
        ["43", "hello", "123.456000", "19862", "1716122096.000000000"]
    ],
    "code": "090001",
    "statementStatusUrl": "/api/v2/statements/01b4f9d4-3202-9f46-0000-0001cf56d011?requestId=d448e46b-1194-4d4c-ab14-1b4b05fcfe71",
    "requestId": "d448e46b-1194-4d4c-ab14-1b4b05fcfe71",
    "sqlState": "00000",
    "statementHandle": "01b4f9d4-3202-9f46-0000-0001cf56d011",
    "message": "Statement executed successfully.",
    "createdOn": 1718259164549
}
            '''

        else:
            self.send_response(404)
            return

        self.response(body)

        return


if __name__ == "__main__":
    # Create web server
    webServer = HTTPServer((hostName, serverPort), MockServer)
    print("WASM FDW Mock Server started at http://%s:%s" % (hostName, serverPort))

    try:
        webServer.serve_forever()
    except KeyboardInterrupt:
        pass

    webServer.server_close()
    print("Server stopped.")

