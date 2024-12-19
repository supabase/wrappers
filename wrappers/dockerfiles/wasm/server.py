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
        elif fdw == "notion":
            body = '''
{
  "object": "page",
  "id": "5a67c86f-d0da-4d0a-9dd7-f4cf164e6247",
  "created_time": "2021-10-15T05:41:00.000Z",
  "last_edited_time": "2021-10-15T05:49:00.000Z",
  "created_by": {
    "object": "user",
    "id": "fd0ed76c-44bd-413a-9448-18ff4b1d6a5e"
  },
  "last_edited_by": {
    "object": "user",
    "id": "fd0ed76c-44bd-413a-9448-18ff4b1d6a5e"
  },
  "cover": null,
  "icon": null,
  "parent": {
    "type": "workspace",
    "workspace": true
  },
  "archived": false,
  "in_trash": false,
  "properties": {
    "title": {
      "id": "title",
      "type": "title",
      "title": [
        {
          "type": "text",
          "text": {
            "content": "test page3",
            "link": null
          },
          "annotations": {
            "bold": false,
            "italic": false,
            "strikethrough": false,
            "underline": false,
            "code": false,
            "color": "default"
          },
          "plain_text": "test page3",
          "href": null
        }
      ]
    }
  },
  "url": "https://www.notion.so/test-page3-5a67c86fd0da4d0a9dd7f4cf164e6247",
  "public_url": null,
  "request_id": "85a75f82-bd22-414e-a3a7-5c00a9451a1c"
}
            '''
        elif fdw == "calendly":
            body = '''
{
  "collection": [
    {
      "active": true,
      "admin_managed": false,
      "booking_method": "instant",
      "color": "#8247f5",
      "created_at": "2024-11-06T07:22:55.937829Z",
      "custom_questions": [
        {
          "answer_choices": [],
          "enabled": true,
          "include_other": false,
          "name": "Please share anything that will help prepare for our meeting.",
          "position": 0,
          "required": false,
          "type": "text"
        }
      ],
      "deleted_at": null,
      "description_html": null,
      "description_plain": null,
      "duration": 30,
      "duration_options": null,
      "internal_note": null,
      "kind": "solo",
      "locations": null,
      "name": "30 Minute Meeting",
      "pooling_type": null,
      "position": 0,
      "profile": {
        "name": "Test User",
        "owner": "https://api.calendly.com/users/3ea2f4a7-8d91-4342-aeb0-32a13b2236dc",
        "type": "User"
      },
      "scheduling_url": "https://calendly.com/test-user/30min",
      "secret": false,
      "slug": "30min",
      "type": "StandardEventType",
      "updated_at": "2024-11-06T07:22:55.937829Z",
      "uri": "https://api.calendly.com/event_types/158ecbf6-79bb-4205-a5fc-a7fefa5883a2"
    }
  ],
  "pagination": {
    "count": 1,
    "next_page": null,
    "next_page_token": null,
    "previous_page": null,
    "previous_page_token": null
  }
}
            '''
        elif fdw == "cal":
            body = '''
{
    "status": "success",
    "data": {
        "id": 1234567,
        "email": "test@test.com",
        "timeFormat": 12,
        "defaultScheduleId": 123456,
        "weekStart": "Sunday",
        "timeZone": "Australia/Sydney",
        "username": "test",
        "organizationId": null
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
        elif fdw == "cfd1":
            body = '''
{
  "result": [
    {
      "results": [
        {
          "id": 42,
          "name": "test name 2"
        },
        {
          "id": 123,
          "name": "test name"
        }
      ],
      "success": true,
      "meta": {
        "served_by": "v3-prod",
        "duration": 0.1983,
        "changes": 0,
        "last_row_id": 0,
        "changed_db": false,
        "size_after": 16384,
        "rows_read": 2,
        "rows_written": 0
      }
    }
  ],
  "errors": [],
  "messages": [],
  "success": true
}
            '''
        elif fdw == "infura":
            content_length = int(self.headers['Content-Length'])
            request_body = self.rfile.read(content_length).decode('utf-8')
            request = json.loads(request_body)
            method = request.get('method')

            responses = {
                'eth_blockNumber': {
                    'jsonrpc': '2.0',
                    'id': request.get('id', 1),
                    'result': '0x1234567'
                },
                'eth_getBlockByNumber': {
                    'jsonrpc': '2.0',
                    'id': request.get('id', 1),
                    'result': {
                        'number': '0x1234567',
                        'hash': '0xabcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789',
                        'parentHash': '0x0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef',
                        'nonce': '0x123456',
                        'miner': '0xdef0123456789abcdef0123456789abcdef012345',
                        'difficulty': '0x5678',
                        'totalDifficulty': '0x12345678',
                        'size': '0x1234',
                        'gasLimit': '0x5678',
                        'gasUsed': '0x1234',
                        'timestamp': '0x5678'
                    }
                }
            }

            body = json.dumps(responses.get(method, {
                'jsonrpc': '2.0',
                'id': request.get('id', 1),
                'error': {'code': -32601, 'message': 'Method not found'}
            }))
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

