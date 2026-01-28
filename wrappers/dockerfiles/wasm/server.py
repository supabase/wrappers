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
        elif fdw == "clerk":
            body = '''
[
  {
    "id": "user_2rvWkk90azWI2o3PH4LDuCMDPPh",
    "object": "user",
    "username": null,
    "first_name": null,
    "last_name": null,
    "image_url": "https://img.clerk.com/eyJ0eXBlIjoiZGVmYXVsdCIsImlpZCI6Imluc18ycnVoOVpldUJWa3pNc1FoRHg5VWdNS2ZySGMiLCJyaWQiOiJ1c2VyXzJydldrazkwYXpXSTJvM1BINExEdUNNRFBQaCJ9",
    "has_image": false,
    "primary_email_address_id": "idn_2rvWkfsWd4iVsYWUYn5zKAnXqtM",
    "primary_phone_number_id": null,
    "primary_web3_wallet_id": null,
    "password_enabled": true,
    "two_factor_enabled": false,
    "totp_enabled": false,
    "backup_code_enabled": false,
    "email_addresses": [
      {
        "id": "idn_2rvWkfsWd4iVsYWUYn5zKAnXqtM",
        "object": "email_address",
        "email_address": "test@test.com",
        "reserved": false,
        "verification": {
          "status": "verified",
          "strategy": "admin",
          "attempts": null,
          "expire_at": null
        },
        "linked_to": [],
        "matches_sso_connection": false,
        "created_at": 1737440173271,
        "updated_at": 1737440173271
      }
    ],
    "phone_numbers": [],
    "web3_wallets": [],
    "passkeys": [],
    "external_accounts": [],
    "saml_accounts": [],
    "enterprise_accounts": [],
    "public_metadata": {},
    "private_metadata": {},
    "unsafe_metadata": {},
    "external_id": null,
    "last_sign_in_at": null,
    "banned": false,
    "locked": false,
    "lockout_expires_in_seconds": null,
    "verification_attempts_remaining": 100,
    "created_at": 1737440173260,
    "updated_at": 1737440173281,
    "delete_self_enabled": true,
    "create_organization_enabled": true,
    "last_active_at": null,
    "mfa_enabled_at": null,
    "mfa_disabled_at": null,
    "legal_accepted_at": null,
    "profile_image_url": "https://www.gravatar.com/avatar?d=mp"
  }
]
            '''
        elif fdw == "orb":
            body = '''
{
  "data": [
    {
      "accounting_sync_configuration": {
        "accounting_providers": [],
        "excluded": false
      },
      "additional_emails": [],
      "auto_collection": true,
      "balance": "0.00",
      "billing_address": null,
      "created_at": "2025-02-15T13:04:43+00:00",
      "currency": "USD",
      "email": "test@test.com",
      "email_delivery": true,
      "exempt_from_automated_tax": false,
      "external_customer_id": "aaabbbcccddd",
      "hierarchy": {
        "children": [],
        "parent": null
      },
      "id": "XimGiw3pnsgusvc3",
      "metadata": {
        "is_local_entity": "true",
        "mydata.0": "aaabbbcccddd"
      },
      "name": "test@test.com customer",
      "payment_provider": "stripe_charge",
      "payment_provider_id": "cus_xxxx",
      "portal_url": "https://portal.withorb.com/view?token=aaaa.bbb.ccc",
      "reporting_configuration": null,
      "shipping_address": null,
      "tax_id": null,
      "timezone": "Etc/UTC"
    }
  ],
  "pagination_metadata": {
    "has_more": false,
    "next_cursor": null
  }
}
            '''
        elif fdw == "hubspot":
            body = '''
{
  "results": [
    {
      "id": "1501",
      "properties": {
        "createdate": "2021-04-28T10:26:44.741Z",
        "hs_object_id": "1501",
        "lastmodifieddate": "2025-02-28T05:09:57.297Z",
        "user_id": "8527"
      },
      "createdAt": "2021-04-28T10:26:44.741Z",
      "updatedAt": "2025-02-28T05:09:57.297Z",
      "archived": false
    },
    {
      "id": "1502",
      "properties": {
        "createdate": "2021-04-28T10:26:44.804Z",
        "hs_object_id": "1502",
        "lastmodifieddate": "2025-02-24T17:25:56.940Z",
        "user_id": "8528"
      },
      "createdAt": "2021-04-28T10:26:44.804Z",
      "updatedAt": "2025-02-24T17:25:56.940Z",
      "archived": false
    }
  ],
  "paging": {
    "next": {
      "after": "1503",
      "link": "https://api.hubapi.com/crm/v3/objects/contacts?limit=2&properties=user_id&after=1503"
    }
  }
}
            '''
        elif fdw == "gravatar":
            body = '''
{
  "hash": "973dfe463ec85785f5f95af5ba3906eedb2d931c24e69824a89ea65dba4e813b",
  "display_name": "Test",
  "profile_url": "https://gravatar.com/test",
  "avatar_url": "https://1.gravatar.com/avatar/0133ce4a2479bd7267f37e3b2f5a741c4aaab910950434d7f14e89bddfe1",
  "avatar_alt_text": "",
  "location": "",
  "description": "",
  "job_title": "",
  "company": "",
  "verified_accounts": [],
  "pronunciation": "",
  "pronouns": "",
  "timezone": "",
  "languages": [],
  "first_name": "",
  "last_name": "",
  "is_organization": false,
  "links": [],
  "interests": [],
  "payments": {
    "links": [],
    "crypto_wallets": []
  },
  "contact_info": {},
  "gallery": [],
  "number_verified_accounts": 0,
  "last_profile_edit": "2025-07-19T00:42:37Z",
  "registration_date": "2023-04-25T12:17:23Z",
  "section_visibility": {
    "hidden_contact_info": false,
    "hidden_links": false,
    "hidden_interests": false,
    "hidden_wallet": false,
    "hidden_photos": false,
    "hidden_verified_accounts": false
  }
}
            '''
        elif fdw == "openapi":
            # Generic OpenAPI FDW test endpoints covering all features

            # Pattern 1: Simple list endpoint
            if req_path == "/users" or req_path.startswith("/users?"):
                body = '''
{
  "data": [
    {
      "id": "user-123",
      "name": "John Doe",
      "email": "john@example.com",
      "created_at": "2024-01-15T10:30:00Z",
      "active": true
    },
    {
      "id": "user-456",
      "name": "Jane Smith",
      "email": "jane@example.com",
      "created_at": "2024-02-20T14:45:00Z",
      "active": false
    }
  ],
  "meta": {
    "pagination": {
      "has_more": false,
      "next_cursor": null
    }
  }
}
                '''
            # Pattern 2: Nested resource with path param (/users/{id}/posts)
            # Must check before single user pattern
            elif "/posts" in req_path and req_path.startswith("/users/"):
                user_id = req_path.split("/")[2]
                body = f'''
{{
  "data": [
    {{
      "id": "post-001",
      "user_id": "{user_id}",
      "title": "First Post",
      "content": "Hello world!",
      "created_at": "2024-03-01T09:00:00Z"
    }},
    {{
      "id": "post-002",
      "user_id": "{user_id}",
      "title": "Second Post",
      "content": "Another post",
      "created_at": "2024-03-02T10:00:00Z"
    }}
  ]
}}
                '''
            # Pattern 3: Single resource by ID (rowid pushdown)
            elif req_path.startswith("/users/user-"):
                user_id = req_path.split("/")[2]
                body = f'''
{{
  "id": "{user_id}",
  "name": "John Doe",
  "email": "john@example.com",
  "created_at": "2024-01-15T10:30:00Z",
  "active": true
}}
                '''
            # Pattern 4: Multiple path parameters (e.g., /projects/{org}/{repo}/issues)
            elif req_path.startswith("/projects/"):
                parts = req_path.split("/")
                if len(parts) >= 5 and parts[4] == "issues":
                    org = parts[2]
                    repo = parts[3]
                    body = f'''
{{
  "items": [
    {{
      "id": 1,
      "org": "{org}",
      "repo": "{repo}",
      "title": "Bug: Something broken",
      "state": "open",
      "created_at": "2024-01-10T08:00:00Z"
    }},
    {{
      "id": 2,
      "org": "{org}",
      "repo": "{repo}",
      "title": "Feature: Add new thing",
      "state": "closed",
      "created_at": "2024-01-11T09:00:00Z"
    }}
  ]
}}
                    '''
                else:
                    self.send_response(404)
                    return
            # Pattern 5: GeoJSON FeatureCollection response
            elif req_path == "/locations" or req_path.startswith("/locations?"):
                body = '''
{
  "type": "FeatureCollection",
  "features": [
    {
      "type": "Feature",
      "id": "loc-001",
      "geometry": {
        "type": "Point",
        "coordinates": [-97.7431, 30.2672]
      },
      "properties": {
        "id": "loc-001",
        "name": "Austin",
        "category": "city",
        "population": 964254
      }
    },
    {
      "type": "Feature",
      "id": "loc-002",
      "geometry": {
        "type": "Point",
        "coordinates": [-96.7970, 32.7767]
      },
      "properties": {
        "id": "loc-002",
        "name": "Dallas",
        "category": "city",
        "population": 1304379
      }
    }
  ]
}
                '''
            # Pattern 6: Single GeoJSON Feature with path param
            elif req_path.startswith("/locations/loc-"):
                loc_id = req_path.split("/")[2]
                body = f'''
{{
  "type": "Feature",
  "id": "{loc_id}",
  "geometry": {{
    "type": "Point",
    "coordinates": [-97.7431, 30.2672]
  }},
  "properties": {{
    "id": "{loc_id}",
    "name": "Austin",
    "category": "city",
    "population": 964254
  }}
}}
                '''
            # Pattern 7: Nested path with multiple params (e.g., /resources/{type}/{id})
            elif req_path.startswith("/resources/"):
                parts = req_path.split("/")
                if len(parts) >= 4:
                    res_type = parts[2]
                    res_id = parts[3].split("?")[0]
                    body = f'''
{{
  "type": "{res_type}",
  "id": "{res_id}",
  "name": "Resource {res_id}",
  "description": "A {res_type} resource",
  "created_at": "2024-01-01T00:00:00Z"
}}
                    '''
                else:
                    self.send_response(404)
                    return
            # Pattern 8: Link-based pagination (next_url)
            elif req_path == "/paginated" or req_path.startswith("/paginated?"):
                if "page=2" in req_path:
                    body = '''
{
  "results": [
    {"id": 3, "name": "Item Three"},
    {"id": 4, "name": "Item Four"}
  ],
  "pagination": {
    "next_url": null,
    "has_more": false
  }
}
                    '''
                else:
                    body = '''
{
  "results": [
    {"id": 1, "name": "Item One"},
    {"id": 2, "name": "Item Two"}
  ],
  "pagination": {
    "next_url": "http://localhost:8096/openapi/paginated?page=2",
    "has_more": true
  }
}
                    '''
            # Pattern 9: Direct array response
            elif req_path == "/tags" or req_path.startswith("/tags?"):
                body = '''
[
  {"id": 1, "name": "rust", "count": 150},
  {"id": 2, "name": "postgres", "count": 89},
  {"id": 3, "name": "wasm", "count": 42}
]
                '''
            else:
                self.send_response(404)
                return
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
        elif fdw == "shopify":
            body = '''
{
  "data": {
    "products": {
      "nodes": [
        {
          "id": "gid://shopify/Product/9975063609658"
        },
        {
          "id": "gid://shopify/Product/9975063904570"
        }
      ]
    }
  },
  "extensions": {
    "cost": {
      "requestedQueryCost": 13,
      "actualQueryCost": 7,
      "throttleStatus": {
        "maximumAvailable": 2000,
        "currentlyAvailable": 1993,
        "restoreRate": 100
      }
    }
  }
}
            '''
        elif fdw == "infura":
            content_length = int(self.headers['Content-Length'])
            post_data = self.rfile.read(content_length)
            req_body = json.loads(post_data.decode('utf-8'))
            method = req_body.get('method', '')

            if method == 'eth_getBlockByNumber':
                body = '''
{
  "jsonrpc": "2.0",
  "id": 1,
  "result": {
    "number": "0x1234567",
    "hash": "0xd4e56740f876aef8c010b86a40d5f56745a118d0906a34e69aec8c0db1cb8fa3",
    "parentHash": "0x0000000000000000000000000000000000000000000000000000000000000000",
    "timestamp": "0x55ba4224",
    "miner": "0x0000000000000000000000000000000000000000",
    "gasUsed": "0x0",
    "gasLimit": "0x1c9c380",
    "baseFeePerGas": "0x3b9aca00",
    "transactions": ["0x1", "0x2", "0x3"]
  }
}
                '''
            elif method == 'eth_getTransactionByHash':
                body = '''
{
  "jsonrpc": "2.0",
  "id": 1,
  "result": {
    "hash": "0x5c504ed432cb51138bcf09aa5e8a410dd4a1e204ef84bfed1be16dfba1b22060",
    "blockNumber": "0x1234567",
    "blockHash": "0xd4e56740f876aef8c010b86a40d5f56745a118d0906a34e69aec8c0db1cb8fa3",
    "from": "0xd8da6bf26964af9d7eed9e03e53415d37aa96045",
    "to": "0x742d35cc6634c0532925a3b844bc454e4438f44e",
    "value": "0xde0b6b3a7640000",
    "gas": "0x5208",
    "gasPrice": "0x3b9aca00",
    "nonce": "0x0",
    "input": "0x",
    "transactionIndex": "0x0"
  }
}
                '''
            elif method == 'eth_getBalance':
                body = '''
{
  "jsonrpc": "2.0",
  "id": 1,
  "result": "0x1bc16d674ec80000"
}
                '''
            elif method == 'eth_getLogs':
                body = '''
{
  "jsonrpc": "2.0",
  "id": 1,
  "result": [
    {
      "address": "0xdac17f958d2ee523a2206206994597c13d831ec7",
      "blockNumber": "0x1234567",
      "blockHash": "0xd4e56740f876aef8c010b86a40d5f56745a118d0906a34e69aec8c0db1cb8fa3",
      "transactionHash": "0x5c504ed432cb51138bcf09aa5e8a410dd4a1e204ef84bfed1be16dfba1b22060",
      "transactionIndex": "0x0",
      "logIndex": "0x0",
      "data": "0x000000000000000000000000000000000000000000000000000000000000002a",
      "topics": ["0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"],
      "removed": false
    }
  ]
}
                '''
            elif method == 'eth_chainId':
                body = '''
{
  "jsonrpc": "2.0",
  "id": 1,
  "result": "0x1"
}
                '''
            elif method == 'eth_blockNumber':
                body = '''
{
  "jsonrpc": "2.0",
  "id": 2,
  "result": "0x1234567"
}
                '''
            elif method == 'eth_gasPrice':
                body = '''
{
  "jsonrpc": "2.0",
  "id": 3,
  "result": "0x3b9aca00"
}
                '''
            else:
                body = '''
{
  "jsonrpc": "2.0",
  "id": 1,
  "error": {"code": -32601, "message": "Method not found"}
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

