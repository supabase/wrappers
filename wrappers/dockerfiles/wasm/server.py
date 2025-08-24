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

