#!/usr/bin/env python3
import http.server
import socketserver
import json
import boto3
import random
import string

# Constants for Cognito
endpoint_url = "http://localhost:9229"
user_pool_name = "MyUserPool"
number_of_users = 90

# Create a Cognito Identity Provider client
client = boto3.client('cognito-idp', endpoint_url=endpoint_url)

def random_string(length=8):
    """Generate a random string of fixed length."""
    letters = string.ascii_lowercase
    return ''.join(random.choice(letters) for i in range(length))

def create_user_pool(name):
    try:
        response = client.create_user_pool(
            PoolName=name,
            Policies={
                'PasswordPolicy': {
                    'MinimumLength': 8,
                    'RequireUppercase': True,
                    'RequireLowercase': True,
                    'RequireNumbers': True,
                    'RequireSymbols': True
                }
            },
            UsernameAttributes=['email'],
            AutoVerifiedAttributes=['email']
        )
        return response['UserPool']['Id']
    except client.exceptions.ResourceExistsException:
        print(f"User pool {name} already exists.")
        response = client.list_user_pools(MaxResults=1, Filter=f'name = "{name}"')
        if response['UserPools']:
            return response['UserPools'][0]['Id']
        else:
            raise Exception(f"User pool {name} not found after creation attempt.")

def create_user(user_pool_id, username):
    try:
        email = f"{username}@example.com"
        response = client.admin_create_user(
            UserPoolId=user_pool_id,
            Username=username,
            UserAttributes=[
                {
                    'Name': 'email',
                    'Value': email
                },
                {
                    'Name': 'email_verified',
                    'Value': 'True'
                }
            ],
        )
        print(f"User created: {username} with email {email}")
        return response
    except Exception as e:
        print(f"Error creating user {username}: {e}")

class MockServerHandler(http.server.SimpleHTTPRequestHandler):
    def do_GET(self):
        response_data = self.list_users()

        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.end_headers()
        self.wfile.write(json.dumps(response_data).encode())

    def list_users(self):
        try:
            response = client.list_users(UserPoolId=user_pool_id, Limit=10)
            users = response.get('Users', [])
            for user in users:
                # Convert datetime objects to string
                if 'UserCreateDate' in user:
                    user['UserCreateDate'] = user['UserCreateDate'].isoformat()
                if 'UserLastModifiedDate' in user:
                    user['UserLastModifiedDate'] = user['UserLastModifiedDate'].isoformat()
            print(response)
            return response
        except Exception as e:
            return {'error': str(e)}

if __name__ == "__main__":
    # Create user pool and add users
    user_pool_id = create_user_pool(user_pool_name)
    for _ in range(number_of_users):
        username = random_string()
        create_user(user_pool_id, username)

    # Define server address and port
    port = 3792
    server_address = ('', port)
    # Create an HTTP server instance
    httpd = socketserver.TCPServer(server_address, MockServerHandler)
    print(f"Cognito mock Server running on port {port}...")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    httpd.server_close()
    print("Server stopped")
