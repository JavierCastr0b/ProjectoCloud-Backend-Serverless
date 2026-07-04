import base64
import hashlib
import hmac
import json
import secrets
import time
from decimal import Decimal
from http import HTTPStatus


def json_body(event):
    body = event.get("body", "")
    if isinstance(body, str):
        try:
            return json.loads(body)
        except json.JSONDecodeError:
            raise ValueError("Invalid JSON payload")
    if isinstance(body, dict):
        return body
    raise ValueError("Missing request body")


def response(status_code, body):
    return {
        "statusCode": status_code,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Headers": "Content-Type, Authorization, X-Tenant-Id",
            "Access-Control-Allow-Methods": "OPTIONS,GET,POST"
        },
        "body": json.dumps(body, default=_json_default)
    }


def _json_default(value):
    """Convierte valores de DynamoDB a tipos serializables por JSON."""
    if isinstance(value, Decimal):
        return int(value) if value % 1 == 0 else float(value)
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


def get_header(event, name):
    headers = event.get("headers") or {}
    lower_headers = {k.lower(): v for k, v in headers.items()}
    return lower_headers.get(name.lower())


def get_bearer_token(event):
    authorization = get_header(event, "Authorization")
    if not authorization:
        return None
    parts = authorization.split()
    if len(parts) != 2 or parts[0].lower() != "bearer":
        return None
    return parts[1].strip()


def utc_timestamp():
    return int(time.time())


def generate_secure_token():
    return secrets.token_urlsafe(32)


def hash_password(password):
    if not password:
        raise ValueError("Password cannot be empty")
    salt = secrets.token_hex(16)
    dk = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt.encode("utf-8"), 120_000)
    hashed = base64.urlsafe_b64encode(dk).decode("utf-8")
    return f"{salt}${hashed}"


def verify_password(password, stored_hash):
    try:
        salt, saved = stored_hash.split("$")
    except ValueError:
        return False
    dk = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt.encode("utf-8"), 120_000)
    expected = base64.urlsafe_b64encode(dk).decode("utf-8")
    return hmac.compare_digest(expected, saved)
