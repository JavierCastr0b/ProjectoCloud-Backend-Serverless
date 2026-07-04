import json
import os
import time
import uuid
import base64
from datetime import datetime
from http import HTTPStatus
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

import boto3
from boto3.dynamodb.conditions import Key

from utils import (
    generate_secure_token,
    get_bearer_token,
    hash_password,
    json_body,
    response,
    utc_timestamp,
    verify_password,
)


dynamodb = boto3.resource("dynamodb")
s3 = boto3.client("s3")
sf = boto3.client("stepfunctions")
events = boto3.client("events")
sns = boto3.client("sns")

USERS_TABLE = os.environ["USERS_TABLE"]
AUTH_TOKENS_TABLE = os.environ["AUTH_TOKENS_TABLE"]
ORDERS_TABLE = os.environ["ORDERS_TABLE"]
EVENTS_TABLE = os.environ["EVENTS_TABLE"]
PRODUCTS_TABLE = os.environ["PRODUCTS_TABLE"]
S3_BUCKET = os.environ["S3_BUCKET"]
SNS_ORDERS_TOPIC_ARN = os.environ.get("SNS_ORDERS_TOPIC_ARN", "")
STATE_MACHINE_ARN = os.environ["ORDER_WORKFLOW_STATE_MACHINE_ARN"]
RAPPI_API_URL = os.environ.get("RAPPI_API_URL", "")
DEFAULT_TENANT = os.environ.get("TENANT_ID", "madamtusan")

users_table = dynamodb.Table(USERS_TABLE)
auth_tokens_table = dynamodb.Table(AUTH_TOKENS_TABLE)
orders_table = dynamodb.Table(ORDERS_TABLE)
events_table = dynamodb.Table(EVENTS_TABLE)
products_table = dynamodb.Table(PRODUCTS_TABLE)


def _build_user_key(tenant_id, user_id):
    return f"{tenant_id}#{user_id}"


def _now_iso():
    return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def _publish_event(detail_type, detail):
    payload = {
        "Source": "madamtusan.orders",
        "DetailType": detail_type,
        "Detail": json.dumps(detail),
        "EventBusName": "default",
    }
    events.put_events(Entries=[payload])
    event_id = str(uuid.uuid4())
    events_table.put_item(
        Item={
            "event_id": event_id,
            "tenant_id": detail["tenant_id"],
            "order_id": detail.get("order_id", ""),
            "detail_type": detail_type,
            "detail": detail,
            "created_at": _now_iso(),
        }
    )


def _ensure_tenant(body):
    tenant_id = body.get("tenant_id") or body.get("tenant")
    return tenant_id or DEFAULT_TENANT


def _is_admin(payload):
    return payload.get("role") == "admin"


def _prompt_unauthorized():
    return response(HTTPStatus.UNAUTHORIZED, {"message": "Unauthorized"})


def authenticate(event):
    token = get_bearer_token(event)
    if not token:
        raise ValueError("Missing authorization token")

    result = auth_tokens_table.get_item(Key={"token": token})
    item = result.get("Item")
    if not item:
        raise ValueError("Invalid token")

    if item.get("expires_at", 0) < utc_timestamp():
        raise ValueError("Token expired")

    return item


def register_user(event, context):
    try:
        body = json_body(event)
        tenant_id = _ensure_tenant(body)
        user_id = body["user_id"]
        password = body["password"]
        role = body.get("role", "customer")

        if role != "customer":
            return response(
                HTTPStatus.FORBIDDEN,
                {
                    "message": "Worker/admin accounts must be created by an admin user via POST /users"
                },
            )

        hashed = hash_password(password)
        users_table.put_item(
            Item={
                "tenant_user_id": _build_user_key(tenant_id, user_id),
                "tenant_id": tenant_id,
                "user_id": user_id,
                "password_hash": hashed,
                "role": role,
                "email": body.get("email", ""),
                "phone": body.get("phone", ""),
                "address": body.get("address", ""),
                "avatar_url": body.get("avatar_url", ""),
                "email_subscribed": body.get("email_subscribed", True),
                "created_at": _now_iso(),
            },
            ConditionExpression="attribute_not_exists(tenant_user_id)",
        )
        return response(HTTPStatus.CREATED, {"message": "Customer registered"})
    except KeyError as exc:
        return response(HTTPStatus.BAD_REQUEST, {"message": f"Missing field: {exc.args[0]}"})
    except Exception as exc:
        return response(HTTPStatus.BAD_REQUEST, {"message": str(exc)})


def login(event, context):
    try:
        body = json_body(event)
        tenant_id = _ensure_tenant(body)
        user_id = body["user_id"]
        password = body["password"]

        user_key = _build_user_key(tenant_id, user_id)
        result = users_table.get_item(Key={"tenant_user_id": user_key})
        item = result.get("Item")
        if not item or not verify_password(password, item["password_hash"]):
            return response(HTTPStatus.UNAUTHORIZED, {"message": "Invalid credentials"})

        token = generate_secure_token()
        expires_at = utc_timestamp() + 24 * 3600
        auth_tokens_table.put_item(
            Item={
                "token": token,
                "tenant_id": tenant_id,
                "user_id": user_id,
                "role": item.get("role", "customer"),
                "expires_at": expires_at,
                "created_at": _now_iso(),
            }
        )
        return response(HTTPStatus.OK, {"token": token, "expires_at": expires_at, "role": item.get("role", "customer")})
    except KeyError as exc:
        return response(HTTPStatus.BAD_REQUEST, {"message": f"Missing field: {exc.args[0]}"})
    except Exception as exc:
        return response(HTTPStatus.BAD_REQUEST, {"message": str(exc)})


def create_user(event, context):
    try:
        auth_payload = authenticate(event)
        if not _is_admin(auth_payload):
            return response(HTTPStatus.FORBIDDEN, {"message": "Admin privileges required"})

        body = json_body(event)
        tenant_id = auth_payload["tenant_id"]
        user_id = body["user_id"]
        password = body["password"]
        role = body.get("role", "worker")
        if role not in ["customer", "worker", "cook", "pack", "deliverer", "admin"]:
            return response(HTTPStatus.BAD_REQUEST, {"message": "Invalid role"})

        hashed = hash_password(password)
        users_table.put_item(
            Item={
                "tenant_user_id": _build_user_key(tenant_id, user_id),
                "tenant_id": tenant_id,
                "user_id": user_id,
                "password_hash": hashed,
                "role": role,
                "created_by": auth_payload["user_id"],
                "created_at": _now_iso(),
            },
            ConditionExpression="attribute_not_exists(tenant_user_id)",
        )
        return response(HTTPStatus.CREATED, {"message": "User created", "role": role})
    except KeyError as exc:
        return response(HTTPStatus.BAD_REQUEST, {"message": f"Missing field: {exc.args[0]}"})
    except Exception as exc:
        return response(HTTPStatus.BAD_REQUEST, {"message": str(exc)})


def delete_user(event, context):
    try:
        auth_payload = authenticate(event)
        if not _is_admin(auth_payload):
            return response(HTTPStatus.FORBIDDEN, {"message": "Admin privileges required"})

        tenant_id = auth_payload["tenant_id"]
        user_id = event["pathParameters"]["userId"]
        user_key = _build_user_key(tenant_id, user_id)
        result = users_table.get_item(Key={"tenant_user_id": user_key})
        user = result.get("Item")
        if not user:
            return response(HTTPStatus.NOT_FOUND, {"message": "User not found"})

        users_table.delete_item(Key={"tenant_user_id": user_key})
        return response(HTTPStatus.OK, {"message": "User deleted"})
    except ValueError as exc:
        return response(HTTPStatus.UNAUTHORIZED, {"message": str(exc)})
    except Exception as exc:
        return response(HTTPStatus.BAD_REQUEST, {"message": str(exc)})


def subscribe_email(event, context):
    try:
        auth_payload = authenticate(event)
        tenant_id = auth_payload["tenant_id"]
        user_id = auth_payload["user_id"]
        body = json_body(event)
        
        user_key = _build_user_key(tenant_id, user_id)
        result = users_table.get_item(Key={"tenant_user_id": user_key})
        user = result.get("Item")
        
        if not user:
            return response(HTTPStatus.NOT_FOUND, {"message": "User not found"})
        
        email = user.get("email", "")
        if not email:
            return response(HTTPStatus.BAD_REQUEST, {"message": "User has no email"})
        
        subscribe = body.get("subscribe", True)
        
        users_table.update_item(
            Key={"tenant_user_id": user_key},
            UpdateExpression="SET email_subscribed = :sub",
            ExpressionAttributeValues={":sub": subscribe},
        )
        
        if subscribe and SNS_ORDERS_TOPIC_ARN:
            try:
                sns.subscribe(
                    TopicArn=SNS_ORDERS_TOPIC_ARN,
                    Protocol="email",
                    Endpoint=email,
                    Attributes={"FilterPolicy": json.dumps({"tenant_id": [tenant_id]})}
                )
            except Exception:
                pass
        
        status_msg = "subscribed" if subscribe else "unsubscribed"
        return response(HTTPStatus.OK, {"message": f"Email {status_msg}"})
    except ValueError as exc:
        return response(HTTPStatus.UNAUTHORIZED, {"message": str(exc)})
    except Exception as exc:
        return response(HTTPStatus.BAD_REQUEST, {"message": str(exc)})


def update_profile(event, context):
    try:
        auth_payload = authenticate(event)
        tenant_id = auth_payload["tenant_id"]
        user_id = auth_payload["user_id"]
        body = json_body(event)
        
        user_key = _build_user_key(tenant_id, user_id)
        
        update_expr = "SET "
        expr_values = {}
        
        if "email" in body:
            update_expr += "email = :email, "
            expr_values[":email"] = body["email"]
        
        if "phone" in body:
            update_expr += "phone = :phone, "
            expr_values[":phone"] = body["phone"]
        
        if "address" in body:
            update_expr += "address = :address, "
            expr_values[":address"] = body["address"]
        
        if "avatar_url" in body:
            update_expr += "avatar_url = :avatar_url, "
            expr_values[":avatar_url"] = body["avatar_url"]
        
        if not expr_values:
            return response(HTTPStatus.BAD_REQUEST, {"message": "No fields to update"})
        
        update_expr += "updated_at = :updated_at"
        expr_values[":updated_at"] = _now_iso()
        
        users_table.update_item(
            Key={"tenant_user_id": user_key},
            UpdateExpression=update_expr,
            ExpressionAttributeValues=expr_values,
        )
        
        return response(HTTPStatus.OK, {"message": "Profile updated"})
    except ValueError as exc:
        return response(HTTPStatus.UNAUTHORIZED, {"message": str(exc)})
    except KeyError as exc:
        return response(HTTPStatus.BAD_REQUEST, {"message": f"Missing field: {exc.args[0]}"})
    except Exception as exc:
        return response(HTTPStatus.BAD_REQUEST, {"message": str(exc)})


def create_order(event, context):
    try:
        auth_payload = authenticate(event)
        body = json_body(event)
        tenant_id = auth_payload["tenant_id"]
        user_id = auth_payload["user_id"]
        source = body.get("source", "web")
        items = body.get("items", [])
        if not items:
            return response(HTTPStatus.BAD_REQUEST, {"message": "Order items are required"})

        order_id = str(uuid.uuid4())
        order = {
            "order_id": order_id,
            "tenant_id": tenant_id,
            "created_by": user_id,
            "source": source,
            "is_rappi": source.lower() == "rappi",
            "status": "RECEIVED",
            "workflow_step": "RECEIVED",
            "task_token": None,
            "items": items,
            "history": [
                {
                    "timestamp": _now_iso(),
                    "status": "RECEIVED",
                    "step": "ORDER_CREATED",
                    "actor": user_id,
                }
            ],
            "created_at": _now_iso(),
        }

        orders_table.put_item(Item=order)
        s3.put_object(
            Bucket=S3_BUCKET,
            Key=f"orders/{order_id}.json",
            Body=json.dumps(order).encode("utf-8"),
            ContentType="application/json",
        )

        _publish_event("order.state.changed", {"tenant_id": tenant_id, "order_id": order_id, "status": "RECEIVED", "step": "ORDER_CREATED", "source": source})

        _send_order_notification(tenant_id, user_id, order_id, items)

        sf.start_execution(
            stateMachineArn=STATE_MACHINE_ARN,
            name=f"order-{order_id}-{int(time.time())}",
            input=json.dumps({"order": order}),
        )

        return response(HTTPStatus.CREATED, {"order_id": order_id, "message": "Order created and workflow started"})
    except ValueError as exc:
        return response(HTTPStatus.UNAUTHORIZED, {"message": str(exc)})
    except KeyError as exc:
        return response(HTTPStatus.BAD_REQUEST, {"message": f"Missing field: {exc.args[0]}"})
    except Exception as exc:
        return response(HTTPStatus.BAD_REQUEST, {"message": str(exc)})


def list_orders(event, context):
    try:
        auth_payload = authenticate(event)
        tenant_id = auth_payload["tenant_id"]
        result = orders_table.query(
            IndexName="TenantIndex",
            KeyConditionExpression=Key("tenant_id").eq(tenant_id),
        )
        return response(HTTPStatus.OK, {"orders": result.get("Items", [])})
    except ValueError as exc:
        return response(HTTPStatus.UNAUTHORIZED, {"message": str(exc)})
    except Exception as exc:
        return response(HTTPStatus.BAD_REQUEST, {"message": str(exc)})


def get_order(event, context):
    try:
        auth_payload = authenticate(event)
        tenant_id = auth_payload["tenant_id"]
        order_id = event["pathParameters"]["orderId"]
        result = orders_table.get_item(Key={"order_id": order_id})
        order = result.get("Item")
        if not order or order.get("tenant_id") != tenant_id:
            return response(HTTPStatus.NOT_FOUND, {"message": "Order not found"})
        return response(HTTPStatus.OK, {"order": order})
    except ValueError as exc:
        return response(HTTPStatus.UNAUTHORIZED, {"message": str(exc)})
    except Exception as exc:
        return response(HTTPStatus.BAD_REQUEST, {"message": str(exc)})


def delete_order(event, context):
    try:
        auth_payload = authenticate(event)
        if not _is_admin(auth_payload):
            return response(HTTPStatus.FORBIDDEN, {"message": "Admin privileges required"})

        order_id = event["pathParameters"]["orderId"]
        result = orders_table.get_item(Key={"order_id": order_id})
        order = result.get("Item")
        if not order or order.get("tenant_id") != auth_payload["tenant_id"]:
            return response(HTTPStatus.NOT_FOUND, {"message": "Order not found"})

        orders_table.delete_item(Key={"order_id": order_id})
        s3.delete_object(Bucket=S3_BUCKET, Key=f"orders/{order_id}.json")

        _publish_event(
            "order.state.changed",
            {
                "tenant_id": auth_payload["tenant_id"],
                "order_id": order_id,
                "status": "DELETED",
                "actor": auth_payload["user_id"],
            },
        )
        return response(HTTPStatus.OK, {"message": "Order deleted"})
    except ValueError as exc:
        return response(HTTPStatus.UNAUTHORIZED, {"message": str(exc)})
    except Exception as exc:
        return response(HTTPStatus.BAD_REQUEST, {"message": str(exc)})


def task_handler(event, context):
    order = event.get("order") or {}
    task_token = event.get("taskToken")
    workflow_step = event.get("workflow_step", "UNKNOWN")
    order_id = order.get("order_id")
    tenant_id = order.get("tenant_id")

    if not task_token or not order_id or not tenant_id:
        raise ValueError("Missing taskToken, order_id or tenant_id")

    orders_table.update_item(
        Key={"order_id": order_id},
        UpdateExpression=(
            "SET task_token = :token, workflow_step = :step, #status = :status, "
            "history = list_append(if_not_exists(history, :empty_list), :history_item)"
        ),
        ExpressionAttributeNames={"#status": "status"},
        ExpressionAttributeValues={
            ":token": task_token,
            ":step": workflow_step,
            ":status": f"WAITING_{workflow_step}",
            ":history_item": [
                {
                    "timestamp": _now_iso(),
                    "status": f"WAITING_{workflow_step}",
                    "step": workflow_step,
                    "actor": "system",
                }
            ],
            ":empty_list": [],
        },
    )

    _publish_event(
        "order.state.changed",
        {
            "tenant_id": tenant_id,
            "order_id": order_id,
            "status": f"WAITING_{workflow_step}",
            "step": workflow_step,
            "source": order.get("source"),
        },
    )

    return {"message": "Task token stored", "order_id": order_id}


def submit_task_callback(event, context):
    try:
        auth_payload = authenticate(event)
        body = json_body(event)
        tenant_id = auth_payload["tenant_id"]
        order_id = body["order_id"]
        task_token = body["taskToken"]
        worker_id = body.get("worker_id", auth_payload["user_id"])
        step = body.get("workflow_step", "UNKNOWN")
        status = body.get("status", "COMPLETED")

        orders_table.update_item(
            Key={"order_id": order_id},
            UpdateExpression=(
                "SET task_token = :null, workflow_step = :step, #status = :status, "
                "history = list_append(if_not_exists(history, :empty_list), :history_item)"
            ),
            ExpressionAttributeNames={"#status": "status"},
            ExpressionAttributeValues={
                ":null": None,
                ":step": step,
                ":status": status,
                ":history_item": [
                    {
                        "timestamp": _now_iso(),
                        "status": status,
                        "step": step,
                        "actor": worker_id,
                    }
                ],
                ":empty_list": [],
            },
        )

        _publish_event(
            "order.state.changed",
            {
                "tenant_id": tenant_id,
                "order_id": order_id,
                "status": status,
                "step": step,
                "worker_id": worker_id,
            },
        )

        sf.send_task_success(
            taskToken=task_token,
            output=json.dumps(
                {
                    "order": {
                        "order_id": order_id,
                        "tenant_id": tenant_id,
                        "status": status,
                        "workflow_step": step,
                    }
                }
            ),
        )

        return response(HTTPStatus.OK, {"message": "Task callback accepted"})
    except ValueError as exc:
        return response(HTTPStatus.UNAUTHORIZED, {"message": str(exc)})
    except KeyError as exc:
        return response(HTTPStatus.BAD_REQUEST, {"message": f"Missing field: {exc.args[0]}"})
    except Exception as exc:
        return response(HTTPStatus.BAD_REQUEST, {"message": str(exc)})


def list_products(event, context):
    try:
        category = event.get("queryStringParameters", {}).get("category") if event.get("queryStringParameters") else None
        
        if category:
            result = products_table.query(
                IndexName="CategoryIndex",
                KeyConditionExpression=Key("category").eq(category),
            )
        else:
            result = products_table.scan()
        
        products = [_with_signed_image(item) for item in result.get("Items", [])]
        return response(HTTPStatus.OK, {"products": products, "count": len(products)})
    except Exception as exc:
        return response(HTTPStatus.BAD_REQUEST, {"message": str(exc)})


def get_product(event, context):
    try:
        product_id = event["pathParameters"]["productId"]
        result = products_table.get_item(Key={"product_id": product_id})
        product = result.get("Item")
        if not product:
            return response(HTTPStatus.NOT_FOUND, {"message": "Product not found"})
        return response(HTTPStatus.OK, {"product": _with_signed_image(product)})
    except Exception as exc:
        return response(HTTPStatus.BAD_REQUEST, {"message": str(exc)})


def _signed_s3_url(key):
    return s3.generate_presigned_url(
        "get_object",
        Params={"Bucket": S3_BUCKET, "Key": key},
        ExpiresIn=3600,
    )


def _with_signed_image(product):
    product = dict(product)
    image_key = product.get("image_key")
    if image_key:
        product["image_url"] = _signed_s3_url(image_key)
    return product


def list_assets(event, context):
    """Devuelve logos, banners y categorías con URLs HTTPS temporales."""
    try:
        groups = {"branding": [], "banners": [], "sections": []}
        paginator = s3.get_paginator("list_objects_v2")
        pages = paginator.paginate(Bucket=S3_BUCKET, Prefix="site-assets/")

        for page in pages:
            for item in page.get("Contents", []):
                key = item["Key"]
                parts = key.split("/", 2)
                if len(parts) != 3 or parts[1] not in groups:
                    continue
                groups[parts[1]].append(
                    {
                        "key": key,
                        "name": parts[2],
                        "url": _signed_s3_url(key),
                    }
                )

        for assets in groups.values():
            assets.sort(key=lambda asset: asset["name"])
        return response(HTTPStatus.OK, {"assets": groups, "expires_in": 3600})
    except Exception as exc:
        return response(HTTPStatus.BAD_REQUEST, {"message": str(exc)})


def rappi_notifier(event, context):
    detail = event.get("detail", {})
    if isinstance(detail, str):
        detail = json.loads(detail)
    if not detail.get("tenant_id") or not detail.get("order_id"):
        return {"message": "Ignored event"}

    if not detail.get("source") or detail.get("source").lower() != "rappi":
        return {"message": "Not a Rappi order"}
    if not RAPPI_API_URL:
        return {"message": "No RAPPI_API_URL configured"}

    payload = json.dumps(
        {
            "tenant_id": detail["tenant_id"],
            "order_id": detail["order_id"],
            "status": detail["status"],
            "step": detail.get("step"),
            "worker_id": detail.get("worker_id"),
            "timestamp": _now_iso(),
        }
    ).encode("utf-8")
    request = Request(RAPPI_API_URL, data=payload, headers={"Content-Type": "application/json"}, method="POST")
    try:
        with urlopen(request, timeout=10) as resp:
            body = resp.read().decode("utf-8")
            return {"status": resp.status, "response": body}
    except HTTPError as err:
        return {"status": err.code, "error": err.reason}
    except URLError as err:
        return {"error": str(err)}


def upload_avatar(event, context):
    try:
        auth_payload = authenticate(event)
        tenant_id = auth_payload["tenant_id"]
        user_id = auth_payload["user_id"]
        
        body = json_body(event)
        image_data = body.get("image")  # Base64-encoded image
        
        if not image_data:
            return response(HTTPStatus.BAD_REQUEST, {"message": "Image data required"})
        
        file_name = f"avatars/{user_id}.jpg"
        bucket = os.environ["S3_BUCKET"]
        
        try:
            image_bytes = base64.b64decode(image_data)
        except Exception as exc:
            return response(HTTPStatus.BAD_REQUEST, {"message": f"Invalid base64 image: {str(exc)}"})
        
        s3.put_object(
            Bucket=bucket,
            Key=file_name,
            Body=image_bytes,
            ContentType="image/jpeg",
            Metadata={"tenant_id": tenant_id, "user_id": user_id},
        )
        
        avatar_url = f"s3://{bucket}/{file_name}"
        
        user_key = _build_user_key(tenant_id, user_id)
        users_table.update_item(
            Key={"tenant_user_id": user_key},
            UpdateExpression="SET avatar_url = :url, updated_at = :updated_at",
            ExpressionAttributeValues={
                ":url": avatar_url,
                ":updated_at": _now_iso(),
            },
        )
        
        return response(HTTPStatus.OK, {"avatar_url": avatar_url, "message": "Avatar uploaded"})
    except ValueError as exc:
        return response(HTTPStatus.UNAUTHORIZED, {"message": str(exc)})
    except Exception as exc:
        return response(HTTPStatus.BAD_REQUEST, {"message": str(exc)})


def _send_order_notification(tenant_id, user_id, order_id, items):
    """Envía notificación por email de confirmación de orden"""
    if not SNS_ORDERS_TOPIC_ARN:
        return
    
    try:
        user_key = _build_user_key(tenant_id, user_id)
        result = users_table.get_item(Key={"tenant_user_id": user_key})
        user = result.get("Item", {})
        
        if not user.get("email_subscribed"):
            return
        
        email = user.get("email", "")
        if not email:
            return
        
        items_str = "\n".join([f"- {item.get('product_id', 'Unknown')}: {item.get('quantity', 1)}x ${item.get('price', 0)}" for item in items])
        
        message = f"""
¡Gracias por tu pedido!

Número de orden: {order_id}
Estado: Recibido

Items:
{items_str}

Tu pedido será preparado pronto. Puedes rastrear el estado en tu cuenta.

Saludos,
Madam Tusán
"""
        
        sns.publish(
            TopicArn=SNS_ORDERS_TOPIC_ARN,
            Subject=f"Confirmación de pedido #{order_id}",
            Message=message,
            MessageAttributes={
                "tenant_id": {"DataType": "String", "StringValue": tenant_id},
                "user_email": {"DataType": "String", "StringValue": email}
            }
        )
    except Exception as e:
        print(f"Error sending order notification: {e}")
