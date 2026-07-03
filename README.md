# Madam Tusan Backend Serverless



























































    upload_asset(file_path, s3_folder)        s3_folder = sys.argv[2]    file_path = sys.argv[1]            sys.exit(1)        print(__doc__)    if len(sys.argv) < 3:if __name__ == "__main__":        sys.exit(1)        print(f"❌ Error: {e}")    except Exception as e:        return url        print(f"✅ Subido: {url}")        url = f"s3://{bucket_name}/{s3_key}"        s3.upload_file(str(file_path), bucket_name, s3_key)    try:        s3_key = f"{s3_folder.rstrip('/')}/{file_path.name}"    # Construye la clave S3        bucket_name = config["bucket_name"]    s3 = boto3.client("s3", region_name=config["region"])            sys.exit(1)        print(f"❌ Archivo no encontrado: {file_path}")    if not file_path.exists():    file_path = Path(file_path)            sys.exit(1)        }, indent=2))            "region": "us-east-1"            "bucket_name": "madamtusan-backend-assets-dev-ACCOUNT_ID",        print(json.dumps({        print("Crea el archivo con:")        print("❌ Falta bucket-config.json")    except FileNotFoundError:            config = json.load(f)        with open("bucket-config.json", "r") as f:    try:    # Lee config del bucket        """Sube un archivo a S3 en la carpeta especificada."""def upload_asset(file_path, s3_folder):import boto3from pathlib import Pathimport jsonimport sys"""  python upload_asset.py banner.jpg assets/banners/  python upload_asset.py logo.png assets/Uso:Script para subir assets (logos, banners, etc.) a S3.Backend serverless para `madamtusan` usando AWS Lambda, DynamoDB, S3, Step Functions y EventBridge.

## Componentes incluidos
- Registro de usuarios y login sin Cognito
- Autenticación manual con tokens guardados en DynamoDB
- Ordenes multi-tenant con `tenant_id` y `user_id`
- Flujo de trabajo de pedido con Step Functions y Wait for Callback with Task Token
- Eventos de estado publicados en EventBridge
- Notificaciones hacia Rappi mediante un API externo configurado en `RAPPI_API_URL`
- Guardado de resumen de orden en S3 para cumplir con el uso de S3

## Archivos principales
- `serverless.yml`: definición de funciones, tablas DynamoDB, bucket S3 y máquina de estado
- `handler.py`: funciones Lambda de login, orden, callback y notificación Rappi
- `utils.py`: utilidades de hashing, tokenización y respuesta HTTP

## Variables de entorno necesarias
- `RAPPI_API_URL`: URL de la API de Rappi o de la nube externa para notificaciones de estado
- `TENANT_ID`: valor por defecto del tenant (`madamtusan`)

## Despliegue
1. Instala Serverless Framework:
   ```bash
   npm install -g serverless
   ```
2. En el directorio del backend:
   ```bash
   cd /home/toritosomali/CLOUD/ProjectoCloud-Backend-Serverless
   ```
3. Despliega:
   ```bash
   export RAPPI_API_URL="https://tu-api-externa.example.com/estado"
   serverless deploy --stage dev
   ```
4. Se crea el bucket automáticamente: `madamtusan-backend-assets-dev-<ACCOUNT_ID>`

## Estructura del bucket S3
```
madamtusan-backend-assets-dev-123456789/
├── products/           # Imágenes de productos
│   ├── sushi/
│   ├── gyoza/
│   ├── ramen/
│   └── ...
├── assets/             # Logos, banners, etc.
│   ├── logo.png
│   ├── banner.jpg
│   └── favicon.ico
└── orders/             # JSON de órdenes
    ├── order-id-1.json
    └── order-id-2.json
```

## Cargar productos
```bash
python load_products.py /ruta/a/chinesefoodnet
```

## Cargar logo u otros assets
```bash
# Opción 1: Manualmente con AWS CLI
aws s3 cp logo.png s3://madamtusan-backend-assets-dev-123456789/assets/logo.png

# Opción 2: Usar el script
python upload_asset.py logo.png assets/
```

## Endpoints principales
- `POST /auth/register`  (registro público de clientes)
- `POST /auth/login`     (login para obtener token Bearer)
- `POST /users`          (admin crea trabajadores y otros roles)
- `DELETE /users/{userId}` (admin elimina trabajadores)
- `GET /products`        (lista productos, filtrable por ?category=)
- `GET /products/{productId}`
- `POST /orders`
- `GET /orders`
- `GET /orders/{orderId}`
- `DELETE /orders/{orderId}` (admin elimina ordenes)
- `POST /tasks/callback`

## Roles y creación de usuarios
- Los clientes pueden auto-registrarse en `POST /auth/register`
- Los trabajadores y administradores deben crearse desde `POST /users`
  con un token válido de un admin existente

## Carrito de usuario
El carrito no se guarda en este backend. En esta solución el carrito se maneja en la UI/frontend y solo se envía al backend cuando el usuario confirma el pedido con `POST /orders`.

## Ejemplo de flujo
1. Cliente crea usuario y hace login.
2. Cliente envía `POST /orders` con `items` y `source`.
3. Lambda crea orden en DynamoDB y lanza Step Functions.
4. Cada etapa humana (`COOK`, `PACK`, `DELIVER`) queda en espera por token.
5. App de trabajadores llama `POST /tasks/callback` con `taskToken`, `order_id`, `workflow_step` y `worker_id`.
6. Si la orden es de `source: rapppi`, el evento `order.state.changed` dispara `rappiNotifier`.

## Notas
- El backend es serverless y multi-tenant.
- No se usa Cognito: la autenticación se guarda en `AUTH_TOKENS_TABLE`.
- Para pruebas locales puedes usar `serverless invoke local --function registerUser --path sample-register.json`.
