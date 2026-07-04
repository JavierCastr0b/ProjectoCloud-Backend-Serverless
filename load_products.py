import json
import mimetypes
import re
import sys
import tempfile
import time
from decimal import Decimal
from pathlib import Path
from urllib.parse import unquote, urlparse
from urllib.request import Request, urlopen

import boto3


VALID_EXTENSIONS = {".jpg", ".jpeg", ".png", ".webp", ".svg"}
TIMESTAMP_SUFFIX = re.compile(r"-\d{18}$")

# Categorías y precios de referencia para la demo. Se pueden ajustar sin
# cambiar los identificadores ni volver a cargar las imágenes.
PRODUCT_DATA = {
    "chaufa-de-pollo": ("chaufas", 32),
    "wantan-clasico": ("dim-sums", 18),
    "taypa-para-4": ("promociones", 180),
    "sopa-wantan-especial": ("sopas", 34),
    "tallarin-de-pollo-en-trozos-con-verduras": ("tallarines", 39),
    "siu-mai-especial": ("dim-sums", 25),
    "pollo-chi-jau-kay": ("clasicos", 39),
    "tallarin-de-pollo-con-verduras": ("tallarines", 37),
    "rollitos-primavera": ("dim-sums", 26),
    "aeropuerto-de-pollo-chi-jau-kay-sobre-tortilla": ("aeropuertos", 45),
    "la-plancha-taypa": ("especialidades", 67),
    "chaufa-samsi": ("chaufas", 42),
    "taypa-para-8": ("promociones", 360),
    "taypa-para-6": ("promociones", 280),
    "taypa-especial-4": ("promociones", 230),
    "doble-compartir": ("para-compartir", 79),
    "chicharron-pollo-al-limon-canela-china": ("clasicos", 42),
    "chicharron-de-pechuga-empanizada": ("clasicos", 42),
    "pollo-limonkay": ("clasicos", 39),
    "chancho-con-pina": ("clasicos", 45),
    "pollo-ti-pa-kay": ("clasicos", 39),
    "chanchito-asado": ("asados", 48),
    "1-2-pollo-asado-al-cilindro": ("asados", 48),
    "1-4-pato-asado": ("asados", 49),
    "panceta-asada": ("asados", 52),
    "1-2-pato-asado": ("asados", 89),
}


def read_urls(url_file):
    """Lee URLs válidas y elimina duplicados conservando el orden."""
    seen = set()
    urls = []
    for raw_line in Path(url_file).read_text(encoding="utf-8").splitlines():
        url = raw_line.strip()
        extension = Path(unquote(urlparse(url).path)).suffix.lower()
        if not url.startswith(("https://", "http://")):
            continue
        if extension not in VALID_EXTENSIONS or url in seen:
            continue
        seen.add(url)
        urls.append(url)
    return urls


def filename_from_url(url):
    return Path(unquote(urlparse(url).path)).name


def asset_group(url):
    path = urlparse(url).path.lower()
    if "/products/" in path:
        return "products"
    if "/sections/" in path:
        return "sections"
    if "/banners/" in path:
        return "banners"
    return "branding"


def product_slug(url):
    stem = Path(filename_from_url(url)).stem.lower()
    return TIMESTAMP_SUFFIX.sub("", stem)


def product_name(slug):
    words = slug.replace("-", " ")
    replacements = {
        "1 2": "1/2",
        "1 4": "1/4",
        "chi jau kay": "Chi Jau Kay",
        "ti pa kay": "Ti Pa Kay",
        "siu mai": "Siu Mai",
        "taypa": "Taypá",
        "samsi": "Samsi",
    }
    name = words.title()
    for source, replacement in replacements.items():
        name = re.sub(source, replacement, name, flags=re.IGNORECASE)
    return name


def preferred_product_urls(urls):
    """Elige una sola imagen por plato, priorizando WebP sobre JPG."""
    products = {}
    for url in urls:
        if asset_group(url) != "products":
            continue
        slug = product_slug(url)
        current = products.get(slug)
        if current is None or Path(filename_from_url(url)).suffix.lower() == ".webp":
            products[slug] = url
    return products


def public_api_url(bucket, region, key):
    if region == "us-east-1":
        return f"https://{bucket}.s3.amazonaws.com/{key}"
    return f"https://{bucket}.s3.{region}.amazonaws.com/{key}"


def download(url, destination):
    request = Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urlopen(request, timeout=60) as response:
        destination.write_bytes(response.read())


def upload_asset(s3, bucket, url, temp_dir):
    filename = filename_from_url(url)
    group = asset_group(url)
    key = f"site-assets/{group}/{filename}"
    local_file = temp_dir / filename
    download(url, local_file)

    content_type = mimetypes.guess_type(filename)[0] or "application/octet-stream"
    s3.upload_file(
        str(local_file),
        bucket,
        key,
        ExtraArgs={
            "ContentType": content_type,
            "CacheControl": "public, max-age=31536000, immutable",
        },
    )
    return key


def load_assets(url_file, bucket_config):
    region = bucket_config.get("region", "us-east-1")
    bucket = bucket_config["bucket_name"]
    table_name = bucket_config.get(
        "products_table", "madamtusan-backend-products-dev"
    )

    s3 = boto3.client("s3", region_name=region)
    dynamodb = boto3.resource("dynamodb", region_name=region)
    products_table = dynamodb.Table(table_name)
    urls = read_urls(url_file)
    selected_products = preferred_product_urls(urls)
    uploaded_keys = {}

    if not urls:
        raise ValueError("No se encontraron URLs de imágenes válidas")

    print(f"Recursos únicos encontrados: {len(urls)}")
    print(f"Productos únicos encontrados: {len(selected_products)}")

    with tempfile.TemporaryDirectory(prefix="madamtusan-assets-") as temp:
        temp_dir = Path(temp)
        for index, url in enumerate(urls, start=1):
            try:
                key = upload_asset(s3, bucket, url, temp_dir)
                uploaded_keys[url] = key
                print(f"[{index}/{len(urls)}] S3 ✓ {key}")
            except Exception as exc:
                print(f"[{index}/{len(urls)}] S3 ✗ {url}: {exc}")

    loaded_products = 0
    for slug, url in sorted(selected_products.items()):
        image_key = uploaded_keys.get(url)
        if not image_key:
            print(f"DynamoDB ✗ {slug}: su imagen no pudo subirse")
            continue

        category, price = PRODUCT_DATA.get(slug, ("especialidades", 35))
        record = {
            "product_id": slug,
            "tenant_id": "madamtusan",
            "category": category,
            "name": product_name(slug),
            "description": f"Plato de la carta Madam Tusan: {product_name(slug)}.",
            "price": Decimal(str(price)),
            "currency": "PEN",
            "image_key": image_key,
            # Es útil para inspección; la API reemplaza image_url por una URL firmada.
            "image_url": public_api_url(bucket, region, image_key),
            "min_quantity": 1,
            "active": True,
            "updated_at": int(time.time()),
            "source": "madamtusan.com.pe",
        }
        products_table.put_item(Item=record)
        loaded_products += 1
        print(f"DynamoDB ✓ {record['name']} ({category})")

    print(f"\nCarga terminada: {len(uploaded_keys)} recursos en S3")
    print(f"Carga terminada: {loaded_products} productos en DynamoDB")


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        raise SystemExit(1)

    url_file = sys.argv[1]
    config_file = sys.argv[2] if len(sys.argv) >= 3 else "bucket-config.json"

    try:
        bucket_config = json.loads(Path(config_file).read_text(encoding="utf-8"))
    except FileNotFoundError:
        print(f"❌ Archivo de configuración no encontrado: {config_file}")
        print(
            json.dumps(
                {
                    "bucket_name": "madamtusan-backend-assets-dev-ACCOUNT_ID",
                    "region": "us-east-1",
                    "products_table": "madamtusan-backend-products-dev",
                },
                indent=2,
            )
        )
        raise SystemExit(1)

    try:
        load_assets(url_file, bucket_config)
    except (OSError, ValueError) as exc:
        print(f"❌ {exc}")
        raise SystemExit(1)


if __name__ == "__main__":
    main()
