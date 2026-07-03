import os
import sys
import json
import uuid
from pathlib import Path
import boto3

# Mapa de categorías y precios
CATEGORY_MAP = {
    "sushi": {
        "name": "Sushi Rolls",
        "description": "Delicioso sushi fresco",
        "price": 25.00,
        "min_quantity": 1
    },
    "gyoza": {
        "name": "Gyozas",
        "description": "Dumplings asiáticos",
        "price": 12.00,
        "min_quantity": 1
    },
    "ramen": {
        "name": "Ramen",
        "description": "Noodles en caldo caliente",
        "price": 18.00,
        "min_quantity": 1
    },
    "tempura": {
        "name": "Tempura",
        "description": "Vegetales y mariscos fritos",
        "price": 15.00,
        "min_quantity": 1
    },
    "bento": {
        "name": "Bento Box",
        "description": "Caja de comida variada",
        "price": 20.00,
        "min_quantity": 1
    },
    "donburi": {
        "name": "Donburi",
        "description": "Arroz con topping",
        "price": 16.00,
        "min_quantity": 1
    },
}


def load_products(dataset_path, bucket_config):
    """Carga productos desde el dataset a S3 y DynamoDB."""
    s3 = boto3.client("s3", region_name=bucket_config["region"])
    dynamodb = boto3.resource("dynamodb", region_name=bucket_config["region"])
    
    bucket_name = bucket_config["bucket_name"]
    products_table = dynamodb.Table("madamtusan-backend-products-dev")
    
    dataset_root = Path(dataset_path)
    
    if not dataset_root.exists():
        print(f"❌ Dataset no encontrado: {dataset_path}")
        sys.exit(1)
    
    total_uploaded = 0
    
    for category_folder in sorted(dataset_root.iterdir()):
        if not category_folder.is_dir():
            continue
        
        category_name = category_folder.name.lower()
        
        if category_name not in CATEGORY_MAP:
            print(f" Ignorando categoría no mapeada: {category_name}")
            continue
        
        category_info = CATEGORY_MAP[category_name]
        
        print(f"\nProcesando: {category_name}")
        
        image_count = 0
        for image_file in sorted(category_folder.glob("*.jpg")):
            try:
                product_id = str(uuid.uuid4())
                s3_key = f"products/{category_name}/{image_file.name}"
                
                # Sube imagen a S3
                s3.upload_file(str(image_file), bucket_name, s3_key)
                
                # Guarda producto en DynamoDB
                product_record = {
                    "product_id": product_id,
                    "category": category_name,
                    "name": f"{category_info['name']} #{image_count + 1}",
                    "description": category_info["description"],
                    "price": category_info["price"],
                    "image_url": f"s3://{bucket_name}/{s3_key}",
                    "min_quantity": category_info["min_quantity"],
                    "created_at": int(__import__('time').time()),
                }
                
                products_table.put_item(Item=product_record)
                
                image_count += 1
                total_uploaded += 1
                
                print(f"  ✓ {image_file.name}")
                
                if image_count >= 10:
                    print(f"  (limitado a 10 imágenes por categoría)")
                    break
            
            except Exception as e:
                print(f"  ❌ Error con {image_file.name}: {e}")
        
        print(f"  Cargadas {image_count} imágenes")
    
    print(f"\nTotal cargadas: {total_uploaded} imágenes")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)
    
    dataset_path = sys.argv[1]
    
    config_file = "bucket-config.json"
    if len(sys.argv) >= 3:
        config_file = sys.argv[2]
    
    try:
        with open(config_file, "r") as f:
            bucket_config = json.load(f)
    except FileNotFoundError:
        print(f"❌ Archivo de configuración no encontrado: {config_file}")
        print("\nCrea bucket-config.json con:")
        print(json.dumps({
            "bucket_name": "madamtusan-backend-assets-dev-ACCOUNT_ID",
            "region": "us-east-1"
        }, indent=2))
        sys.exit(1)
    
    load_products(dataset_path, bucket_config)
