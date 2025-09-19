#minio_client.py
import os
from minio import Minio

def get_minio_client():
    """
    Tạo và trả về đối tượng client MinIO sử dụng biến môi trường.
    """
    minio_url = os.environ.get("MINIO_URL", "minio:9000")
    minio_user = os.environ.get("MINIO_USER", "minio_admin")
    minio_password = os.environ.get("MINIO_PASSWORD", "password123")
    
    client = Minio(
        minio_url,
        access_key=minio_user,
        secret_key=minio_password,
        secure=False
    )
    return client

def upload_file_to_minio(bucket_name, object_name, file_path):
    """
    Tải (upload) một file từ đường dẫn cục bộ lên MinIO.

    Args:
        bucket_name (str): Tên bucket đích trên MinIO (ví dụ: 'bronze').
        object_name (str): Tên file sẽ được lưu trên MinIO, bao gồm cả đường dẫn
                           (ví dụ: 'raw_data/2023-10-27/products.json').
        file_path (str): Đường dẫn tuyệt đối hoặc tương đối đến file cục bộ cần tải lên.
    """
    client = get_minio_client()
    
    # 1. Kiểm tra xem bucket đã tồn tại chưa
    try:
        if not client.bucket_exists(bucket_name):
            client.make_bucket(bucket_name)
            print(f"Bucket '{bucket_name}' created successfully.")
        
        # 2. Tải file lên MinIO
        client.fput_object(
            bucket_name,
            object_name,
            file_path,
        )
        print(f"'{file_path}' has been uploaded to '{bucket_name}/{object_name}'.")
        
    except Exception as e:
        print(f"Error uploading file: {e}")
        return False
    
    return True

