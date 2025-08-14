import os
import pandas as pd
from sqlalchemy import create_engine

def run():
    engine = create_engine(os.getenv('DATABASE_URL'))
    data_dir = os.getenv('DATA_DIR','data')

    files = [f for f in os.listdir(data_dir) if f.endswith('.csv')]

    for file in files:
        # Dòng 18: Đọc file CSV vào một DataFrame của Pandas
        df = pd.read_csv(os.path.join(data_dir, file))
        
        # Dòng 21: Tạo tên bảng từ tên file một cách tự động
        table_name = f"raw_{file.replace('olist_', '').replace('_dataset.csv', '')}"
        
        # Dòng 24-26: Tạo schema 'raw_data' trong database nếu nó chưa tồn tại
        with engine.connect() as connection:# type: ignore
            connection.execute("CREATE SCHEMA IF NOT EXISTS raw_data;")
       
        # Dòng 30: Dùng phương thức to_sql của Pandas để đẩy DataFrame vào database
        df.to_sql(table_name, engine, schema='raw_data', if_exists='replace', index=False)
       