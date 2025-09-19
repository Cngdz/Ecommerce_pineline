# silver_transform.py
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, coalesce, current_date
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from minio_client import get_minio_client

def create_spark_session():
    """Create and configure Spark session """
    spark = SparkSession.builder \
        .appName("Ecommerce Silver Transformation") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,org.projectnessie.spark.extensions.NessieSparkSessionExtensions") \
        .config("spark.sql.catalog.nessie", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.nessie.warehouse", "s3a://lakehouse/") \
        .config("spark.sql.catalog.nessie.uri", "http://nessie-catalog:19120/api/v2") \
        .config("spark.sql.catalog.nessie.ref", "main") \
        .config("spark.sql.catalog.nessie.type", "nessie") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minio_admin") \
        .config("spark.hadoop.fs.s3a.secret.key", "password123") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()
    return spark

def define_schema():
    return StructType([
        StructField("product_id", StringType()),
        StructField("name", StringType()),
        StructField("brand_name", StringType()),
        StructField("price", DoubleType()),
        StructField("original_price", DoubleType()),
        StructField("available", IntegerType()),
        StructField("rating_average", DoubleType()),
        StructField("review_count", IntegerType()),
        StructField("quantity_sold", IntegerType())
    ])

def clean_and_transform(df):
    return df.filter(col("product_id").isNotNull()
    ).withColumn(
        "name",
        when(col("name").isNull(), "Unknown")
        .otherwise(col("name"))
    ).withColumn(
        "brand_name",
        when(col("brand_name").isNull(), "Unknown")
        .otherwise(col("brand_name"))
    ).withColumn(
        "price",
        when((col("price").isNull()) | (col("price") < 0), 0.0)
        .otherwise(col("price"))
    ).withColumn(
        "original_price",
        when((col("original_price").isNull()) | (col("original_price") < 0), col("price"))
        .otherwise(col("original_price"))
    ).withColumn(
        "rating_average",
        when((col("rating_average") < 0) | (col("rating_average") > 5), None)
        .otherwise(col("rating_average"))
    ).withColumn(
        "review_count",
        coalesce(col("review_count"), lit(0))
    ).withColumn(
        "quantity_sold",
        coalesce(col("quantity_sold"), lit(0))
    ).withColumn(
        "available",
        coalesce(col("available"), lit(0))
    ).withColumn(
        "ingestion_date", current_date()   
    ).select(
        "product_id",
        "name",
        "brand_name",
        "price",
        "original_price",
        "rating_average",
        "review_count",
        "quantity_sold",
        "available",
        "ingestion_date"
        )

def validate_data(df):
    """Validate data quality"""
    # Check for null product IDs
    null_products = df.filter(col("product_id").isNull()).count()
    if null_products > 0:
        raise ValueError(f"Found {null_products} rows with null product IDs")
    
    # Check for invalid prices
    invalid_prices = df.filter((col("price") < 0) | (col("original_price") < 0)).count()
    if invalid_prices > 0:
        raise ValueError(f"Found {invalid_prices} rows with invalid prices")

def ensure_bucket_exists(bucket_name):
    """
    Đảm bảo bucket tồn tại trên MinIO, nếu chưa có thì tạo mới.
    """
    client = get_minio_client()
    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)
        print(f"Bucket '{bucket_name}' created successfully.")
    else:
        print(f"Bucket '{bucket_name}' already exists.")

def process_data(spark, input_path):
    """Main data processing function"""
    try:
        print(f"Reading data from: {input_path}")
        
        df = spark.read.json(input_path, schema=define_schema())
        
        df.cache()
        
        cleaned_df = clean_and_transform(df)
        
        validate_data(cleaned_df)
        
        num_partitions = max(1, cleaned_df.count() // 100000)
        cleaned_df = cleaned_df.repartition(num_partitions)
        
        cleaned_df.writeTo("nessie.silver.products").createOrReplace()
            
        df.unpersist()
        print("Data successfully transformed.")

    except Exception as e:
        print(f"An error occurred during Spark job: {e}")
        # Thoát với mã lỗi để Airflow biết tác vụ đã thất bại
        sys.exit(1)

if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.exit(1)
    input_path = sys.argv[1]
    spark = create_spark_session()
    
    process_data(spark, input_path)