from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("glue-s3-tables-rest") \
    .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.4.2") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.defaultCatalog", "s3_rest_catalog") \
    .config("spark.sql.catalog.s3_rest_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.s3_rest_catalog.type", "rest") \
    .config("spark.sql.catalog.s3_rest_catalog.uri", "https://s3tables.eu-north-1.amazonaws.com/iceberg") \
    .config("spark.sql.catalog.s3_rest_catalog.warehouse", "arn:aws:s3tables:eu-north-1:941116500025:bucket/demo") \
    .config("spark.sql.catalog.s3_rest_catalog.rest.sigv4-enabled", "true") \
    .config("spark.sql.catalog.s3_rest_catalog.rest.signing-name", "s3tables") \
    .config("spark.sql.catalog.s3_rest_catalog.rest.signing-region", "eu-north-1") \
    .config('spark.sql.catalog.s3_rest_catalog.io-impl','org.apache.iceberg.aws.s3.S3FileIO') \
    .config('spark.sql.catalog.s3_rest_catalog.rest-metrics-reporting-enabled','false') \
    .getOrCreate()

namespace = "demo_db"
table = "demo_table"

spark.sql("SHOW DATABASES").show()
spark.sql("SHOW CATALOGS").show()

spark.sql(f"DESCRIBE NAMESPACE {namespace}").show()

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {namespace}.{table} (
       sale_date DATE,
       product_category STRING,
       sales_amount int
    )
""")

spark.sql(f"""
    INSERT INTO demo_db.demo_table
    VALUES
    (CAST('2026-02-15' AS DATE), 'ABC', 1000),
    (CAST('2026-02-16' AS DATE), 'XYZ', 2000);
""")

spark.sql(f"SELECT * FROM {namespace}.{table} LIMIT 10").show()