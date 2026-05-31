from pyspark.sql import SparkSession
from pyspark.sql import functions as F


conf = {
    "spark.driver.memory": "1g",
    "spark.executor.memory": "1g",
    "spark.driver.cores": "1",
    "spark.executor.cores": "1",
    "spark.sql.shuffle.partitions": "4",
    "spark.default.parallelism": "4"
}

builder = (
    SparkSession
    .builder
    .appName("clickhouse_reports")
    .config(
        "spark.jars",
        "/home/jovyan/work/drivers/postgresql-42.7.11.jar,/home/jovyan/work/drivers/clickhouse-jdbc-0.9.7-all.jar"
    )
)

for k, v in conf.items():
    builder = builder.config(k, v)

spark = builder.getOrCreate()


postgres_url = "jdbc:postgresql://postgres:5432/mydb"
postgres_properties = {
    "user": "postgres",
    "password": "password",
    "driver": "org.postgresql.Driver"
}

clickhouse_url = "jdbc:clickhouse:http://clickhouse:8123/analytics"
clickhouse_properties = {
    "driver": "com.clickhouse.jdbc.Driver",
    "user": "etl_user",
    "password": "etl_password"
}


fact_sales = spark.read.jdbc(
    postgres_url,
    "fact_sales",
    properties=postgres_properties
)

dim_customer = spark.read.jdbc(
    postgres_url,
    "dim_customer",
    properties=postgres_properties
)

dim_store = spark.read.jdbc(
    postgres_url,
    "dim_store",
    properties=postgres_properties
)

dim_supplier = spark.read.jdbc(
    postgres_url,
    "dim_supplier",
    properties=postgres_properties
)

dim_product = spark.read.jdbc(
    postgres_url,
    "dim_product",
    properties=postgres_properties
)


sales_by_products = fact_sales.alias("f") \
    .join(
        dim_product.alias("p"),
        F.col("f.product_key") == F.col("p.product_key"),
        "inner"
    ) \
    .groupBy(
        F.col("p.product_key"),
        F.col("p.product_name"),
        F.col("p.product_category"),
        F.col("p.pet_category")
    ) \
    .agg(
        F.sum("f.sale_total_price").cast("double").alias("total_revenue"),
        F.sum("f.sale_quantity").cast("long").alias("total_quantity_sold"),
        F.avg("p.product_rating").cast("double").alias("avg_rating"),
        F.max("p.product_reviews").cast("long").alias("product_reviews")
    )


sales_by_customers = fact_sales.alias("f") \
    .join(
        dim_customer.alias("c"),
        F.col("f.customer_key") == F.col("c.customer_key"),
        "inner"
    ) \
    .groupBy(
        F.col("c.customer_key"),
        F.col("c.first_name"),
        F.col("c.last_name"),
        F.col("c.country")
    ) \
    .agg(
        F.count("*").cast("long").alias("orders_count"),
        F.sum("f.sale_total_price").cast("double").alias("total_spent"),
        F.avg("f.sale_total_price").cast("double").alias("avg_check")
    )


sales_by_time = fact_sales \
    .groupBy(
        F.year("sale_date").cast("int").alias("sale_year"),
        F.month("sale_date").cast("int").alias("sale_month")
    ) \
    .agg(
        F.count("*").cast("long").alias("orders_count"),
        F.sum("sale_total_price").cast("double").alias("total_revenue"),
        F.avg("sale_total_price").cast("double").alias("avg_order_value")
    ) \
    .orderBy("sale_year", "sale_month")


sales_by_stores = fact_sales.alias("f") \
    .join(
        dim_store.alias("s"),
        F.col("f.store_key") == F.col("s.store_key"),
        "left"
    ) \
    .groupBy(
        F.col("s.store_key"),
        F.col("s.store_name"),
        F.col("s.store_city"),
        F.col("s.store_country")
    ) \
    .agg(
        F.count("*").cast("long").alias("orders_count"),
        F.sum("f.sale_total_price").cast("double").alias("total_revenue"),
        F.avg("f.sale_total_price").cast("double").alias("avg_check")
    )


sales_by_suppliers = fact_sales.alias("f") \
    .join(
        dim_supplier.alias("s"),
        F.col("f.supplier_key") == F.col("s.supplier_key"),
        "left"
    ) \
    .join(
        dim_product.alias("p"),
        F.col("f.product_key") == F.col("p.product_key"),
        "left"
    ) \
    .groupBy(
        F.col("s.supplier_key"),
        F.col("s.supplier_name"),
        F.col("s.supplier_country")
    ) \
    .agg(
        F.sum("f.sale_total_price").cast("double").alias("total_revenue"),
        F.avg("p.product_price").cast("double").alias("avg_product_price"),
        F.sum("f.sale_quantity").cast("long").alias("total_quantity_sold")
    )


product_quality = fact_sales.alias("f") \
    .join(
        dim_product.alias("p"),
        F.col("f.product_key") == F.col("p.product_key"),
        "inner"
    ) \
    .groupBy(
        F.col("p.product_key"),
        F.col("p.product_name"),
        F.col("p.product_rating"),
        F.col("p.product_reviews")
    ) \
    .agg(
        F.sum("f.sale_quantity").cast("long").alias("total_quantity_sold"),
        F.sum("f.sale_total_price").cast("double").alias("total_revenue")
    )


sales_by_products.write.jdbc(
    clickhouse_url,
    "analytics.sales_by_products",
    mode="append",
    properties=clickhouse_properties
)

sales_by_customers.write.jdbc(
    clickhouse_url,
    "analytics.sales_by_customers",
    mode="append",
    properties=clickhouse_properties
)

sales_by_time.write.jdbc(
    clickhouse_url,
    "analytics.sales_by_time",
    mode="append",
    properties=clickhouse_properties
)

sales_by_stores.write.jdbc(
    clickhouse_url,
    "analytics.sales_by_stores",
    mode="append",
    properties=clickhouse_properties
)

sales_by_suppliers.write.jdbc(
    clickhouse_url,
    "analytics.sales_by_suppliers",
    mode="append",
    properties=clickhouse_properties
)

product_quality.write.jdbc(
    clickhouse_url,
    "analytics.product_quality",
    mode="append",
    properties=clickhouse_properties
)

print("clickhouse reports finished")
spark.stop()