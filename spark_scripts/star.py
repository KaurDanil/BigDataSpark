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
    .appName("star")
    .config(
        "spark.jars",
        "/home/jovyan/work/drivers/postgresql-42.7.11.jar"
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


mock_data = spark.read.jdbc(
    postgres_url,
    "mock_data",
    properties=postgres_properties
).dropDuplicates()


dim_customer = mock_data.select(
    F.col("sale_customer_id").alias("customer_id"),
    F.col("customer_first_name").alias("first_name"),
    F.col("customer_last_name").alias("last_name"),
    F.col("customer_age").alias("age"),
    F.col("customer_email").alias("email"),
    F.col("customer_country").alias("country"),
    F.col("customer_postal_code").alias("postal_code"),
    F.col("customer_pet_type").alias("pet_type"),
    F.col("customer_pet_name").alias("pet_name"),
    F.col("customer_pet_breed").alias("pet_breed")
).filter(
    F.col("sale_customer_id").isNotNull()
).dropDuplicates(["customer_id"])


dim_seller = mock_data.select(
    F.col("sale_seller_id").alias("seller_id"),
    F.col("seller_first_name").alias("first_name"),
    F.col("seller_last_name").alias("last_name"),
    F.col("seller_email").alias("email"),
    F.col("seller_country").alias("country"),
    F.col("seller_postal_code").alias("postal_code")
).filter(
    F.col("sale_seller_id").isNotNull()
).dropDuplicates(["seller_id"])


dim_store = mock_data.select(
    F.col("store_name"),
    F.col("store_location"),
    F.col("store_city"),
    F.col("store_state"),
    F.col("store_country"),
    F.col("store_phone"),
    F.col("store_email")
).dropDuplicates()


dim_supplier = mock_data.select(
    F.col("supplier_name"),
    F.col("supplier_contact"),
    F.col("supplier_email"),
    F.col("supplier_phone"),
    F.col("supplier_address"),
    F.col("supplier_city"),
    F.col("supplier_country")
).dropDuplicates()


dim_product = mock_data.select(
    F.col("sale_product_id").alias("product_id"),
    F.col("product_name"),
    F.col("product_category"),
    F.col("pet_category"),
    F.col("product_price"),
    F.col("product_quantity"),
    F.col("product_weight"),
    F.col("product_color"),
    F.col("product_size"),
    F.col("product_brand"),
    F.col("product_material"),
    F.col("product_description"),
    F.col("product_rating"),
    F.col("product_reviews"),
    F.to_date(F.col("product_release_date"), "M/d/yyyy").alias("product_release_date"),
    F.to_date(F.col("product_expiry_date"), "M/d/yyyy").alias("product_expiry_date")
    ).filter(
    F.col("sale_product_id").isNotNull()
).dropDuplicates(["product_id"])


dim_customer.write.jdbc(
    postgres_url,
    "dim_customer",
    mode="append",
    properties=postgres_properties
)

dim_seller.write.jdbc(
    postgres_url,
    "dim_seller",
    mode="append",
    properties=postgres_properties
)

dim_store.write.jdbc(
    postgres_url,
    "dim_store",
    mode="append",
    properties=postgres_properties
)

dim_supplier.write.jdbc(
    postgres_url,
    "dim_supplier",
    mode="append",
    properties=postgres_properties
)

dim_product.write.jdbc(
    postgres_url,
    "dim_product",
    mode="append",
    properties=postgres_properties
)


dc = spark.read.jdbc(postgres_url, "dim_customer", properties=postgres_properties).alias("dc")
ds = spark.read.jdbc(postgres_url, "dim_seller", properties=postgres_properties).alias("ds")
dst = spark.read.jdbc(postgres_url, "dim_store", properties=postgres_properties).alias("dst")
dsp = spark.read.jdbc(postgres_url, "dim_supplier", properties=postgres_properties).alias("dsp")
dp = spark.read.jdbc(postgres_url, "dim_product", properties=postgres_properties).alias("dp")
m = mock_data.alias("m")


fact_sales = m \
    .join(
        dc,
        F.col("m.sale_customer_id") == F.col("dc.customer_id"),
        "inner"
    ) \
    .join(
        ds,
        F.col("m.sale_seller_id") == F.col("ds.seller_id"),
        "inner"
    ) \
    .join(
        dp,
        F.col("m.sale_product_id") == F.col("dp.product_id"),
        "inner"
    ) \
    .join(
        dst,
        (F.col("m.store_name") == F.col("dst.store_name")) &
        (F.col("m.store_location") == F.col("dst.store_location")) &
        (F.col("m.store_city") == F.col("dst.store_city")) &
        (F.col("m.store_state") == F.col("dst.store_state")) &
        (F.col("m.store_country") == F.col("dst.store_country")) &
        (F.col("m.store_phone") == F.col("dst.store_phone")) &
        (F.col("m.store_email") == F.col("dst.store_email")),
        "left"
    ) \
    .join(
        dsp,
        (F.col("m.supplier_name") == F.col("dsp.supplier_name")) &
        (F.col("m.supplier_contact") == F.col("dsp.supplier_contact")) &
        (F.col("m.supplier_email") == F.col("dsp.supplier_email")) &
        (F.col("m.supplier_phone") == F.col("dsp.supplier_phone")) &
        (F.col("m.supplier_address") == F.col("dsp.supplier_address")) &
        (F.col("m.supplier_city") == F.col("dsp.supplier_city")) &
        (F.col("m.supplier_country") == F.col("dsp.supplier_country")),
        "left"
    ) \
    .select(
        F.col("m.id").alias("source_row_id"),
        F.col("dc.customer_key").alias("customer_key"),
        F.col("ds.seller_key").alias("seller_key"),
        F.col("dp.product_key").alias("product_key"),
        F.col("dst.store_key").alias("store_key"),
        F.col("dsp.supplier_key").alias("supplier_key"),
        F.to_date(F.col("m.sale_date"), "M/d/yyyy").alias("sale_date"),
        F.col("m.sale_quantity").cast("int").alias("sale_quantity"),
        F.col("m.sale_total_price").cast("decimal(12,2)").alias("sale_total_price")
    )


fact_sales.write.jdbc(
    postgres_url,
    "fact_sales",
    mode="append",
    properties=postgres_properties
)

print("star finished")
spark.stop()