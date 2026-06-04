from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp, datediff, when, col
import sys


def silver_transform():
    spark = SparkSession.builder.appName("olist_silver")\
    .config("spark.driver.extraClassPath", "/mnt/d/Practise Projects/E-commerce/drivers/postgresql.jar") \
    .config("spark.executor.extraClassPath", "/mnt/d/Practise Projects/E-commerce/drivers/postgresql.jar") \
    .master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    try:
        bronze_data_path = "/mnt/d/Practise Projects/E-commerce/data_lake/bronze"
        silver_data_path = "/mnt/d/Practise Projects/E-commerce/data_lake/silver"

        #read from bronze
        customers_df = spark.read.parquet(f"{bronze_data_path}/olist_customers_dataset")
        orders_df = spark.read.parquet(f"{bronze_data_path}/olist_orders_dataset")
        order_items_df = spark.read.parquet(f"{bronze_data_path}/olist_order_items_dataset")
        products_df = spark.read.parquet(f"{bronze_data_path}/olist_products_dataset")
        payments_df = spark.read.parquet(f"{bronze_data_path}/olist_order_payments_dataset")
        sellers_df = spark.read.parquet(f"{bronze_data_path}/olist_sellers_dataset")
        product_category_df = spark.read.parquet(f"{bronze_data_path}/product_category_name_translation")


        #perform transformations
        # convert order timestamps from string
        orders_df = orders_df.withColumn("order_purchase_timestamp", to_timestamp("order_purchase_timestamp"))  
        orders_df = orders_df.withColumn("order_delivered_customer_date", to_timestamp("order_delivered_customer_date"))  
        orders_df = orders_df.withColumn("order_estimated_delivery_date", to_timestamp("order_estimated_delivery_date"))  
        orders_df = orders_df.withColumn("delivery_delay_days",datediff("order_delivered_customer_date", "order_estimated_delivery_date"))
        orders_df = orders_df.withColumn("is_late", when(col("delivery_delay_days") > 0, 1).otherwise(0))
        



        #join dataframes
        silver_df = customers_df.join(orders_df, "customer_id", "inner")\
                                .join(order_items_df, "order_id", "left")\
                                .join(products_df, "product_id", "left")\
                                .join(payments_df, "order_id", "left")\
                                .join(sellers_df, "seller_id", "left")\
                                .join(product_category_df, "product_category_name", "left") 
        

        silver_df = silver_df.withColumn("total_item_value",col("freight_value") + col("price"))

        # write to silver in a file
        silver_df.select(
            "order_id",
            "customer_id",
            "order_status",
            "order_purchase_timestamp",
            "order_delivered_customer_date",
            "order_estimated_delivery_date",
            "delivery_delay_days",
            "is_late",
            "product_id",
            "product_category_name_english",
            "seller_id",
            "seller_city",
            "seller_state",
            "price",
            "freight_value",
            "total_item_value",
            "payment_type",
            "payment_value",
            "customer_city",
            "customer_state"
        ).write.mode("overwrite").parquet(f"{silver_data_path}/orders_enriched")


        #sanity check
        df = spark.read.parquet('/mnt/d/Practise Projects/E-commerce/data_lake/silver/orders_enriched')
        print('Rows:', df.count())
        print('Columns:', df.columns)

        print("Writing to PostgreSQL...", flush=True)
        df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/olist_dw") \
        .option("dbtable", "silver_orders_enriched") \
        .option("user", "olist") \
        .option("password", "olist") \
        .option("driver", "org.postgresql.Driver") \
        .mode("overwrite") \
        .save()

        print("Done — silver loaded into PostgreSQL!", flush=True)

        print("Silver layer done!")
    except Exception as e:
        print(f"Error processing silver layer: {e}")    

    spark.stop()


if __name__ == "__main__":
    silver_transform()

    