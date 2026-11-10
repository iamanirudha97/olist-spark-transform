from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def main():
    spark = (
        SparkSession.builder
        .appName("OlistSparkSmokeTest")
        .master("local[*]")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    print(f"Spark version: {spark.version}")

    data = [
        ("ORD-001", 100.0),
        ("ORD-002", 250.0),
        ("ORD-003", 75.0),
    ]

    df = spark.createDataFrame(
        data,
        ["order_id", "price"]
    )

    print("Original DataFrame:")
    df.show()

    print("DataFrame schema:")
    df.printSchema()

    result = df.withColumn(
        "price_with_tax",
        F.round(F.col("price") * 1.10, 2)
    )

    print("Transformed DataFrame:")
    result.show()

    spark.stop()


if __name__ == "__main__":
    main()
