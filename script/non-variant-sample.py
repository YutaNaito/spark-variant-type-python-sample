from pyspark.sql import SparkSession
from pyspark.sql.types import ByteType, StructType, StructField, StringType
import pyspark.sql.functions as f
import time


spark = (
    SparkSession.builder.appName("Nested JSON Example").master("local").getOrCreate()
)

json1 = '{"number": 1, "name": "A", "level1": {"number": 1, "name": "A", "level2": {"number": 1, "name": "A", "level3": {"number": 1, "name": "A", "level4": {"number": 1, "name": "A", "level5": {"number": 1, "name": "A", "level6": {"number": 1, "name": "A", "level7": {"number": 1, "name": "A", "level8": {"number": 1, "name": "A", "level9": {"number": 1, "name": "A", "level10": {"key": "value1"}}}}}}}}}}}'
json2 = '{"number": 2, "name": "B", "level1": {"number": 2, "name": "B", "level2": {"number": 2, "name": "B", "level3": {"number": 2, "name": "B", "level4": {"number": 2, "name": "B", "level5": {"number": 2, "name": "B", "level6": {"number": 2, "name": "B", "level7": {"number": 2, "name": "B", "level8": {"number": 2, "name": "B", "level9": {"number": 2, "name": "B", "level10": {"key": "value2"}}}}}}}}}}}'
json3 = '{"number": 3, "name": "C", "level1": {"number": 3, "name": "C", "level2": {"number": 3, "name": "C", "level3": {"number": 3, "name": "C", "level4": {"number": 3, "name": "C", "level5": {"number": 3, "name": "C", "level6": {"number": 3, "name": "C", "level7": {"number": 3, "name": "C", "level8": {"number": 3, "name": "C", "level9": {"number": 3, "name": "C", "level10": {"key": "value3"}}}}}}}}}}}'
json4 = '{"number": 4, "name": "D", "level1": {"number": 4, "name": "D", "level2": {"number": 4, "name": "D", "level3": {"number": 4, "name": "D", "level4": {"number": 4, "name": "D", "level5": {"number": 4, "name": "D", "level6": {"number": 4, "name": "D", "level7": {"number": 4, "name": "D", "level8": {"number": 4, "name": "D", "level9": {"number": 4, "name": "D", "level10": {"key": "value4"}}}}}}}}}}}'
json5 = '{"number": 5, "name": "E", "level1": {"number": 5, "name": "E", "level2": {"number": 5, "name": "E", "level3": {"number": 5, "name": "E", "level4": {"number": 5, "name": "E", "level5": {"number": 5, "name": "E", "level6": {"number": 5, "name": "E", "level7": {"number": 5, "name": "E", "level8": {"number": 5, "name": "E", "level9": {"number": 5, "name": "E", "level10": {"key": "value5"}}}}}}}}}}}'
json6 = '{"number": 6, "name": "F", "level1": {"number": 6, "name": "F", "level2": {"number": 6, "name": "F", "level3": {"number": 6, "name": "F", "level4": {"number": 6, "name": "F", "level5": {"number": 6, "name": "F", "level6": {"number": 6, "name": "F", "level7": {"number": 6, "name": "F", "level8": {"number": 6, "name": "F", "level9": {"number": 6, "name": "F", "level10": {"key": "value6"}}}}}}}}}}}'
json7 = '{"number": 7, "name": "G", "level1": {"number": 7, "name": "G", "level2": {"number": 7, "name": "G", "level3": {"number": 7, "name": "G", "level4": {"number": 7, "name": "G", "level5": {"number": 7, "name": "G", "level6": {"number": 7, "name": "G", "level7": {"number": 7, "name": "G", "level8": {"number": 7, "name": "G", "level9": {"number": 7, "name": "G", "level10": {"key": "value7"}}}}}}}}}}}'
json8 = '{"number": 8, "name": "H", "level1": {"number": 8, "name": "H", "level2": {"number": 8, "name": "H", "level3": {"number": 8, "name": "H", "level4": {"number": 8, "name": "H", "level5": {"number": 8, "name": "H", "level6": {"number": 8, "name": "H", "level7": {"number": 8, "name": "H", "level8": {"number": 8, "name": "H", "level9": {"number": 8, "name": "H", "level10": {"key": "value8"}}}}}}}}}}}'
json9 = '{"number": 9, "name": "I", "level1": {"number": 9, "name": "I", "level2": {"number": 9, "name": "I", "level3": {"number": 9, "name": "I", "level4": {"number": 9, "name": "I", "level5": {"number": 9, "name": "I", "level6": {"number": 9, "name": "I", "level7": {"number": 9, "name": "I", "level8": {"number": 9, "name": "I", "level9": {"number": 9, "name": "I", "level10": {"key": "value9"}}}}}}}}}}}'
json10 = '{"number": 10, "name": "J", "level1": {"number": 10, "name": "J", "level2": {"number": 10, "name": "J", "level3": {"number": 10, "name": "J", "level4": {"number": 10, "name": "J", "level5": {"number": 10, "name": "J", "level6": {"number": 10, "name": "J", "level7": {"number": 10, "name": "J", "level8": {"number": 10, "name": "J", "level9": {"number": 10, "name": "J", "level10": {"key": "value10"}}}}}}}}}}}'

df = spark.createDataFrame(
    [
        ["1", json1],
        ["2", json2],
        ["3", json3],
        ["4", json4],
        ["5", json5],
        ["6", json6],
        ["7", json7],
        ["8", json8],
        ["9", json9],
        ["10", json10],
    ],
    ["id", "json"],
)

schema = StructType(
    [
        StructField("number", ByteType(), True),
        StructField("name", ByteType(), True),
        StructField(
            "level1",
            StructType(
                [
                    StructField("number", ByteType(), True),
                    StructField("name", StringType(), True),
                    StructField(
                        "level2",
                        StructType(
                            [
                                StructField("number", ByteType(), True),
                                StructField("name", StringType(), True),
                                StructField(
                                    "level3",
                                    StructType(
                                        [
                                            StructField("number", ByteType(), True),
                                            StructField("name", StringType(), True),
                                            StructField(
                                                "level4",
                                                StructType(
                                                    [
                                                        StructField(
                                                            "number", ByteType(), True
                                                        ),
                                                        StructField(
                                                            "name", StringType(), True
                                                        ),
                                                        StructField(
                                                            "level5",
                                                            StructType(
                                                                [
                                                                    StructField(
                                                                        "number",
                                                                        ByteType(),
                                                                        True,
                                                                    ),
                                                                    StructField(
                                                                        "name",
                                                                        StringType(),
                                                                        True,
                                                                    ),
                                                                    StructField(
                                                                        "level6",
                                                                        StructType(
                                                                            [
                                                                                StructField(
                                                                                    "level7",
                                                                                    StructType(
                                                                                        [
                                                                                            StructField(
                                                                                                "number",
                                                                                                ByteType(),
                                                                                                True,
                                                                                            ),
                                                                                            StructField(
                                                                                                "name",
                                                                                                StringType(),
                                                                                                True,
                                                                                            ),
                                                                                            StructField(
                                                                                                "level8",
                                                                                                StructType(
                                                                                                    [
                                                                                                        StructField(
                                                                                                            "number",
                                                                                                            ByteType(),
                                                                                                            True,
                                                                                                        ),
                                                                                                        StructField(
                                                                                                            "name",
                                                                                                            StringType(),
                                                                                                            True,
                                                                                                        ),
                                                                                                        StructField(
                                                                                                            "level9",
                                                                                                            StructType(
                                                                                                                [
                                                                                                                    StructField(
                                                                                                                        "level10",
                                                                                                                        StructType(
                                                                                                                            [
                                                                                                                                StructField(
                                                                                                                                    "key",
                                                                                                                                    StringType(),
                                                                                                                                    True,
                                                                                                                                )
                                                                                                                            ]
                                                                                                                        ),
                                                                                                                        True,
                                                                                                                    )
                                                                                                                ]
                                                                                                            ),
                                                                                                            True,
                                                                                                        ),
                                                                                                    ]
                                                                                                ),
                                                                                                True,
                                                                                            ),
                                                                                        ]
                                                                                    ),
                                                                                    True,
                                                                                )
                                                                            ]
                                                                        ),
                                                                        True,
                                                                    ),
                                                                ]
                                                            ),
                                                            True,
                                                        ),
                                                    ]
                                                ),
                                                True,
                                            ),
                                        ]
                                    ),
                                    True,
                                ),
                            ]
                        ),
                        True,
                    ),
                ]
            ),
            True,
        ),
    ]
)

start_time = time.time()

parsed_df = df.withColumn("json", f.from_json(f.col("json"), schema).alias("key"))

result_df = parsed_df.select(
    f.col("id"),
    f.col(
        "json.level1.level2.level3.level4.level5.level6.level7.level8.level9.level10.key"
    ).alias("key"),
)

end_time = time.time()

result_df.show()

processing_time_ms = (end_time - start_time) * 1000
print(f"処理時間: {processing_time_ms:.2f} ms")

parsed_df.printSchema()

parsed_df.explain(True)

memory_size1 = spark._jvm.org.apache.spark.util.SizeEstimator.estimate(df._jdf)
memory_size2 = spark._jvm.org.apache.spark.util.SizeEstimator.estimate(parsed_df._jdf)

print(f"JSONのパース前：{memory_size1}byte")
print(f"JSONのパース後：{memory_size2}byte")

spark.stop()
