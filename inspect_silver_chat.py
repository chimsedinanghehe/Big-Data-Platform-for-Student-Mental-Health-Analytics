from pyspark.sql import SparkSession
from pyspark.sql import functions as F

path = "gs://student-mental-health-lake-nhom1-2026/silver/anonymized_chat/"

spark = SparkSession.builder.appName("inspect-silver-chat").getOrCreate()
df = spark.read.parquet(path)

print("Silver row count:", df.count())
df.printSchema()
df.select(
    "event_id",
    "timestamp",
    "anonymous_session_id",
    "question_clean",
    "answer_clean",
    "standalone_query_clean",
    "display_question",
    "risk_level",
    "sentiment",
    "topic",
    "sensitive_flag",
).show(20, truncate=False)

df.groupBy("date", "hour").count().orderBy("date", "hour").show()
df.groupBy("risk_level").count().show()
df.groupBy("sentiment").count().show()
df.groupBy("topic").count().show()

spark.stop()