
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import HashingTF, Tokenizer, StopWordsRemover
import sys
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.streaming import StreamingContext
import pyspark.sql.functions as func
import time
from pyspark.ml import PipelineModel

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import HashingTF, Tokenizer, StopWordsRemover
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructType, StructField, ArrayType
from pyspark.sql.functions import udf, from_json, col



if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("""
        Usage: structured_kafka_wordcount.py <bootstrap-servers> <subscribe-type> <topics>
        """, file=sys.stderr)
        sys.exit(-1)

    bootstrapServers = sys.argv[1]
    subscribeType = sys.argv[2]
    topics = sys.argv[3]

    appName = "Analise de Tendencia Suicída Spark"

    spark = SparkSession.builder.appName(appName).config("spark.some.config.option", "some-value").getOrCreate()
    lines = spark.readStream.format("kafka").option("kafka.bootstrap.servers", bootstrapServers).option("subscribe", topics).option("startingOffsets", "latest").load()
    spark.sparkContext.setLogLevel("ERROR")    

    schemaKafka=StructType([ StructField("tweet",StringType(),True),StructField("Sentimentos",IntegerType(),True)])
    

    lines_query = lines.selectExpr("cast(value as string)").select(func.col("value").cast("string").alias("tweet"))
   

    query1 = lines_query.writeStream.queryName("counting").format("memory").outputMode("append").start()
   

    pipeline_model = PipelineModel.load("/home/gabriel/Downloads/spark-3.0.3-bin-hadoop2.7/bin/path") 

    prediction = pipeline_model.transform(lines_query)
    query = prediction.writeStream.format("console").outputMode("append").start()
    query.awaitTermination()
