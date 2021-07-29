from pathlib import Path

from pyspark import SparkContext
from pyspark.sql import SparkSession


sc = SparkContext(master="local[*]")
conf = sc.getConf()

spark = SparkSession.builder.config(conf=conf).appName("hello-world").getOrCreate()

# Read file
region_data_path = Path(__file__).parent.parent.parent / "data" / "lineitem.tbl"
print(region_data_path.exists(), "*****"*50)
df = spark.read.option("header", False).csv(path=str(region_data_path), sep="|")

print(df.count())
