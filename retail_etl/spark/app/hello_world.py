from pyspark import SparkContext


# Create spark context
sc = SparkContext()

# Read file
logData = sc.range(1, 10).count()

# Print result
print("Lines with a: {}".format(logData))
