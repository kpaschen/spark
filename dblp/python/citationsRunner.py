import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, IntegerType

import citations

# Make sure these are set
#os.environ["Java_HOME"] = "/usr/lib64/jvm/jre-1.8.0-openjdk"
#os.environ["SPARK_HOME"] = "/home/kathrin/textbooks/spark/spark-2.4.0-bin-hadoop2.7"
os.environ["PYTHONHASHSEED"] = "2"

if __name__ == "__main__":

    if len(sys.argv) < 3:
        sys.exit("Usage: citationsRunner <inputfiles> <outputdir>")

    outputdir, *inputfiles = sys.argv[1:]

    print("outputdir: %s", outputdir)
    print("inputfiles: %s", inputfiles)

    # spark = SparkSession.builder.master("local[*]").getOrCreate()
    # TODO: configure spark session here?
    spark = SparkSession.builder.getOrCreate()
    sc = spark.sparkContext

    # prepare the schema
    schema = StructType([
        StructField('id', StringType(), False),
        StructField('references', ArrayType(StringType(), False), True),
        StructField('year', IntegerType(), False),
    ])

    print("reading from %s", inputfiles)
    citationData = spark.read.json(inputfiles, schema=schema)
    # citCountArrays: id, [array of citation counts by year]
    citCountArrays = citations.citationCountsE2E(citationData, 34)

    averageByAggregate = citations.averageAggregates(citCountArrays, 100)

    print("writing output to %s", outputdir)
    citCountArrays.coalesce(10).saveAsTextFile('{}/count_arrays.csv'.format(outputdir))
    # This save only works with dataframes.
    #citCountArrays.coalesce(1).write.format('com.databricks.spark.csv').options(
    #        header='true').save(
    #                'file:///{}/count_arrays.csv'.format(outputdir))
    open('{}/averages'.format(outputdir) , 'w').write(
            ",".join([str(x) for x in averageByAggregate]))
 
    spark.stop()
