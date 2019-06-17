import os
import getopt
import sys

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, IntegerType

import citationsCommon, citations, citationsDF

# Make sure these are set
#os.environ["Java_HOME"] = ...
#os.environ["SPARK_HOME"] = ...
os.environ["PYTHONHASHSEED"] = "2"

if __name__ == "__main__":

    if len(sys.argv) < 3:
        sys.exit("Usage: citationsRunner <outputdir> <inputfiles>")

    ovpairs, args = getopt.getopt(sys.argv[1:], "d", ["dataframe"])

    useDataFrames = False
    for opt, _ in ovpairs:
        if opt in ['-d', '--dataframe']:
            useDataFrames = True

    outputdir, *inputfiles = args

    print("outputdir: ", outputdir)
    print("inputfiles: ", inputfiles)
    if not inputfiles:
        sys.exit("Need an input file")

    useJson = inputfiles[0].endswith("json")

    spark = SparkSession.builder.getOrCreate()
    spark.conf.set("spark.sql.parquet.binaryAsString", True)
    sc = spark.sparkContext

    citationData = None
    if useJson:
      # prepare the schema
      schema = StructType([
          StructField('id', StringType(), False),
          StructField('references', ArrayType(StringType(), False), True),
          StructField('year', IntegerType(), False),
      ])
      citationData = spark.read.json(inputfiles, schema=schema)
    else:
      # parquet does not support schema
      citationData = spark.read.parquet(inputfiles[0])

    # citCountArrays: id, [array of citation counts by year]
    citCountArrays = None
    if useDataFrames:
      citCountArrays = citationsDF.citationCountsE2E(citationData, 34)
    else:
      citCountArrays = citations.citationCountsE2E(citationData, 34)

    averageByAggregate = citationsCommon.averageAggregates(citCountArrays, 100)

    citCountArrays.coalesce(10).saveAsTextFile(
            '{}/count_arrays.csv'.format(outputdir))
    open('{}/averages'.format(outputdir) , 'w').write(
            ",".join([str(x) for x in averageByAggregate]))
 
    spark.stop()
