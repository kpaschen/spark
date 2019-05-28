// The main object for running the citations pipeline.

package com.nephometrics.dblp

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

/**
 * Example Usage:
 * {{{
 * sbt run CitationsRunner ../../data/dblp-ref-*.json
 * }}}
 */
object CitationsRunner {
  def main(args: Array[String]) {
    if (args.length < 1) {
      System.err.println("Usage: CitationsRunner <file>")
      System.exit(1)
    }

    val filename = args(0)

    val spark = SparkSession
      .builder
      .appName("citations")
      .getOrCreate()

    // spark.sparkContext.setLogLevel(org.apache.log4j.Level.ERROR.toString())
    val stats = new Citations(spark)

    // This selects a subset of fields and specifies types and nullability,
    // which is useful because I know the id field is not null.
    // Also ensures year is an integer not a long.
    val schema = StructType(Array(
      StructField("id", StringType, false),
      StructField("references", ArrayType(StringType, false), true),
      StructField("year", IntegerType, false)))
    val baseData = spark.read.schema(schema).json(filename)
    val idRefs = stats.idRefYearDs(baseData)
    val counted = stats.countCitationsByYear(idRefs)
    val cited = stats.countCitationsByAge(counted, baseData)
    val history = stats.collectCitationHistory(cited)

    // 10 is based on experience, this will lead to about 20MB per file.
    history.repartition(10).write.format("json").save("/tmp/citationhistory")

    spark.stop()
  }
}
