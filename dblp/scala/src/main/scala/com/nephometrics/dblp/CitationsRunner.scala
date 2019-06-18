// The main object for running the citations pipeline.

package com.nephometrics.dblp

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

/**
 * Example Usage:
 * {{{
 * sbt run CitationsRunner <path to output> <input files>
 * If you have wildcards in the input files, enclose them in quotes
 * to avoid the shell expanding the wildcards.
 * }}}
 */
object CitationsRunner {
  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println("Usage: CitationsRunner <output path> <input files>")
      System.exit(1)
    }

    //val filenames = args.slice(1, args.length)
    val filenames = args(1)
    val outputs = args(0)

    System.err.println("outputs to to " + outputs)
    System.err.println("inputs: " + filenames)

    val spark = SparkSession
      .builder
      .appName("citations")
      .getOrCreate()

    //spark.sparkContext.setLogLevel(org.apache.log4j.Level.ERROR.toString())
    val stats = new Citations(spark)

    // You can't initialize this using _ because the JVM doesn't know
    // how to create a default instance. The null.asInstanceOf[T] pattern
    // is apparently the recommended option.
    var baseData:DataFrame = null.asInstanceOf[DataFrame]

    // See if we're loading json or parquet data.
    if (filenames.endsWith("json")) {
      // This selects a subset of fields and specifies types and nullability,
      // which is useful because I know the id field is not null.
      // Also ensures year is an integer not a long.
      val schema = StructType(Array(
        StructField("id", StringType, false),
        StructField("references", ArrayType(StringType, false), true),
        StructField("year", IntegerType, false)))
        baseData = spark.read.schema(schema).json(filenames)
    } else {
      // You can't use a schema to read a parquet file when that requires
      // typecasting (UnsupportedOperationException), so read it like this,
      // the later 'select' statements will be clever enough to only read
      // what they need.
      val tmp = spark.read.parquet(filenames)
      baseData = tmp.withColumn("year", tmp("year").cast(IntegerType))
    }
    val idRefs = stats.idRefYearDs(baseData)
    val counted = stats.countCitationsByYear(idRefs)
    val cited = stats.countCitationsByAge(counted, baseData)
    val history = stats.collectCitationHistory(cited)

    // 10 is based on experience, this will lead to about 20MB per file.
    history.repartition(10).write.format("json").save(outputs)

    spark.stop()
  }
}
