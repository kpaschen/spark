package com.nephometrics.dblp

import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._


case class BasePublication(id: String, references: Array[String], year: Int)

case class CitedPublication(id: String, yearCited: Option[Int],
  age: Option[Int], count: Option[Int])

// Map publication id to a list of citation counts by age.
// The ith entry in the history is the number of citations i years after
// publication.
case class CitationHistory(id: String, history: Array[Int])

class Citations(@transient val spark: SparkSession) {

  def this() = this(SparkSession.builder.getOrCreate())

  import spark.implicits._

  def idRefYearDs(baseData: DataFrame): Dataset[BasePublication] = {
    baseData.select("id", "references", "year").filter(
      "references is not NULL").as[BasePublication]
  }

  // Take a dataset of BasePublications, invert on the reference ids,
  // group by reference id + yearCited, sum the citation counts.
  def countCitationsByYear(publications: Dataset[BasePublication]): DataFrame = {
     val citedPublications = publications.flatMap(row =>
         for(b <- row.references) yield (b, row.year, 1))
     // Make sure the citation count is an integer.
     val x = citedPublications.groupBy("_1", "_2").agg(sum(col("_3")))
       .withColumnRenamed("sum(_3)", "tmp")
     x.withColumn("citationCount", x("tmp").cast(IntegerType))
       .drop("tmp")
  }

  // Given the output of countCitationsByYear (a DataFrame[string, int, int])
  // as well as a Dataframe with publication ids and years of publication,
  // create a Dataframe mapping publication id to <year the publication
  // was cited, years since the publication was published,
  // number of times the publication was cited that year>.
  // Convert the dataframe (intermediate type due to the way joins work)
  // to a Dataset[CitedPublication].
  def countCitationsByAge(citCountByYear: DataFrame,
    baseTable: DataFrame): Dataset[CitedPublication] = {
      // This gets us <id -> yearPublished>
      // You should run this on the original base table in order to
      // get publication dates also for publications with no references.
      val ddpairs = baseTable.select("id", "year")
      citCountByYear.join(ddpairs, citCountByYear("_1") === ddpairs("id"),
        joinType="right_outer")
        .drop("_1")
        .withColumn("age", when($"_2".isNotNull,
          $"_2" - $"year").otherwise(lit(0)))
        .drop("year")
        .toDF("yearCited", "count", "id", "age")
        .as[CitedPublication]
  }

  // Take a dataframe of CitedPublication and combine records for the same
  // publication to get lists of pairs (age, citationcount).
  // Then turn those into arrays where the ith entry is the number of citations
  // i years after publication.
  // For publications with no citations, the list is empty.
  def collectCitationHistory(citCounts: Dataset[CitedPublication]): Dataset[CitationHistory] = {
    val tmp = citCounts.map(row => (row.id, row.count match {
      case Some(x) => Array[Tuple2[Int, Int]]((row.age getOrElse 0, x))
      case None => Array[Tuple2[Int, Int]]()
    })).groupByKey(_._1).reduceGroups(
      (a, b) => (a._1, a._2 ++ b._2)).map(row => (row._2._1, row._2._2.toMap))
    tmp.map(row => (row._1, {
      var l = Array[Int]()
      if (!row._2.isEmpty) {
        val mx = row._2.keys.max
        for (a <- 0 to mx) {
          l :+= row._2 getOrElse(a, 0)
        }
      }; l;
    }
    )).toDF("id", "history").as[CitationHistory]
  }
}
