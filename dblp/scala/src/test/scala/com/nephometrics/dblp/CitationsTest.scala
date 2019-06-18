package com.nephometrics.dblp

import org.apache.spark.sql.types._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.FunSuite


// This just makes it easier to create test data.
case class Publication(id: String, year: Int, venue: String,
  references: Option[Array[String]])

class DblpCitationsTest extends FunSuite with BeforeAndAfterAll {
  var stats: Citations = _

  var sourceDF: DataFrame = _
  var baseDataSet: Dataset[BasePublication] = _

  // For some reason that I don't understand yet, doing the setup
  // in beforeEach() is actually faster than doing it in beforeAll().
  // However, I want to make sure stats and sourceDF are only
  // initialized once.
  override def beforeAll(): Unit = {
    val spark: SparkSession = SparkSession
    .builder()
    .appName("citations")
    .master("local")
    .config("spark.sql.shuffle.partitions", "1")
    .config("spark.ui.enabled", "false")
    .getOrCreate()

    // spark.sparkContext.setLogLevel(org.apache.log4j.Level.ERROR.toString())

    import spark.implicits._

    stats = new Citations(spark)
    // pub1 published year 10, no citations
    // pub2 published year 9, cited by pub1 and pub5 in year 10 (at age 1)
    // pub3 published year 8, cited by pub2 in year 9 and pub1 in year 10
    // pub5 published in year 10, no citations
    // pub4 does not have an entry
    sourceDF = Seq(
      Publication("pub1", 10, "here", Some(Array("pub2", "pub3"))),
      Publication("pub2", 9, "there", Some(Array("pub3", "pub4"))),
      Publication("pub3", 8, "there", None),
      Publication("pub5", 10, "there", Some(Array("pub2", "pub4")))
    ).toDF()

    baseDataSet = stats.idRefYearDs(sourceDF)

    super.beforeAll()
  }

  test("BaseDataConversion") {
    assert(baseDataSet.count() == 3)
    val results = baseDataSet.collect()

    for (r <- results) {
      r.id match {
        case "pub1" => {
          assert(r.year == 10, "pub1 has year " + r.year + " instead of 10")
          val refs = r.references
          assert(refs.contains("pub2"))
          assert(refs.contains("pub3"))
          assert(refs.length == 2)
        }
        case "pub2" => {
          assert(r.year == 9)
          val refs = r.references
          assert(refs.contains("pub3"))
          assert(refs.contains("pub4"))
          assert(refs.length == 2)
        }
        case "pub5" => {
          assert(r.year == 10)
          val refs = r.references
          assert(refs.contains("pub2"))
          assert(refs.contains("pub4"))
          assert(refs.length == 2)
        }
        case unexpected => 
          fail(s"unexpected publication $unexpected found in result set.")
      }
    }
  }

  test("citations are counted by year") {
    // It looks like maybe spark or the test runner are smart
    // enough to re-use this. Because the next test becomes
    // slower when I disable this test, so I'm pretty sure there is some
    // data re-use going on.
    // But maybe only when baseDataSet is initialised in beforeAll(),
    // which only works in 2.12?
    val counted = stats.countCitationsByYear(baseDataSet)
    val results = counted.collect()
    assert(counted.count() == 5)

    for (r <- results) {
      r(0) match {
        case "pub2" => assert(10 == r(1))
                       assert(2 == r(2))
        case "pub3" => assert(9 == r(1) || 10 == r(1))
                       assert(1 == r(2))
        case "pub4" => assert(9 == r(1) || 10 == r(1))
                       assert(1 == r(2))
      }
    }
  }

  test("citation age is computed from year published and year cited") {
    val counted = stats.countCitationsByYear(baseDataSet)
    val cited = stats.countCitationsByAge(counted, sourceDF)
    val results = cited.collect()
    assert(5 == cited.count())

    // Expect to see:
    // pub2: cited twice in year 10 at age 1
    // pub3: cited once in year 9 and once in year 10 at ages 1 and 2 resp.
    // pub1, pub5: never cited
    for (r <- results) {
      r.id match {
        case "pub1" => assert(r.yearCited.isEmpty)
        case "pub2" => assert(10 == r.yearCited.get)
                       assert(2 == r.count.get)
        case "pub5" => assert(r.yearCited.isEmpty)
        case "pub3" => assert(r.yearCited.get == 9 || r.yearCited.get == 10)
                       assert(1 == r.count.get)
      }
    }
  }

  test("citation history collected for publications") {
    val counted = stats.countCitationsByYear(baseDataSet)
    val cited = stats.countCitationsByAge(counted, sourceDF)
    val history = stats.collectCitationHistory(cited)

    val results = history.collect()
    // Expectations: pub1, pub5 have empty histories
    // pub3: 0, 1, 1
    // pub2: 0, 2
    for (r <- results) {
      r.id match {
        case "pub1" => assert(r.history.isEmpty)
        case "pub5" => assert(r.history.isEmpty)
        case "pub2" => assert(r.history.length == 2)
                       assert(r.history.apply(0) == 0)
                       assert(r.history.apply(1) == 2)
        case "pub3" => assert(r.history.length == 3)
                       assert(r.history.apply(0) == 0)
                       assert(r.history.apply(1) == 1)
                       assert(r.history.apply(2) == 1)
      }
    }
  }
}
