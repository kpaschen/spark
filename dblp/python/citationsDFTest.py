import sys
import pytest
from . import citationsDF
from pyspark.sql import Row,SparkSession
import pyspark.sql.functions

pytestmark = pytest.mark.usefixtures("with_spark_context")

def test_count_by_id_and_year(with_spark_context):
    d = [
      Row(id='1', references=[2,3,4], year=2015),
      Row(id='2', references=[3,5], year=2014),
      Row(id='3', references=[6,7], year=2013),
      Row(id='4', references=[6], year=2013),
    ]
    spark = SparkSession(with_spark_context)
    df = with_spark_context.parallelize(d).toDF()
    count = citationsDF.countByIdAndYear(df).collect()
    assert 7 == len(count)
    for row in count:
        if '2.2015' == row._1:
            assert 1 == row.citationCount
        elif '3.2015' == row._1:
            assert 1 == row.citationCount
        elif '4.2015' == row._1:
            assert 1 == row.citationCount
        elif '3.2014' == row._1:
            assert 1 == row.citationCount
        elif '5.2014' == row._1:
            assert 1 == row.citationCount
        elif '6.2013' == row._1:
            assert 2 == row.citationCount
        elif '7.2013' == row._1:
            assert 1 == row.citationCount
        else:
            print('unexpected key: ', row._1)
            assert 0

def test_join_id_year_age(with_spark_context):
    d = [
      Row(id='1', references=[2,3,4], year=2015),
      Row(id='2', references=[3,5], year=2014),
      Row(id='3', references=[6,7], year=2013),
      Row(id='4', references=[6], year=2013),
      # add one publication that has a reference pre-publication date.
      Row(id='5', references=[1], year=2012),
    ]
    spark = SparkSession(with_spark_context)
    df = with_spark_context.parallelize(d).toDF()
    count = citationsDF.countByIdAndYear(df)
    id_year_age = citationsDF.joinIdYearAge(count, df).collect()
    assert 6 == len(id_year_age)
    for row in id_year_age:
        if '4.2015' == row._1:
            assert 2 == row.age
        elif '2.2015' == row._1:
            assert 1 == row.age
        elif '3.2015' == row._1:
            assert 2 == row.age
        elif '3.2014' == row._1:
            assert 1 == row.age
        elif '5.2014' == row._1:
            assert 2 == row.age
        # The filter for negative age gets applied later.
        elif '1.2012' == row._1:
            assert -3 == row.age
        else:
            print('unexpected row ', row)
            assert 0

def test_citation_count_arrays(with_spark_context):
    d = [
      Row(id='1', references=[2,3,4], year=2015),
      Row(id='2', references=[3,5], year=2014),
      Row(id='3', references=[6,7], year=2013),
      Row(id='4', references=[6], year=2013),
      Row(id='5', references=[1], year=2012),
    ]
    spark = SparkSession(with_spark_context)
    df = with_spark_context.parallelize(d).toDF()
    count = citationsDF.countByIdAndYear(df)
    id_year_age = citationsDF.joinIdYearAge(count, df)
    ca = citationsDF.citationCountArrays(id_year_age).collect()
    print(ca, file=sys.stderr)
    assert 4 == len(ca)
    for row in ca:
        if '2' == row[0]:
            assert [0, 1] == row[1]
        elif '3' == row[0]:
            assert [0, 1, 1] == row[1]
        elif '4' == row[0]:
            assert [0, 0, 1] == row[1]
        elif '5' == row[0]:
            assert [0, 0, 1] == row[1]
        else:
            print('unexpected row ', row)
            assert 0

def test_average_aggregates(with_spark_context):
    d = [
      Row(id='1', references=[2,3,4], year=2015),
      Row(id='2', references=[3,5], year=2014),
      Row(id='3', references=[6,7], year=2013),
      Row(id='4', references=[6], year=2013),
    ]
    spark = SparkSession(with_spark_context)
    df = with_spark_context.parallelize(d).toDF()
    count = citationsDF.countByIdAndYear(df)
    id_year_age = citationsDF.joinIdYearAge(count, df)
    ca = citationsDF.citationCountArrays(id_year_age)
    agg = citationsDF.averageAggregates(ca, 5)
    assert [0.0, 0.667, 1.0, 0.0, 0.0] == agg

