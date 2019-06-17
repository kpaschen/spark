import pytest
from . import citations,citationsCommon
from pyspark.sql import Row

pytestmark = pytest.mark.usefixtures("with_spark_context")

def test_pairs_to_array():
    pairs = [(1, 2), (3, 6), (4, 8)]
    arr = citationsCommon.pairsToArrayHelper.pairsToArray(pairs)
    assert arr == [0, 2, 0, 6, 8]

def test_count_by_id_and_year(with_spark_context):
    d = [
      Row(id='1', references=[2,3,4], year=2015),
      Row(id='2', references=[3,5], year=2014),
      Row(id='3', references=[6,7], year=2013),
      Row(id='4', references=[6], year=2013),
    ]
    rdd = with_spark_context.parallelize(d)
    count = citations.countByIdAndYear(rdd).collect()
    assert [('2.2015', 1),
            ('3.2015', 1),
            ('4.2015', 1),
            ('3.2014', 1),
            ('5.2014', 1),
            ('6.2013', 2),
            ('7.2013', 1)] == count

def test_join_id_year_age(with_spark_context):
    d = [
      Row(id='1', references=[2,3,4], year=2015),
      Row(id='2', references=[3,5], year=2014),
      Row(id='3', references=[6,7], year=2013),
      Row(id='4', references=[6], year=2013),
      # add one publication that has a reference pre-publication date.
      Row(id='5', references=[1], year=2012),
    ]
    rdd = with_spark_context.parallelize(d)
    count = citations.countByIdAndYear(rdd)
    id_year_age = citations.joinIdYearAge(count, rdd).collect()
    assert [('4.2015', 2),
            ('2.2015', 1),
            ('3.2015', 2),
            ('3.2014', 1),
            ('5.2014', 2)] == id_year_age

def test_citation_count_arrays(with_spark_context):
    d = [
      Row(id='1', references=[2,3,4], year=2015),
      Row(id='2', references=[3,5], year=2014),
      Row(id='3', references=[6,7], year=2013),
      Row(id='4', references=[6], year=2013),
    ]
    rdd = with_spark_context.parallelize(d)
    count = citations.countByIdAndYear(rdd)
    id_year_age = citations.joinIdYearAge(count, rdd)
    ca = citations.citationCountArrays(id_year_age, count).collect()
    assert [('2', [0, 1]),
            ('4', [0, 0, 1]),
            ('3', [0, 1, 1])] == ca

def test_average_aggregates(with_spark_context):
    d = [
      Row(id='1', references=[2,3,4], year=2015),
      Row(id='2', references=[3,5], year=2014),
      Row(id='3', references=[6,7], year=2013),
      Row(id='4', references=[6], year=2013),
    ]
    rdd = with_spark_context.parallelize(d)
    count = citations.countByIdAndYear(rdd)
    id_year_age = citations.joinIdYearAge(count, rdd)
    ca = citations.citationCountArrays(id_year_age, count)
    agg = citations.averageAggregates(ca, 5)
    assert [0.0, 0.667, 1.0, 0.0, 0.0] == agg

