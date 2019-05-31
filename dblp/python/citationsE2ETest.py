import pytest
from . import citations
from pyspark.sql import Row
from pyspark.sql import SparkSession

pytestmark = pytest.mark.usefixtures("with_spark_context")

def test_citation_e2e(with_spark_context):
    # This is necessary in order for toDF() to be available.
    spark = SparkSession(with_spark_context)
    d = [
      Row(id='1', references=[2,3,4], year=2015),
      Row(id='2', references=[3,5], year=2014),
      Row(id='3', references=[6,7], year=2013),
      Row(id='4', references=[6], year=2013),
      Row(id='5', references=None, year=2013),
    ]
    df = with_spark_context.parallelize(d).toDF(["id", "references", "year"])
    ca = citations.citationCountsE2E(df, 1).collect()
    ca.sort()
    assert [('2', [0, 1]),
            ('3', [0, 1, 1]),
            ('4', [0, 0, 1]),
            ('5', [0, 1])] == ca


