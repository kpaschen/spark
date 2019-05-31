import findspark
findspark.init()

import logging
import pytest

from pyspark import SparkContext

@pytest.fixture(scope='session')
def with_spark_context():
    spark_context = SparkContext("local", "citationstest")
    return spark_context

