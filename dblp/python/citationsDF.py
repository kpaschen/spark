from pyspark.sql.types import IntegerType, ArrayType
from pyspark.sql.functions import col, collect_list, struct, udf
import pyspark.sql.functions

import citationsCommon

def countByIdAndYear(df):
    docsplit = df.rdd.flatMap(lambda row:
            [('{}.{}'.format(ref, row[2]), 1) for ref in row[1]])
    return docsplit.toDF().groupBy("_1").agg({"_2": "sum"}).withColumnRenamed(
            "sum(_2)", "citationCount")

def joinIdYearAge(idYearCount, df):
    # idYear: id, year cited
    split_id_year = pyspark.sql.functions.split(idYearCount['_1'], r'\.')
    citCountsSplit = idYearCount.withColumn(
            'id', split_id_year.getItem(0)).withColumn(
                    'yearCited', split_id_year.getItem(1).cast(IntegerType()))

    # dfpairs: id, year published
    dfpairs = df.select("id", "year").withColumn(
            "year", df["year"].cast(IntegerType()))

    # idYearAge: id, year cited - year published
    return citCountsSplit.join(dfpairs,
            citCountsSplit['id'] == dfpairs['id']).withColumn(
                    'age', col('yearCited') - col('year')).drop(dfpairs['id'])

def citationCountArrays(idYearAge):
    # idYearAge contains citationCount, id, yearCited, year, age
    # combine 'age' and 'citationCount' into pairs, then drop everything
    # except for 'id' and those pairs, group by id and make lists of the pairs.
    tmp = idYearAge.drop('_1').drop('year').filter(
            col('age') > -1).withColumn('ageCountPair', struct(
                idYearAge.age, idYearAge.citationCount)).drop(
                        'age').drop('citationCount').groupBy('id').agg(
                                collect_list('ageCountPair').alias('ageCountPairs'))
    # now tmp rows have an id and a list of ageCountPairs, and each ageCountPair
    # is a Row with age and citationCount. At this point, we can either
    # continue processing the data using udfs or various array or map functions
    # on dataframes, or just convert tmp to an rdd and use mapValues on it.
    # I ended up doing the latter since it seems way easier and is probably no 
    # less efficient than using a udf from python.
    # tmp.select(pyspark.sql.functions.map_from_entries('ageCountPairs')).show()
    p2Afunc = citationsCommon.pairsToArrayHelper.pairsToArray
    return tmp.withColumn('ageCountMap',
            pyspark.sql.functions.map_from_entries('ageCountPairs')).drop(
                    'ageCountPairs').rdd.mapValues(lambda x: p2Afunc(x))

# df is the dataframe read from json before we've filtered out rows where
# references is NULL
# partitionCount says how many partitions to coalesce the intermediate
# data to.
def citationCountsE2E(df, partitionCount=34):
    citers = df.select("id", "references", "year").filter("references is not NULL")
    idYearCount = countByIdAndYear(citers)
    # For publication dates, include publications with no references.
    idYearAge = joinIdYearAge(idYearCount, df)
    citCountArrays = citationCountArrays(idYearAge.coalesce(partitionCount))
    return citCountArrays


