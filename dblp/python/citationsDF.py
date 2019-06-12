from decimal import localcontext,Decimal
from pyspark.sql.types import IntegerType, ArrayType
from pyspark.sql.functions import col, collect_list, struct, udf
import pyspark.sql.functions

# The pairsToArray method gets called from a mapper, so make sure
# it's easy to serialize.
class pairsToArrayHelper(object):
    @staticmethod
    def pairsToArray(pairs):
        return [pairs[x] if (x in pairs) else 0 for x in range(max(pairs.keys()) + 1)]

class aggregationHelper(object):
    @staticmethod
    def seqOp(acc, newItem):
        for idx, value in enumerate(newItem[1]):
            acc[0][idx] += value
            acc[1][idx] += 1
        return acc
    
    @staticmethod
    def combOp(acc1, acc2):
        return ([x1 + x2 for x1,x2 in zip(acc1[0], acc2[0])],
                [y1 + y2 for y1,y2 in zip(acc1[1], acc2[1])])

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
    p2Afunc = pairsToArrayHelper.pairsToArray
    return tmp.withColumn('ageCountMap',
            pyspark.sql.functions.map_from_entries('ageCountPairs')).drop(
                    'ageCountPairs').rdd.mapValues(lambda x: p2Afunc(x))

def averageAggregates(cc_arrays, max_years):
    startingTuple = ([0] * max_years, [0] * max_years)
    sumsAndCounts = cc_arrays.aggregate(startingTuple,
            aggregationHelper.seqOp, aggregationHelper.combOp)
    with localcontext() as ctx:
        ctx.prec = 3
        return [float((Decimal(x) / Decimal(y)) if y > 0 else 0.0) for x,y in zip(sumsAndCounts[0], sumsAndCounts[1])]

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


