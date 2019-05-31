from decimal import localcontext,Decimal

# The pairsToArray method gets called from a mapper, so make sure
# it's easy to serialize.
class pairsToArrayHelper(object):
    @staticmethod
    def pairsToArray(pairs):
        d = dict(pairs)
        return [d[x] if (x in d) else 0 for x in range(max(d.keys()) + 1)]

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

def countByIdAndYear(rdd):
    docsplit = rdd.flatMap(lambda row:
            [('{}.{}'.format(ref, row[2]), 1) for ref in row[1]])
    return docsplit.reduceByKey(lambda c, d: c + d)

def joinIdYearAge(idYearCount, rdd):
    # idYear: id, year cited
    idYear = idYearCount.map(lambda row: (row[0][:-5], int(row[0][-4:])))
    # ddpairs: id, year published
    ddpairs = rdd.map(lambda row: (row[0], row[2]))

    # idYearAge: id, year cited - year published
    return idYear.join(ddpairs).filter(lambda row: (row[1][0] - row[1][1] >= -2)).map(
            lambda row: ('{}.{}'.format(row[0], row[1][0]), (row[1][0] - row[1][1])))

def citationCountArrays(idYearAge, idYearCount):
    p2Afunc = pairsToArrayHelper.pairsToArray
    return idYearAge.join(idYearCount).map(
            lambda row: (row[0][:-5], [(row[1][0], row[1][1])])).reduceByKey(
                    lambda c, d: c + d).mapValues(lambda x: p2Afunc(x))

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
    dd = df.select("id", "references", "year").filter("references is not NULL").rdd
    idYearCount = countByIdAndYear(dd)
    # For publication dates, include publications with no references.
    idYearAge = joinIdYearAge(idYearCount, df.rdd)
    citCountArrays = citationCountArrays(idYearAge.coalesce(partitionCount), idYearCount)
    return citCountArrays


