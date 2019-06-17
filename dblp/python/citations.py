import citationsCommon

def countByIdAndYear(rdd):
    docsplit = rdd.flatMap(lambda row:
            [('{}.{}'.format(ref, row[2]), 1) for ref in row[1]])
    return docsplit.reduceByKey(lambda c, d: c + d)

def joinIdYearAge(idYearCount, ddpairs):
    # idYear: id, year cited
    idYear = idYearCount.map(lambda row: (row[0][:-5], int(row[0][-4:])))
    # ddpairs is expected to be: id, year published

    # idYearAge: id, year cited - year published
    return idYear.join(ddpairs).filter(lambda row: (row[1][0] - row[1][1] >= -2)).map(
            lambda row: ('{}.{}'.format(row[0], row[1][0]), (row[1][0] - row[1][1])))

def citationCountArrays(idYearAge, idYearCount):
    p2Afunc = citationsCommon.pairsToArrayHelper.pairsToArray
    return idYearAge.join(idYearCount).map(
            lambda row: (row[0][:-5], [(row[1][0], row[1][1])])).reduceByKey(
                    lambda c, d: c + d).mapValues(lambda x: p2Afunc(x))

# df is the dataframe read from json before we've filtered out rows where
# references is NULL
# partitionCount says how many partitions to coalesce the intermediate
# data to.
def citationCountsE2E(df, partitionCount=34):
    dd = df.select("id", "references", "year").filter("references is not NULL").rdd
    idYearCount = countByIdAndYear(dd)
    # For publication dates, include publications with no references.
    idYearAge = joinIdYearAge(idYearCount, df.select("id", "year").rdd)
    citCountArrays = citationCountArrays(idYearAge.coalesce(partitionCount),
            idYearCount)
    return citCountArrays


