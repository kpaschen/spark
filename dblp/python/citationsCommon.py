from decimal import localcontext, Decimal

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


def averageAggregates(cc_arrays, max_years):
    startingTuple = ([0] * max_years, [0] * max_years)
    sumsAndCounts = cc_arrays.aggregate(startingTuple,
            aggregationHelper.seqOp,
            aggregationHelper.combOp)
    with localcontext() as ctx:
        ctx.prec = 3
        return [float((Decimal(x) / Decimal(y)) if y > 0 else 0.0) for x,y in zip(
            sumsAndCounts[0], sumsAndCounts[1])]
