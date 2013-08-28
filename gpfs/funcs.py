import math

def count_iterations(alist):
    """
    Counts the number of times value 'x' occurs in a list
    """
    x = {}
    for i in alist:
        if i in x:
            x[i] += 1
        else:
            x[i] = 1

    return x

def stddev(data):
    """Returns the standard deviation of a list and the variance"""

    try:
        length = len(data)
        mean = float(sum(data)) / length
    except TypeError as te:
        # this only affects a few things right now
        print "Type error when try to compute the std deviation: {0}".format(te)
        #print "Most likely the list contains strings..."
        return

    stddev = math.sqrt((1. / length) * sum([(x - mean) ** 2 for x in data]))
    variance = stddev ** 2
    return stddev, variance

def zscore(raw, mean, stddev):
    """Returns the zscore (standard score) of a value"""

    zscore = (raw - mean) / stddev

    return zscore
