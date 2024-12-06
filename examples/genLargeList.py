def handler(params, context):
    return generateLargeList(int(params["input"]))

def generateLargeList(n):
    """
    Generate a list of size n *10^6 and compute the sum of the elements
    """
    try:
        large_list = [i for i in range(n * 10**6)]
        result = sum(large_list)
        del large_list  # memory release
        return result % 10**9
    except MemoryError:
        return -1
