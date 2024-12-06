def handler(params, context):
    n = params["input"]
    return fibonacci_iterative(int(n))


def fibonacci_iterative(n):
    """
    Compute the fibonacci number of n
    :param n: a positive integer.
    """
    if n <= 1:
        return n
    a, b = 0, 1
    for _ in range(2, n + 1):
        a, b = b, a + b
    return b
