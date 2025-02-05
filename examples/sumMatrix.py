def handler(params, context):
    return sumMatrix(int(params["input"]))

def sumMatrix(n):
    """
    Generate a square matrix of size n x n and compute the sum of the elements on the main diagonal
    """
    try:
        matrix = [[i + j for j in range(n)] for i in range(n)]
        diagonal_sum = sum(matrix[i][i] for i in range(n))
        del matrix  # memory release
        return diagonal_sum
    except MemoryError:
        return -1
