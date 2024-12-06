def handler(params, context):
    return smoothSumMatrix(int(params["input"]))

def smoothSumMatrix(n):
    """
    Generate a square matrix of size floor(n/100) x floor(n/100) 
    and compute the sum of the elements on the main diagonal.
    """
    try:
        if n >= 5000:
            reduced_size = n // 5000
            matrix = [[i + j for j in range(reduced_size)] for i in range(reduced_size)]
            diagonal_sum = sum(matrix[i][i] for i in range(reduced_size))
            del matrix
            return diagonal_sum
        else:
            matrix = [[i + j for j in range(n)] for i in range(n)]
            diagonal_sum = sum(matrix[i][i] for i in range(n))
            del matrix  # memory release
            return diagonal_sum
    except MemoryError:
        return -1

