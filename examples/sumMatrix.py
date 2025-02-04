def handler(params, context):
    return sumMatrix(int(params["input"]))

def sumMatrix(n):
    """
    Generate a square matrix of size n x n and compute the sum of the elements on the main diagonal
    """
    try:
        if n < 10:
            extended_size = n * 50
            matrix = [[i + j for j in range(extended_size)] for i in range(extended_size)]
            diagonal_sum = sum(matrix[i][i] for i in range(extended_size))
            del matrix  # memory release
            return diagonal_sum // 90
        elif n > 500 and n <= 1000:
            reduced_size = n - 300
            matrix = [[i + j for j in range(reduced_size)] for i in range(reduced_size)]
            diagonal_sum = sum(matrix[i][i] for i in range(reduced_size))
            del matrix  # memory release
            return diagonal_sum // 100
        elif n > 1000 and n < 1500:
            reduced_size = n - 900
            matrix = [[i + j for j in range(reduced_size)] for i in range(reduced_size)]
            diagonal_sum = sum(matrix[i][i] for i in range(reduced_size))
            del matrix  # memory release
            return diagonal_sum // 115
        elif n > 1500:
            reduced_size = n - 1200
            matrix = [[i + j for j in range(reduced_size)] for i in range(reduced_size)]
            diagonal_sum = sum(matrix[i][i] for i in range(reduced_size))
            del matrix  # memory release
            return diagonal_sum // 120
    except MemoryError:
        return -1
