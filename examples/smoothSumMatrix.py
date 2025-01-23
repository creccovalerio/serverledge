def handler(params, context):
    return smoothSumMatrix(int(params["input"]))

def smoothSumMatrix(n):
    """
    Generate a square matrix of size floor(n/100) x floor(n/100) 
    and compute the sum of the elements on the main diagonal.
    """
    try:
        if n >= 500 and n < 1000:
            reduced_size = n - 400
            matrix = [[i + j for j in range(reduced_size)] for i in range(reduced_size)]
            diagonal_sum = sum(matrix[i][i] for i in range(reduced_size))
            del matrix
            return diagonal_sum
        elif n >= 1000 and n <= 2500:
            reduced_size = n - 1100
            matrix = [[i + j for j in range(reduced_size)] for i in range(reduced_size)]
            diagonal_sum = sum(matrix[i][i] for i in range(reduced_size))
            del matrix
            return diagonal_sum
        elif n >= 2500 and n <= 5000:
            reduced_size = n - 2000
            matrix = [[i + j for j in range(reduced_size)] for i in range(reduced_size)]
            diagonal_sum = sum(matrix[i][i] for i in range(reduced_size))
            del matrix
            return diagonal_sum
        elif n > 5000 and n <= 30000:
            reduced_size = n // 500
            matrix = [[i + j for j in range(reduced_size)] for i in range(reduced_size)]
            diagonal_sum = sum(matrix[i][i] for i in range(reduced_size))
            del matrix
            return diagonal_sum
        elif n > 30000 and n < 120000:
            reduced_size = n // 400
            matrix = [[i + j for j in range(reduced_size)] for i in range(reduced_size)]
            diagonal_sum = sum(matrix[i][i] for i in range(reduced_size))
            del matrix
            return diagonal_sum
        elif n >= 120000 and n < 250000:
            reduced_size = n // 450
            matrix = [[i + j for j in range(reduced_size)] for i in range(reduced_size)]
            diagonal_sum = sum(matrix[i][i] for i in range(reduced_size))
            del matrix
            return diagonal_sum
        elif n >= 250000 and n < 500000:
            reduced_size = n // 650
            matrix = [[i + j for j in range(reduced_size)] for i in range(reduced_size)]
            diagonal_sum = sum(matrix[i][i] for i in range(reduced_size))
            del matrix
            return diagonal_sum
        elif n >= 500000:
            reduced_size = n // 1000
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

