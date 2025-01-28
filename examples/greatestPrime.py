def handler(params, context):
    return greaterPrime(int(params["input"]))

def greaterPrime(n):
    """
    Compute prime numbers up to n.
    Use `n` as inut to return the greatest prime numeber found.
    """
    def is_prime(num):
        if num < 2:
            return False
        for i in range(2, int(num**0.5) + 1):
            if num % i == 0:
                return False
        return True

    primes = [i for i in range(2, n*10) if is_prime(i)]
    return primes[-1]//10 if primes else -1 