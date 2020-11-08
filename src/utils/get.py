def get(x, k):
    x.sort(key = lambda x:x[1],reverse=True)
    return x[:k]