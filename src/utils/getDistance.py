import math

# 计算余弦距离
def cosDistance(x1, x2):
    count1 = []
    count2 = []
    for x in x1:
        count1.append(x)
    for x in x2:
        count2.append(x)
    count = len(count1) * len(count2) * 1.0
    a = set(count1)
    b = set(count2)
    c = a & b
    commonLength = len(c)
    w = commonLength / math.sqrt(count)
    return w