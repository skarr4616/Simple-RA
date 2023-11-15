import numpy as np

np.random.seed(69)
arr = np.arange(1, 101)
np.random.shuffle(arr)

for num in arr:
    print("INSERT", num)

for i in range(1, 100):
    print("RANGE", i, i+1)

print("QUIT")