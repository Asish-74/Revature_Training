"""
Split String into Equal Parts
"""
str1 = input("Enter string Whose length must be a even number: ")
n = 2

length = len(str1)

if length % n != 0:
    print("Invalid Input")
else:
    size = length // n
    for i in range(0, length, size):
        print(str1[i:i+size])
