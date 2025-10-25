# 1**2 + 2**2 + 3**2 + ............. n
terms =int(input("Enter a number"))
sum=0
for i in range(1,terms+1):
    sum+=i*i
print(sum)
