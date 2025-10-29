n = int(input("Enter number of rows: "))
star = 1
sp = n - 1

for i in range(n):
    for j in range(sp):
        print(" ", end="")
    for k in range(star):
        print("*", end="")
    print()
    star += 2
    sp -= 1
