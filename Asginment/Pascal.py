# Pascal's Triangle

def pascal_triangle(n):
    pascal = [[0 for _ in range(n)] for _ in range(n)]

    for i in range(n):
        for j in range(i + 1):
            if j == 0 or j == i:
                pascal[i][j] = 1
            else:
                pascal[i][j] = pascal[i - 1][j - 1] + pascal[i - 1][j]

    # print the triangle
    for i in range(n):
        print(" " * (n - i), end="")
        for j in range(i + 1):
            print(pascal[i][j], end=" ")
        print()

n = int(input("Enter number of rows: "))
pascal_triangle(n)
