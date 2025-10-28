'''
Description: Task 1 :Print all Prime Numbers in an Interval.
Date: 27/10/2025
Author:
Version: 0.1
'''
st= int(input("Enter Starting number: "))
end= int(input("Enter Ending number: "))
def prime(n):
    flag= True
    for i in range(1, n//2+1):
      if n%2==0:
         flag=False
    return True

if prime(st):
    print("it is a prime number")
else :
    print("it is not a prime number")