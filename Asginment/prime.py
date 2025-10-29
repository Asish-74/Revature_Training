'''
Description: Task 1 :Print all Prime Numbers in an Interval.
Date: 27/10/2025
Author:
Version: 0.1
'''
'''
Description: Task 1 - Print all Prime Numbers in an Interval
Date: 27/10/2025
Author: Asish
Version: 0.1
'''

# Input range
st = int(input("Enter Starting number: "))
end = int(input("Enter Ending number: "))

print(f"\nPrime numbers between {st} and {end} are:")

# Loop through all numbers in the range
for num in range(st, end + 1):
    if num > 1:
        for i in range(2, int(num**2) + 1):
            # we can use this for good performance int(num**2)
            if num % i == 0:
                break
        else:
            print(num)
