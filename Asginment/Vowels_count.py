"""
Task 3 Count the Number of Vowels in a string.
"""

str= input("Enter a string: ")
count= 0
for i in range(len(str)):
    if str[i]=='a' or str[i]=='e' or str[i]=='i' or str[i]=='o' or str[i]=='u' or str[i]=='A' or str[i]=='E':
        count +=1
print(count)