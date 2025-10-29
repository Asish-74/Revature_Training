"""
Task 2 Check Whether a String is Palindrome or Not.
"""
str= input("Enter a String: ")
# i=0
# j=len(str)-1
# flag=True
# while i<=j:
#     if str[i]!=str[j]:
#         flag= False
#     i+=1
#     j-=1
#
# if flag:
#     print("Palindrome")
# else:
#     print("Not Palindrome")

rev= str[::-1]
if rev == str: print("Palindrome")
else:
    print("Not Palindrome")