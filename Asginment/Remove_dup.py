"""
:Remove Duplicates from String
"""
str1 = input("Enter String: ")
result=''
for ch  in str1:
    if ch not in result:
        result += ch
str1 = result
del result  # or it will auto delete by GC
print(str1)