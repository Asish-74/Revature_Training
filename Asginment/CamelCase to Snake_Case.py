"""
Convert CamelCase to snake_case
"""
str1 = input("Enter the CamelCase String: ")

result = ""
for ch in str1:
    if ch.isupper():                 # if character is uppercase
        result += "_" + ch.lower()   # add '_' + lowercase version
    else:
        result += ch

# remove leading underscore if exists
if result[0] == "_":
    result = result[1:]

print("snake_case:", result)
