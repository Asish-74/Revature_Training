# str= "hello\0"
str= "hello"
s= iter(str)
print(next(s))
print(next(s))
print(next(s))
print(next(s))
print(next(s))

# it will itterate the null value also