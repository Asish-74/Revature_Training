# Python 3.14.0 (tags/v3.14.0:ebf955d, Oct  7 2025, 10:15:03) [MSC v.1944 64 bit (AMD64)] on win32
# Enter "help" below or click "Help" above for more information.
# 5>6 and 3<7
# False
# not 6
# False
# not -6
# False
# 7&9
# 1
# 7|9
# 15
# 7^9
# 14
# 5<<3
# 40
# 5<<4
# 80
# 5>>2
# 1
# 5>>3
# 0
# 5>>>2
# SyntaxError: invalid syntax
# num1=10
# type(num1)
# <class 'int'>
# num1 is int
# False
# 5 is int
# False
# type(5) is  int
# True
# type(num1) is int
# True
# print('HI')
# HI
# type(num1) is not int
# False
# print('Hi'+'Hello')
# HiHello
# print(2+3)
# 5
# print
# <built-in function print>
#
#
# 9
# print('hi'+10)
# Traceback (most recent call last):
#   File "<pyshell#24>", line 1, in <module>
#     print('hi'+10)
# TypeError: can only concatenate str (not "int") to str
# print('Hi'+'10')
# Hi10
# print('Sum:  ',num1+10)
# Sum:   20
# print('Sum:  '+'\n',num1+8)
# Sum:
#  18
# input()
# 5
# '5'
# inpu()
# Traceback (most recent call last):
#   File "<pyshell#29>", line 1, in <module>
#     inpu()
# NameError: name 'inpu' is not defined. Did you mean: 'input'?
# input()
# hi
# 'hi'
# input()
# True
# 'True'
# input(Enter a number:)
# SyntaxError: invalid syntax. Perhaps you forgot a comma?
# input('Enter a number: ')
# Enter a number: 5
# '5'
# int(input("Enter a number: "))
# Enter a number: 5
# 5
# float(input(5))
# 5
# Traceback (most recent call last):
#   File "<pyshell#35>", line 1, in <module>
#     float(input(5))
# ValueError: could not convert string to float: ''
# flloat(input("Enter a number: "))
# Traceback (most recent call last):
#   File "<pyshell#36>", line 1, in <module>
#     flloat(input("Enter a number: "))
# NameError: name 'flloat' is not defined. Did you mean: 'float'?
# float(input("Enter a number: "))
# Enter a number: 5
# 5.0
# bool(input("Enter a number: "))
# Enter a number: 9
# True
# bool(input("Enter a number: "))
# Enter a number:
# False
# int(input("Enter a number: "))
# Enter a number:
# Traceback (most recent call last):
#   File "<pyshell#40>", line 1, in <module>
#     int(input("Enter a number: "))
# ValueError: invalid literal for int() with base 10: ''
# 'Ram's house'
# SyntaxError: unterminated string literal (detected at line 1)
# "Ram's House"
# "Ram's House"
# str1="Hello !"
# str1
# 'Hello !'
# len(str1)
# 7
# str1[0]
# 'H'
# str1[5]
# ' '
# str1[-2]
# ' '
# str1[-1]
# '!'
# str[0:2]
# Traceback (most recent call last):
#   File "<pyshell#50>", line 1, in <module>
#     str[0:2]
# TypeError: type 'str' is not subscriptable
# str[0:2]
# Traceback (most recent call last):
#   File "<pyshell#51>", line 1, in <module>
#     str[0:2]
# TypeError: type 'str' is not subscriptable
# str1[0:2]
# 'He'
# str1[3:6]
# 'lo '
# str1[0:7:2]
# 'Hlo!'
# str1[0::2]
# 'Hlo!'
# str1[-3:-5]
# ''
# str[-5:-3]
# Traceback (most recent call last):
#   File "<pyshell#57>", line 1, in <module>
#     str[-5:-3]
# TypeError: type 'str' is not subscriptable
# str1[-5:-3]
# 'll'
# str1[-6:-1:2]
# 'el '
# str1.capitalize()
# 'Hello !'
# str1.lower()
# 'hello !'
# str1.upper()
# 'HELLO !'
# str1.count()
# Traceback (most recent call last):
#   File "<pyshell#63>", line 1, in <module>
#     str1.count()
# TypeError: count expected at least 1 argument, got 0
# str1.count('l')
# 2
# str1.endswith('l')
# False
# str1.endswith('!')
# True
# str1.find('!')
# 6
# str1.swapcase()
# 'hELLO !'
# str.islower(0)
# Traceback (most recent call last):
#   File "<pyshell#69>", line 1, in <module>
#     str.islower(0)
# TypeError: descriptor 'islower' for 'str' objects doesn't apply to a 'int' object
# str1.find('t')
# -1
# str1.index('l')
# 2
# str1.index('t')
# Traceback (most recent call last):
#   File "<pyshell#72>", line 1, in <module>
#     str1.index('t')
# ValueError: substring not found
# str1.center()
# Traceback (most recent call last):
#   File "<pyshell#73>", line 1, in <module>
#     str1.center()
# TypeError: center expected at least 1 argument, got 0
# str1.encode('utf-8')
# b'Hello !'
# str1.encode('utf-16')
# b'\xff\xfeH\x00e\x00l\x00l\x00o\x00 \x00!\x00'
# str1.islower()
# False
# str1.isupper()
# False
# str1.isnumeric()
# False
# str1.isalpha()
# False
# str1.replace('o','0')
# 'Hell0 !'
# str1.split()
# ['Hello', '!']
# str2='1-05-2025'
# str2.split('-')
# ['1', '05', '2025']
# str1.split('-')
# ['Hello !']
# str1.join(str2)
# '1Hello !-Hello !0Hello !5Hello !-Hello !2Hello !0Hello !2Hello !5'
# str1.strip()
# 'Hello !'
# str1.strip('o')
# 'Hello !'
# str1.strip('!')
# 'Hello '
# numbers=[10,20,30]
# id(numbers)
# 2438503861184
# numbers.append(40)
# numbers
# [10, 20, 30, 40]
# id(numbers)
# 2438503861184
# numbers[2]
# 30
# numbers[3]=50
# numbers
# [10, 20, 30, 50]
# numbers.append(50)
# numbers.count(50)
# 2
# numbers.index(50)
# 3
# numbers.index(40)
# Traceback (most recent call last):
#   File "<pyshell#100>", line 1, in <module>
#     numbers.index(40)
# ValueError: list.index(x): x not in list
# numbers.insert(4,60)
# numbers
# [10, 20, 30, 50, 60, 50]
# numbers.insert(3,40)
# numbers
# [10, 20, 30, 40, 50, 60, 50]
# numbers.remove(60)
# numbers
# [10, 20, 30, 40, 50, 50]
# numbers.remove(50)
# numbers
# [10, 20, 30, 40, 50]
# numbers.pop()
# 50
# numbers
# [10, 20, 30, 40]
# numbers.pop(-2)
# 30
# numbers.pop(0)
# 10
# numbers
# [20, 40]
# numbers.reverse()
# numbers
# [40, 20]
# numbers.sort()
# numbers
# [20, 40]
# numbers.sort(reverse=True)
# numbers
# [40, 20]
# num=numbers.copy()
# num
# [40, 20]
# id(num)
# 2438503864512
# id(numbers)
# 2438503861184
# num1=numbers
# id(num1)
# 2438503861184
# numbers.extend(num)
# numbers
# [40, 20, 40, 20]
# numbers.clear()
# numbers
# []
# del(numbers)
# numbers
# Traceback (most recent call last):
#   File "<pyshell#131>", line 1, in <module>
#     numbers
# NameError: name 'numbers' is not defined. Did you forget to import 'numbers'?
# tup1=(10,20,30)
# tup1[5]
# Traceback (most recent call last):
#   File "<pyshell#133>", line 1, in <module>
#     tup1[5]
# IndexError: tuple index out of range
# tup1[2]
# 30
# tup1.count(10)
# 1
# tup1.index(30)
# 2
# tup1.clear()
# Traceback (most recent call last):
#   File "<pyshell#137>", line 1, in <module>
#     tup1.clear()
# AttributeError: 'tuple' object has no attribute 'clear'
# del(tup1)
# tup1
# Traceback (most recent call last):
#   File "<pyshell#139>", line 1, in <module>
#     tup1
# NameError: name 'tup1' is not defined. Did you mean: 'tuple'?
# set1={11,10,33,40,40,50,60}
# set1
# {33, 50, 40, 10, 11, 60}
# set1[2]
# Traceback (most recent call last):
#   File "<pyshell#142>", line 1, in <module>
#     set1[2]
# TypeError: 'set' object is not subscriptable
# set1.add(53)
# set1
# {33, 50, 53, 40, 10, 11, 60}
# set2={10,20,30}
# set1.u(set2)
# Traceback (most recent call last):
#   File "<pyshell#146>", line 1, in <module>
#     set1.u(set2)
# AttributeError: 'set' object has no attribute 'u'
# set1.union(set2)
# {33, 40, 10, 11, 50, 20, 53, 60, 30}
# set3=set1.union(set2)
# set3
# {33, 40, 10, 11, 50, 20, 53, 60, 30}
# set4=set3.intersection(set2)
# set4
# {10, 20, 30}
# set5=set1.difference(set4)
# set5
# {33, 40, 11, 50, 53, 60}
# set1.discard(60)
# set1
# {33, 50, 53, 40, 10, 11}
# dict1={1:10,2:20,3:10}
# dict1
# {1: 10, 2: 20, 3: 10}
# dict1[2]
# 20
# dict1[3]=30
# dict1
# {1: 10, 2: 20, 3: 30}
# dict2={'rno':'123','name':'AA'}
# dict2
# {'rno': '123', 'name': 'AA'}
# >>> dict2['rno']
# '123'
# >>> dict2[ss]
# Traceback (most recent call last):
#   File "<pyshell#164>", line 1, in <module>
#     dict2[ss]
# NameError: name 'ss' is not defined
# >>> dict2['rnn']
# Traceback (most recent call last):
#   File "<pyshell#165>", line 1, in <module>
#     dict2['rnn']
# KeyError: 'rnn'
# >>> dict2['ph']=658264562
# >>> dict2
# {'rno': '123', 'name': 'AA', 'ph': 658264562}
# >>> dict2['ph']
# 658264562
# >>> dict1.get('1')
# >>> vv=dict1.get(1)
# >>> vv
# 10
# >>> dict2.keys()
# dict_keys(['rno', 'name', 'ph'])
# >>> dict2.values()
# dict_values(['123', 'AA', 658264562])
# >>> dict2.pop()
# Traceback (most recent call last):
#   File "<pyshell#174>", line 1, in <module>
#     dict2.pop()
# TypeError: pop expected at least 1 argument, got 0
# >>> dict2.pop('ph')
# 658264562
# >>> dict2
# {'rno': '123', 'name': 'AA'}
# >>> dict1.update(1,40)
# Traceback (most recent call last):
#   File "<pyshell#177>", line 1, in <module>
#     dict1.update(1,40)
# TypeError: update expected at most 1 argument, got 2
