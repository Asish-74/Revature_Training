string_Num= input("Enter the Number")
numLength= len(string_Num)
num=int(string_Num)
temp=num
sum=0
while num>0:
    remainder=num%10
    sum+=remainder**numLength
    num //=10

if temp==sum:
    print("It is a Armstrong Number")
else:
    print("Not a Armstrong Number")