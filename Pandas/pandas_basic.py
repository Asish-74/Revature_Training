import  pandas as pd
"""
"""
# creating a series
data =[1,2,3,4,5]
print (type(data))
series=pd.Series(data)  # it print like index(we can say row) with data
print (series)
print(type(series))
print("======================================")

# creating a dataframe
data ={
    "Name": ["Asish", "Deepak", "Ramana"],
    "Age": [22, 21, 33],
    "city": ["Chennai", 'Banglore', "Chennai"]
}
df=pd.DataFrame(data)  # read the data
print (df)
print("======================================")
print('head \n', df.head()) # we can read from first if i give any number in bracket then it print that much data
print('tail-2 \n', df.tail(2)) #last rows
print("======================================")
print('information:')
df.info()
print('desc:\n', df.describe())
print("======================================")
# Selecting coulmns
print(df['Name'])
print(df[['Name' , 'city']])
print("======================================")

print(df[df['Age'] >30])