from Opps.School import School

sname = input("Enter the Name: ")
address = input("Enter the address: ")

schl = School(sname, address)
print(f"School name is {schl.sname} and Address is {schl.address}")