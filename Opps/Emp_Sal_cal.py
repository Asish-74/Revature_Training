from Opps.Employee import Employee

empId= int (input("Enter Employee Id : "))
ename=input("Enter Employee Name : ")
bp=float(input("Enter Basic Pay : "))
employee=Employee(empId=empId, eName=ename, bp=bp)   # object creation
print(f"Emp Id: {empId} \n Name: {employee.ename} \n Gross pay : {employee.calc_grosspay(employee.bp)} \n Net pay : {employee.calc_netpay(employee.bp)}")
