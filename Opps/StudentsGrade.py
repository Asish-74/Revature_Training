# ---------------------------------
# Main file to create objects and demonstrate Multilevel Inheritance
# ---------------------------------

from Opps.StudExCurr import StudExCurr

# Taking inputs from user
ccode = int(input("Enter college code: "))
cname = input("Enter college name: ")
rollno = int(input("Enter roll number: "))
sname = input("Enter student name: ")
m1 = int(input("Enter mark 1: "))
m2 = int(input("Enter mark 2: "))
m3 = int(input("Enter mark 3: "))
exm1 = int(input("Enter extra mark 1: "))
exm2 = int(input("Enter extra mark 2: "))

# Create object for the final derived class (lowest level)
student = StudExCurr(ccode, cname, rollno, sname, m1, m2, m3, exm1, exm2)

# Display all details
print("\n🎓 STUDENT COMPLETE DETAILS 🎓")
print(student.display_full_details())
