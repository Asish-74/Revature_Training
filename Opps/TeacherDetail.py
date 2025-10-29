# ---------------------------------
# Level 2: Derived Class (Child Class)
# Represents teacher details (inherits from College)
# ---------------------------------
from Opps.College import College


class TeacherDetail(College):
    def __init__(self, code, cname, empid=None, tname=None, dept=None):
        # Call the parent class constructor
        super().__init__(code, cname)
        self.empid = empid
        self.tname = tname
        self.dept = dept

    def display(self):
        print(f"College Code: {self._code}, College Name: {self._cname}")
        print(f"Employee ID: {self.empid}, Teacher Name: {self.tname}, Department: {self.dept}")
teach = TeacherDetail(54, "BPUT", 101, "Rahul", "CSE")
teach.display()