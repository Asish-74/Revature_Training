# ---------------------------------
# Level 2: Derived Class (Child of College)
# Represents the student details including marks
# ---------------------------------

from Opps.College import College  # Import the parent class

class StudentDetails(College):  # Inherits from College
    def __init__(self, code, name, rollno, sname, m1, m2, m3):
        # Call the parent constructor using super()
        super().__init__(code, name)

        # Private variables (accessible only within this class)
        self.__rollno = rollno   # student roll number
        self.__sname = sname     # student name
        self.__m1 = m1           # subject mark 1
        self.__m2 = m2           # subject mark 2
        self.__m3 = m3           # subject mark 3

    # -----------------------------
    # Getter and Setter for roll number
    # -----------------------------
    def get_rollno(self):
        """Return the student roll number"""
        return self.__rollno

    def set_rollno(self, rollno):
        """Set a new roll number"""
        self.__rollno = rollno

    # -----------------------------
    # Getter and Setter for student name using @property
    # -----------------------------
    @property
    def sname(self):
        """Getter method for student name"""
        return self.__sname

    @sname.setter
    def sname(self, sname):
        """Setter method for student name"""
        self.__sname = sname

    # -----------------------------
    # Calculate total marks
    # -----------------------------
    def calc_total(self):
        """
        :Desc : Calculate the total of 3 subject marks
        :return: Total marks
        """
        return self.__m1 + self.__m2 + self.__m3

    # -----------------------------
    # Calculate average marks
    # -----------------------------
    def calc_avg(self):
        """
        :Desc : Calculate average marks
        :return: Average marks
        """
        return (self.__m1 + self.__m2 + self.__m3) / 3

    # -----------------------------
    # Display student details
    # -----------------------------
    def display_student(self):
        """
        :Desc : Display student's personal and academic details
        :return: A formatted string of student details
        """
        return (f"Roll No: {self.__rollno}, Name: {self.__sname}, "
                f"Total Marks: {self.calc_total()}, Average: {self.calc_avg():.2f}")
