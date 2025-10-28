# ---------------------------------
# Level 3: Derived Class (Child of StudentDetails)
# Adds extra-curricular marks and calculates total including extra
# ---------------------------------

from Opps.StudentDetails import StudentDetails

class StudExCurr(StudentDetails):
    def __init__(self, code, name, rollno, sname, m1, m2, m3, exm1, exm2):
        # Call parent constructor to initialize student and college details
        super().__init__(code, name, rollno, sname, m1, m2, m3)

        # Private variables for extra marks
        self.__exm1 = exm1
        self.__exm2 = exm2

    # -----------------------------
    # Calculate total extra-curricular marks
    # -----------------------------
    def calc_extot(self):
        """
        :Desc : Calculate total of extra marks
        :return: Sum of exm1 and exm2
        """
        return self.__exm1 + self.__exm2

    # -----------------------------
    # Display full details (overrides parent display)
    # -----------------------------
    def display_full_details(self):
        """
        :Desc : Display full details combining college, academic and extra marks
        :return: Formatted string of complete details
        """
        college_info = self.display_college()   # from College class
        student_info = self.display_student()   # from StudentDetails class
        extra_info = f"Extra Marks Total: {self.calc_extot()}"  # from this class
        return f"{college_info}\n{student_info}\n{extra_info}"
