# ---------------------------------
# Level 1: Base Class (Parent Class)
# Represents the college information shared by all students
# ---------------------------------

class College:
    def __init__(self, code, name):
        # Protected variables (accessible in subclasses)
        self._code = code       # _ single underscore means "Protected"
        self._name = name       # accessible within class & subclasses

    # Display college info
    def display_college(self):
        """
        :Desc : Display the college details
        :return: College code and name as formatted string
        """
        return f"College Code: {self._code}, College Name: {self._name}"
