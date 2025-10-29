# ---------------------------------
# Level 1: Base Class (Parent Class)
# Represents the college information shared by all teachers
# ---------------------------------

class College:
    def __init__(self, code, cname):
        # Protected variables (accessible in subclasses)
        self._code = code       # _ single underscore means "Protected"
        self._cname = cname     # accessible within class & subclasses

    # Property for code
    @property
    def code(self):
        return self._code

    # Property for cname
    @property
    def cname(self):
        return self._cname

    # Display college info
    def display_college(self):
        """
        :Desc : Display the college details
        :return: College code and name as formatted string
        """
        return f"College Code: {self._code}, College Name: {self._cname}"