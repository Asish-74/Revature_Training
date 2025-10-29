class School:
    def __init__(self, sname, address):
        self.sname = sname
        self.address = address

    def show_school_info(self):
        return self.sname, self.address
