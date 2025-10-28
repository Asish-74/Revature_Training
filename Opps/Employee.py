class Employee:

    # default constructor
    # def __init__(self):
    #     self.empId=0
    #     self.name=""
    #     self.bp=0

    # Parameterized constructor
    def __init__(self, empId, eName,bp):
        self.empId=empId
        self.ename=eName
        self.bp=bp
    def calc_allowance(self, bp):
        # allowance = bp * 0.1
        # hra= bp *0.05
        return (bp * 0.1) + (bp *0.05)
    def cal_ded(self, bp):
        return bp * 0.03
    def calc_grosspay(self, bp):
        return bp +self.calc_allowance(bp)  # self as like as this key word
    def calc_netpay(self, bp):
        return self.calc_grosspay(bp) - self.cal_ded(bp)