class BankDetails:
    def __init__(self,cid, cname, bal):
        self.cid = cid
        self.cname = cname
        self.bal = bal


    def  mssg(self):
        print("Welcome to State Bank ")
    def display(self):
        print(f"{self.cid} - {self.cname} - {self.bal}")