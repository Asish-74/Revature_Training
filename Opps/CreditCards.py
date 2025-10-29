from typing import final

from Opps.BankDetails import BankDetails


class CreditCards(BankDetails):
    def __init__(self,cid, cname, bal, creditscore,status):
        super().__init__(cid, cname, bal)
        self.creditscore = creditscore
        self.status=status
    def mssg(self):    # now override the method
        print(f"wellcome to SBI,{self.cname} ")
    def display_cc(self):
        print(f"{self.creditscore} - {self.status} ")