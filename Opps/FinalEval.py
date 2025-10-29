from Opps.StudExCurr import StudExCurr
from Opps.TeacherDetail import TeacherDetail


class FinalEval(StudExCurr,TeacherDetail):
    def __init__(self, code, name, rollno, sname, m1, m2, m3, exm1, exm2, empid, tname, dept,feedbackfromteacher):
        super().__init__(self, code, name, rollno, sname, m1, m2, m3, exm1, exm2)
        super().__init__(code, name,empid, tname, dept)
        self.feedbackfromteacher= feedbackfromteacher

