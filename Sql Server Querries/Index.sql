--Index ---
-- An index in SQL is like an index in a book
--it helps you find data faster without scanning the entire table.

use RevCompanyDb

create nonclustered index ideptno on emp (deptno)
drop index emp.ideptno 

select * from emp
select* from dept