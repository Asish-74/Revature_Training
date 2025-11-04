--view 
-- it likes a xerox copy of a original Table
-- we can perform  create , drop , insert, update ,delete , select 
--if we trying to isnert and delte from view table then it not going to delte any thing and when i inset value it inserted into original table 

use RevCompanyDb

select * from emp

CREATE VIEW vemp AS SELECT empno, ename FROM emp where deptno in (10 , 20 , 30);
select * from vemp
insert into emp (empno, ename, deptno) values (1110, 'XYZ', 20)
insert into vemp values (1111,'YYYY')

drop view vemp 
