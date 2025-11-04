-----	JOIN  ----

select e.ename, d.dname
from emp e join dept d
ON d.deptno in (10,30);

insert into dept values(60, 'QC') 
insert into dept values(70, 'CC') 

insert into emp(empno, ename, mgr, sal, comm, deptno) values 
(1007, 'AAA', 1002, 60000.00, NULL, NULL),
(1008, 'BB', 1001, 75000.00, NULL, NULL),
(1009, 'C', 1002, 50000.00, 500.00, NULL)

select e.ename, d.dname
from emp e inner join dept d
ON d.deptno = e.deptno;   -- INNER JOIN

select e.ename, d.dname
from emp e LEFT OUTER  join dept d
ON d.deptno = e.deptno;

select e.ename, d.dname
from emp e FULL OUTER  join dept d
ON d.deptno = e.deptno;


select * from emp ;

select e.ename as 'Employee', m.ename as 'Manager'
from emp e  join emp m
on e.mgr = m.empno


SELECT e.ename, d.dname
FROM emp e
INNER JOIN dept d
ON e.deptno = d.deptno;


select * 
from emp e cross join dept d 