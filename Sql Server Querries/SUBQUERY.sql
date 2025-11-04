-- SUBQUERY------
use RevCompanyDb

select * from emp 
select * from dept 

select ename
from emp
where deptno in (select deptno from dept
where dname='sal')

--find avg salary 
select ename, sal from emp
where sal> (select avg(sal) from emp );


--dept wise salary 
select deptno, avg(sal)  as 'avgsal'
from emp
Group by deptno ;

SELECT *
FROM EMP
WHERE SAL> (SELECT SAL
             FROM EMP
             WHERE ENAME='CHARLIE');

