CREATE DATABASE RevCompanyDb
use RevCompanyDb

--create tables 

--creating department table
CREATE TABLE dept(deptno smallint, dname varchar (3) not null , 
constraint pk_deptno primary key (deptno)) ;

--creating employee table 
create table emp(empno smallint, ename varchar(30) not null,
mgr smallint,sal numeric(10,2),
comm numeric(7,2),deptno smallint,
constraint pk_empno primary key(empno),
constraint fk_deptno foreign key(deptno) references dept(deptno));

insert into dept values(10, 'IT')
insert into dept values(20, 'HR')
insert into dept values(30, 'SAL')
insert into dept values(40, 'MKT')
insert into dept values(50, 'OPS')
SELECT * FROM dept

INSERT INTO emp (empno, ename, mgr, sal, comm, deptno) VALUES
(1001, 'Alice', NULL, 60000.00, NULL, 10),  -- HR
(1002, 'Bob', 1001, 75000.00, NULL, 20),    -- IT
(1003, 'Charlie', 1002, 50000.00, 500.00, 30), -- Sales
(1004, 'Diana', 1003, 52000.00, 300.00, 30),   -- Sales
(1005, 'Ethan', 1002, 58000.00, NULL, 40),  -- Finance
(1006, 'Fiona', 1005, 62000.00, NULL, 50);  -- Marketing

SELECT * FROM emp



select empno AS "Emp Num" , ename as "Name"
from emp;

select empno AS "Emp Num" , ename as "Name"
from emp
where sal>=60000;

select empno AS "Emp Num" , ename as "Name"
from emp
where empno != 1004;

select empno AS "Emp Num" , ename as "Name"
from emp
where empno in(1002,1003,1004);

select empno AS "Emp Num" , ename as "Name"
from emp
where empno not in(1002,1003,1004);

select empno AS "Emp Num" , ename as "Name"
from emp
where sal between 40000 and 550000;


select empno AS "Emp Num" , ename as "Name"
from emp
where ename = 'bob'    -- case senstive is not mandotary

select empno AS "Emp Num" , ename as "Name"
from emp
where ename LIKE '%e'  --ends with e

select empno AS "Emp Num" , ename as "Name"
from emp
where ename LIKE 'd_a%' 

select empno AS "Emp Num" , ename as "Name"
from emp
where ename LIKE '__a%'  -- first char and second char anything but thrid ch must be a 

select empno AS "Emp Num" , ename as "Name"
from emp
where ename not in ('alice', 'fiona'); 

select empno AS "Emp Num" , ename as "Name"
from emp
where empno =1004 and ename like '%a%' ;    --   Diana

select empno AS "Emp Num" , ename as "Name"
from emp
where empno =1004 or ename like '%a%';




--	ORDER BY --

select empno AS "Emp Num" , ename as "Name", sal as "salary", comm as "commision"
from emp
where sal>50000
order by sal, comm  desc;   -- it will showing all the salary in sorting order (we can set asc or desc , by default asc)


-- Agrrigate functions
-- count(), avg(), sum() ,  

select count(empno) as "No. of Emp" , sum(sal) as "Total sal" , avg(comm) as "Avg Commision", min(sal) as "Least Salary" ,
max(sal) as "Top Earner"
from emp
where sal>55000



--Group By---

SELECT * from emp

-- Query 1: Filter first, then group
SELECT empno AS "Emp Num", ename AS "Name", sal AS "Salary"
FROM emp                 -- Step 1: Take data from the emp table
WHERE sal > 50000        -- Step 2: Filter rows where salary is greater than 50,000
GROUP BY deptno, empno, ename, sal   -- Step 3: Group the filtered data (though grouping not really needed here since no aggregate function is used)
-- Step 4: SELECT displays the final result columns
-- Execution order:
-- FROM -> WHERE -> GROUP BY -> SELECT


-- Query 2: Group first, then filter groups using HAVING
SELECT deptno AS 'Dept Number', SUM(sal) AS 'Total Salary'
FROM emp                   -- Step 1: Take data from emp table
GROUP BY deptno             -- Step 2: Group data by department number
HAVING deptno IN (10, 30, 50)  -- Step 3: Filter only those grouped departments (10, 30, 50)
-- Step 4: SELECT displays each dept’s total salary
--  Execution order:
-- FROM -> GROUP BY -> HAVING -> SELECT



-- Query 3: Combine WHERE and HAVING for different filtering levels
SELECT deptno AS 'Dept Number', SUM(sal) AS 'Total Salary'
FROM emp
WHERE deptno IN (10, 30, 50)   -- Step 1: Filter rows (only these departments) before grouping
GROUP BY deptno                -- Step 2: Group filtered data by department number
HAVING SUM(sal) >= 62000       -- Step 3: After grouping, filter departments whose total salary ≥ 62000
ORDER BY SUM(sal);             -- Step 4: Sort the final result by total salary (ascending by default)
--  Execution order:
-- FROM -> WHERE -> GROUP BY ->  HAVING -> SELECT -> ORDER BY
-- WHERE filters rows before grouping, HAVING filters grouped results after aggregation
