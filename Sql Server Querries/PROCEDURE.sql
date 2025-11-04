--PORCEDURE----
--A Stored Procedure in SQL is a set of SQL statements (like SELECT, INSERT, UPDATE, DELETE, etc.) 
--that are saved and stored in the database with a name, so you can reuse it whenever needed.
--A procedure is like a function in programming — you write it once and run it many times.

-- 1. Show all employees

CREATE PROCEDURE showAllEmp
AS 
BEGIN 
	SELECT * FROM EMP;
END;

EXEC showAllEmp

--2. Show employees by department number (parameter example)

CREATE PROCEDURE GetEmpByDept
    @DeptNo INT
AS
BEGIN
    SELECT empno, ename, sal, deptno
    FROM emp
    WHERE deptno = @DeptNo;
END;

EXEC GetEmpByDept @DeptNo=30;

--3. Show employees with manager names (JOIN + Procedure)
-- CREATE(CREATE A NEW PROCEDURE ) OR ALTER (FOR UPDATING)
CREATE or ALTER PROCEDURE EmpWithMgr
AS
BEGIN
    SELECT 
        e.empno,
        e.ename AS Employee,
        m.ename AS Manager
    FROM emp e
    LEFT JOIN emp m
    ON e.mgr = m.empno;
END;

EXEC EmpWithMgr

-- 4 Show employee name with department name (JOIN with dept)

CREATE or ALTER PROCEDURE EmpWithDept
AS
BEGIN
    SELECT 
        e.empno,
        e.ename,
        d.dname AS Department,
        e.sal
    FROM emp e
    LEFT JOIN dept d
    ON e.deptno = d.deptno;
END;
EXEC EmpWithDept;

-- 5 .Show employees who earn commission
CREATE OR ALTER PROCEDURE EmpWithCommission
AS
BEGIN
    SELECT empno, ename, sal, comm
    FROM emp
    WHERE comm IS NOT NULL;
END;

EXEC EmpWithCommission;
