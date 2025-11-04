use revcomdb

select*from employee;

select e.ename as 'Employee',m.ename as 'Manager'
from employee e join employee m
on e.mgr=m.empno;

select * from employee cross join department;

select e.ename as 'Employee',d.deptno as 'Department no'
from employee e cross join department d;

select*from department;
--Sub Queries

select ename from employee
where deptno=(select deptno from department
where dname='SAL');

select ename from employee
where deptno=(select deptno from department
where dname='qc');

select ename from employee where sal>(select avg(sal) from employee);
--select deptno from department (select deptno,avg(sal) as Avg_sal from employee group by deptno)

select ename,Deptno,sal from employee e
where sal >= (select avg(Sal) from employee where deptno=e.Deptno);


create view vemp as select empno,ename from employee where deptno in (10,11,14);

select * from vemp;

insert into employee(empno,ename,deptno) values (1110,'XXX',14);
insert into vemp values(1111,'XYZ');

update vemp set empno=1012 where empno=1110;
delete from vemp where empno=1111;
drop view vemp;

create nonclustered index idepno on employee(deptno)
drop index employee.idepno;



---Stored Procedures

create procedure sp_empdata
as 
begin 
  select*from employee
end;

exec sp_empdata


create or alter procedure sp_empdata
     @empno int,@ename varchar(20),@deptno int
as 
begin 
    begin transaction
    insert into employee(empno,ename,deptno) values (@empno,@ename,@deptno)
    commit
    select * from employee
end;

exec sp_empdata 1015,'Laxman',12;

create or alter procedure sp_empdata1
     @empno int,@ename varchar(20),@deptno int,@sal numeric(10,2)
as 
begin 
    begin transaction
    insert into employee(empno,ename,sal,deptno) values (@empno,@ename,@sal,@deptno)
    update employee set comm=sal*0.1 where empno=@empno

    commit
    select * from employee
    return 1
end;

declare @status int
exec @status=sp_empdata1 1020,'shah',13,10000
print @status
            
select * from sys.procedures;

create or alter function dbo.EmpInsertion(@empno int,@ename varchar(20))
returns varchar(20)
as
begin
    return CAST(@empno as varchar)+ '-' +@ename
end;

select dbo.EmpInsertion(2000,'qwerty') response

create or alter  function Avgsal()
returns table
as return select deptno,avg(sal) as avgsal from employee group by deptno;

select*from Avgsal()

--Triggers

create trigger emp_log
on employee
after Insert
as
begin
    print 'New employee  record inserted.'
end

insert into employee(empno,ename) values (2001,'jkl')