use RevStudDb

CREATE TABLE student(
rollno int ,
sname varchar(25) not null,
addr varchar (50) ,
CONSTRAINT pk_rollno primary key(rollno)
);

alter table student add  city varchar(20), pin int ;
alter table student  alter column  addr varchar(100);

alter table student add dummycol int ;
EXEC sp_rename 'student.dummycol', 'dummy', 'column';
alter table student drop column dummy;