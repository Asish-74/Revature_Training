use RevStudDb
INSERT INTO dbo.student
VALUES (101, 'Asish', 'Odisha', 'Ganjam', 761026);
INSERT INTO dbo.student
VALUES (102, 'Deeak', 'Odisha', 'Ganjam', 761030);

 

-- inset values in a db 
INSERT INTO dbo.student(rollno,sname,pin)
VALUES (103, 'Ashutosh',  761030);

INSERT INTO dbo.student(sname,pin,rollno)
VALUES ('Narayana' , 760003, 104);

--update data----
update student set addr='Odisha'
where rollno=103;

update student set city ='Ganjam'
where rollno=104;

--delete from student(ALL THE ROWS ARE DELETING)
DELETE FROM student
WHERE rollno=104

--Truncate Querry (all the data will go to delete but columns are not delete, it is permanetly deleted beacuse of All the DDL Commands are auto commit )
Truncate table student ;

begin transaction;

INSERT INTO dbo.student
VALUES (101, 'Asish', 'Odisha', 'Ganjam', 761026);
INSERT INTO dbo.student
VALUES (102, 'Deeak', 'Odisha', 'Ganjam', 761030); 

select * from student

--it save the transaction
save transaction level1;

INSERT INTO dbo.student(rollno,sname,pin)
VALUES (103, 'Ashutosh',  761030);

INSERT INTO dbo.student(sname,pin,rollno)
VALUES ('Narayana' , 760003, 104);

save transaction level2;

update student set addr='xcsadvb'
where rollno=103 ;
--this address is wrong so i am going tomundo  (rollback)

rollback transaction level2; 

-- commit (Permanately saved )
commit transaction;  -- after commit we cant rollback 
update student set addr='Odisha'
where rollno=103 ;