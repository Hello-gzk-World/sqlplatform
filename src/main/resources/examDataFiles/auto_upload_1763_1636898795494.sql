Create table If Not Exists SchoolA (student_id int, student_name varchar(20));Create table If Not Exists SchoolB (student_id int, student_name varchar(20));Create table If Not Exists SchoolC (student_id int, student_name varchar(20));Truncate table SchoolA;insert into SchoolA (student_id, student_name) values ('1', 'Alice');insert into SchoolA (student_id, student_name) values ('2', 'Bob');Truncate table SchoolB;insert into SchoolB (student_id, student_name) values ('3', 'Tom');Truncate table SchoolC;insert into SchoolC (student_id, student_name) values ('3', 'Tom');insert into SchoolC (student_id, student_name) values ('2', 'Jerry');insert into SchoolC (student_id, student_name) values ('10', 'Alice');