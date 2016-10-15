use employees;

create table employees
stored as RCFile
as
select e.emp_no, e.birth_date, e.first_name, e.last_name, e.gender, min(s.from_date) as hire_date  
from employees_temp e
join salaries_temp s
on e.emp_no=s.emp_no
group by e.emp_no, e.birth_date, e.first_name, e.last_name, e.gender;

create table salaries
stored as RCFile
as
select emp_no, salary, from_date, date_sub(to_date, 1) as to_date
from salaries_temp;
