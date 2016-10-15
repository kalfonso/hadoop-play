use employees;

with employees_start_end as
(
select emp_no, from_date, to_date, datediff(max(to_date), min(from_date)) as days_worked 
from salaries group by emp_no, from_date, to_date
)
select e.emp_no, e.first_name, e.last_name, ese.from_date, ese.to_date from employees e
join employees_start_end ese
on e.emp_no=ese.emp_no
where ese.days_worked / 7 < 2
and year(ese.to_date)=1985;
