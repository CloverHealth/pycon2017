drop table if exists tutorial.student_data;
create table tutorial.student_data
(
student_id text,
first_name text,
test_name text,
test_score numeric,
score_modified_date date
)
;

insert into tutorial.student_data
values
('001', 'mark', 'algebra 1 plots', 95, '2017-01-01'),
('001', 'leslie', 'algebra 1 plots', 90, '2017-01-01'),
('001', 'sam', 'algebra 1 plots', 100, '2017-01-01'),
('001', 'matt', 'algebra 1 plots', 80, '2017-01-01'),
('001', 'judy', 'algebra 1 plots', 88, '2017-01-01'),
('001', 'parth', 'algebra 1 plots', 93, '2017-01-01')
