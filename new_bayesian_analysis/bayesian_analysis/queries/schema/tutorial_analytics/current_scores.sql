drop table if exists tutorial_analytics.current_scores;
create table tutorial_analytics.current_scores
(
patient_id integer,
assessment_type text,
risk_score integer,
date_modified date,
primary key (patient_id, assessment_type)
)
;

insert into tutorial_analytics.current_scores
select
  patient_id,
  assessment_type,
  risk_score,
  date_modified
from
(
  select
    patient_id,
    assessment_type,
    risk_score,
    date_modified,
    row_number() over (partition by patient_id, assessment_type order by date_modified desc) as row
  from tutorial_data_ingest.risk_assessments
) as sub
where row = 1
