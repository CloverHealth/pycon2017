drop table if exists tutorial_analytics.doctor_dashboard;
create table tutorial_analytics.doctor_dashboard
(
doctor_id text,
assessment_type text,
avg_risk_score numeric
)
;

insert into tutorial_analytics.doctor_dashboard
select
  patients.doctor_id,
  scores.assessment_type,
  avg(scores.risk_score) as avg_risk_score
from tutorial_analytics.current_scores as scores
  join tutorial_data_ingest.patients as patients
    on scores.patient_id = patients.patient_id
group by
  patients.doctor_id,
  scores.assessment_type
order by
  patients.doctor_id,
  scores.assessment_type
