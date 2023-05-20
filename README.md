# FireArc_Exam

Ingest data from NYC fire incident dispatch to bigquery

##Table of contents:

1)python ingestation of fact table

2)creating tables in Big query

3)sql for all layers

4)ERD and modeling


modeling:
we can optimise the code using spark
* It can distribute the workload across a cluster of machines
* Spark provides the ability to persist intermediate results in memory or disk
the clustered index for the fact is the primary key called: starfire_incident_id

we should partition the data by most filtered column which is the datetime of the incident

in DWH i could have done in dwh fact , it all depands what is the final table we want to get:

Some of the aggregations I can do :
select count(*) AS total_incidents
FROM nyc-firedep.Firedep.nyc_firedep

select alarm_level_index_description, count(*) AS incident_count
from nyc-firedep.Firedep.nyc_firedep
group by alarm_level_index_description

select alarm_level_index_description, avg(response_time_minutes) as avg_response_time
from nyc-firedep.Firedep.nyc_firedep
group by alarm_level_index_description




