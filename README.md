# FireArc_Exam

Ingest data from NYC fire incident dispatch to bigquery

##Table of contents:

1)python ingestation of fact table

2)creating tables in Big query

3)sql for all layers

4)ERD and modeling

we can optimise the code using spark

the clustered index for this api is the primary key called: starfire_incident_id

we should partition the data by most filtered column which is the datetime of the incident

in DWH i can do whatever aggregations i need like:

Some of the aggregations I can do :
SELECT COUNT(*) AS total_incidents
FROM nyc-firedep.Firedep.nyc_firedep

SELECT alarm_level_index_description, COUNT(*) AS incident_count
FROM nyc-firedep.Firedep.nyc_firedep
GROUP BY alarm_level_index_description

SELECT alarm_level_index_description, AVG(response_time_minutes) AS avg_response_time
FROM nyc-firedep.Firedep.nyc_firedep
GROUP BY alarm_level_index_description



