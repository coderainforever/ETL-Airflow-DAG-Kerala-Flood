Created Airflow DAG to split the SOS tweets during Kerala Floods of 2018, 2019 by district. 
- First, the tweets which were pulled using Twitter API was saved in .csv format and uploaded to Google Cloud Storage. 
- Then, they were ingested into a table in a staging database on BigQuery. 
- After that, the table were transformed using SQL and 14 tables and a facts table was created and loaded into another database.
