-- Databricks notebook source
create DATABASE  IF NOT EXISTS f1_presentation;

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC USE f1_presentation

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.widgets.text("p_file_date", "2021-03-28")
-- MAGIC v_file_date = dbutils.widgets.get("p_file_date")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.sql(f"""
-- MAGIC               CREATE TABLE IF NOT EXISTS f1_presentation.calculated_race_results
-- MAGIC               (
-- MAGIC               race_year INT,
-- MAGIC               team_name STRING,
-- MAGIC               driver_id INT,
-- MAGIC               driver_name STRING,
-- MAGIC               race_id INT,
-- MAGIC               position INT,
-- MAGIC               points INT,
-- MAGIC               calculated_points INT,
-- MAGIC               created_date TIMESTAMP,
-- MAGIC               updated_date TIMESTAMP
-- MAGIC               )
-- MAGIC               USING DELTA
-- MAGIC """)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.sql(f"""
-- MAGIC               CREATE OR REPLACE TEMP VIEW race_result_updated
-- MAGIC               AS
-- MAGIC               SELECT races.race_year,
-- MAGIC                      constructors.name AS team_name,
-- MAGIC                      drivers.driver_id,
-- MAGIC                      drivers.name AS driver_name,
-- MAGIC                      races.race_id,
-- MAGIC                      results.position,
-- MAGIC                      results.points,
-- MAGIC                      11 - results.position AS calculated_points
-- MAGIC                 FROM f1_processed.results 
-- MAGIC                 JOIN f1_processed.drivers ON (results.driver_id = drivers.driver_id)
-- MAGIC                 JOIN f1_processed.constructors ON (results.constructor_id = constructors.constructor_id)
-- MAGIC                 JOIN f1_processed.races ON (results.race_id = races.race_id)
-- MAGIC                WHERE results.position <= 10
-- MAGIC                  AND results.file_date = '{v_file_date}'
-- MAGIC """)

-- COMMAND ----------



-- COMMAND ----------

'''
%python
spark.sql(f"""
              MERGE INTO f1_presentation.calculated_race_results tgt
              USING race_result_updated upd
              ON (tgt.driver_id = upd.driver_id AND tgt.race_id = upd.race_id)
              WHEN MATCHED THEN
                UPDATE SET tgt.position = upd.position,
                           tgt.points = upd.points,
                           tgt.calculated_points = upd.calculated_points,
                           tgt.updated_date = current_timestamp
              WHEN NOT MATCHED
                THEN INSERT (race_year, team_name, driver_id, driver_name,race_id, position, points, calculated_points, created_date ) 
                     VALUES (race_year, team_name, driver_id, driver_name,race_id, position, points, calculated_points, current_timestamp)
       """)
       '''
       

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC SELECT COUNT(*) FROM race_result_updated;

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC SELECT COUNT(1) FROM f1_presentation.calculated_race_results;

-- COMMAND ----------

