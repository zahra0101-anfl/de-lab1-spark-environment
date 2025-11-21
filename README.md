
-------------------------------------

✔ Part A — Spark Cluster (Docker)  
• 1 Spark Master + 1 Spark Worker running in Docker  
• Worker cores and memory detected correctly  
• Spark Master UI available at: http://localhost:8080  
 Screenshot: screenshots/spark-master-ui.png  

-------------------------------------

✔ Part B — Local PySpark Setup 
• Python 3.11 used  
• PySpark 3.5.0 installed  
• SparkSession starts successfully  
Output file: output/app-output.txt  

-------------------------------------

✔ Part C — First Spark Application
Script: lab1_hello_spark.py  
• Creates SparkSession  
• Builds a DataFrame  
• Shows schema  
• Runs filters and aggregations  
• Prints final row count  

-------------------------------------

✔ Part D — Spark Application UI (4040) 
Screenshots included:  
 job-overview.png  
 dag.png  
 stage-details.png  
 executors.png  

-------------------------------------

✔ Short Notes (Answers)

1) *What does the DAG represent?*  
• It shows the logical flow of transformations.  
• It represents dependencies between operations.  
• It helps Spark optimize execution.

2) *How many stages ran?*  
• Around 2–3 stages depending on the actions.

3) *How many tasks per stage?*  
• Only 1 task per stage (small dataset, local mode).

4) *What did you notice in the Executors tab?*  
• One executor + driver.  
• All tasks run on the same executor.

5) *What pattern did you observe in job triggering?*  
• Each action triggered a new job.  
• Transformations ran only when an action was called.

-------------------------------------

✔ How to Run: 
docker compose up -d  
python lab1_hello_spark.py