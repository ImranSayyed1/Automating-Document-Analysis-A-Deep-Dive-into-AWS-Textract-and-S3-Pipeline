# Automating-Document-Analysis-A-Deep-Dive-into-AWS-Textract-and-S3-Pipeline
All the codes referred in the blog are present in this repo.
So Lambda_Function_1 is the first function of pipeline which is sends the data from initial data zone to Textract and then processes the Textract o/p to enrich it and send it to intermediatery data zone.
lambda_function_1 has 2 files in it Lambda_function and Helper module which is imported in Lambda_function.

Once the data is intermediatery data zone Lambda_Function_2 is triggered which then again pre-processes the data adding some key file system namespaces in the data which is important for the KPI's to be prepared at the latter stage in this pipeline.
Once the pre-processing is done Lambda_Function_2 will than run both Glue_Job_1 and Glue_Job_2 one after the other.
Once Glue_Job_1 is started it's main function is to first seperate key_value and lineitem data and then process and pass this data to their respective target locations.
Glue_Job_2 will take lineitem and explode it for final part of our data pipeline i.e. Data Visualization.
