## Requirement

The PoC setup is to demo 

1. Airflow orchestrating complex Glue jobs where the workflow structure is input as a descriptive JSON file. 

2. The Glue jobs are Glue 2.0 jobs with faster start time. 

## PoC setup:

### 1. Set up MWAA(Amazon Managed Workflows for Apache Airflow) environment

As the PoC is to demo the Airflow orchestration of Glue jobs, we use MWAA for a quick demo. Function-wise, the community Airflow is similar to MWAA and the deployment architecture of Airflow in production will be discussed in another topic. Follow the quick [start guide](https://docs.aws.amazon.com/mwaa/latest/userguide/quick-start.html) to set up MWAA.

Note: 

1. MWAA currently supports Airflow 1.10.12 and not 2.0. The plugins and subpackages should be compatible with Airflow 1.10

2. As MWAA will need to start run Glue jobs, the policy in the setup will need extra privilege to Glue service. 

3. ```
   {
       "Version": "2012-10-17",
       "Statement": [
           {
               "Sid": "VisualEditor0",
               "Effect": "Allow",
               "Action": [
                   "glue:GetJobs",
                   "glue:StartJobRun",
                   "glue:GetJobRun",
                   "glue:ListJobs",
                   "glue:GetJobRuns",
                   "glue:GetJob"
               ],
               "Resource": "*"
           }
       ]
   }
   ```

### 2. Modify the requirements in MWAA



 



