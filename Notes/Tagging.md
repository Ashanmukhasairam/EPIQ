**Glue jobs**



**for job in $(aws glue get-jobs --query 'Jobs\[].Name' --output text)**

**do**

  **echo "Tagging $job"**



  **aws glue tag-resource \\**

  **--resource-arn arn:aws:glue:us-east-1:730878889077:job/$job \\**

  **--tags '{**

  **"Owner":"DataEngineering",**

  **"Environment":"Dev",**

  **"Product":"EDP",**

  **"BusinessUnit":"",**

  **"ApplicationType":"",**

  **"BusinessRegion":"",**

  **"Operations":"",**

  **"Division":"",**

  **"BusinessOps":"",**

  **"TechOps":"",**

  **"TechEng":"",**

  **"TechArch":""**

  **}'**



**done**

**--------------------------------------------------------------------------------------------------**



**S3 Buckets**



aws s3api put-bucket-tagging \\

--bucket \\

--tagging 'TagSet=\[

{Key=Name,Value=epiq-edp-dl-dev-silver},

{Key=Owner,Value=DataEngineering},

{Key=Environment,Value=Dev},

{Key=Product,Value=EDP},

{Key=BusinessUnit,Value=""},

{Key=ApplicationType,Value=""},

{Key=BusinessRegion,Value=""},

{Key=Operations,Value=""},

{Key=Division,Value=""},

{Key=BusinessOps,Value=""},

{Key=TechOps,Value=""},

{Key=TechEng,Value=""},

{Key=TechArch,Value=""}

]'

**--------------------------------------------------------------------------------------------------**



**AIRFLOW-DEV**



aws mwaa tag-resource \\

--resource-arn arn:aws:airflow:us-east-1:730878889077:environment/edp-salesforce-mwaa-dev \\

--tags '{"environment":"dev","project":"edp","owner":"data-engineering"}'





**CLOUDWATCH-DEV**



aws logs tag-log-group \\

--log-group-name "/aws-glue/crawlers" \\

--tags '{"environment":"dev","project":"edp","owner":"data-engineering"}'





aws logs tag-log-group \\

--log-group-name "/aws-glue/jobs/error" \\

--tags '{"environment":"dev","project":"edp","owner":"data-engineering"}'





-----------------------------------------------------------------------------------------------------



**ATHENA-DEV**



aws athena list-work-groups



aws athena tag-resource \\

--resource-arn arn:aws:athena:us-east-1:730878889077:workgroup/primary \\

--tags Key=Owner,Value=DataEngineering \\

Key=Environment,Value=Dev \\

Key=Product,Value=EDP \\

Key=BusinessUnit,Value="" \\

Key=ApplicationType,Value="" \\

Key=BusinessRegion,Value="" \\

Key=Operations,Value="" \\

Key=Division,Value="" \\

Key=BusinessOps,Value="" \\

Key=TechOps,Value="" \\

Key=TechEng,Value="" \\

Key=TechArch,Value=""





for wg in $(aws athena list-work-groups --query 'WorkGroups\[].Name' --output text)

do

&nbsp; echo "Tagging $wg"



&nbsp; aws athena tag-resource \\

&nbsp; --resource-arn arn:aws:athena:us-east-1:730878889077:workgroup/$wg \\

&nbsp; --tags Key=Owner,Value=DataEngineering \\

&nbsp; Key=Environment,Value=Dev \\

&nbsp; Key=Product,Value=EDP

done



-----------------------------------------------------------------------------------------------------



**SNS-DEV**



~ $ aws sns list-topics

{

    "Topics": \[

        {

            "TopicArn": "arn:aws:sns:us-east-1:730878889077:aws-controltower-SecurityNotifications"

        },

        {

            "TopicArn": "arn:aws:sns:us-east-1:730878889077:bronze-glue-alerts"

        },

        {

            "TopicArn": "arn:aws:sns:us-east-1:730878889077:epiq-glue-dev-alerts"

        },

        {

            "TopicArn": "arn:aws:sns:us-east-1:730878889077:monetoring-alerting-test-topic"

        }

    ]

}





**epiq-glue-dev-alerts**

aws sns tag-resource \\

--resource-arn arn:aws:sns:us-east-1:730878889077:epiq-glue-dev-alerts \\

--tags Key=environment,Value=dev Key=project,Value=edp Key=owner,Value=data-engineering





**For Verification**

aws sns list-tags-for-resource \\

--resource-arn arn:aws:sns:us-east-1:730878889077:epiq-glue-dev-alerts



---



**LAMBDA-DEV**



aws lambda list-functions



aws lambda tag-resource \\

--resource arn:aws:lambda:us-east-1:730878889077:function:EpiqEdpStack-CustomS3AutoDeleteObjectsCustomResour-8IDMRjvv3LZg \\

--tags '{"environment":"dev","project":"edp","owner":"data-engineering"}'



---



**IAM-DEV**



**aws iam list-roles**



**aws iam tag-role \\**

**--role-name AmazonMWAA-edp-salesforce-mwaa-dev-enAZXS \\**

**--tags Key=environment,Value=dev Key=project,Value=edp Key=owner,Value=data-engineering**



**--------------------------------------------------------------------------------------------------------------------**



**EventBridge-Dev**



**aws events list-rules**



**aws events tag-resource \\**

**--resource-arn arn:aws:events:us-east-1:730878889077:rule/RULE\_NAME \\**

**--tags Key=environment,Value=dev Key=project,Value=edp Key=owner,Value=data-engineering**



**-------------------------------------------------------------------------------------------------------------------**



**CodeBuild-Dev**



**aws codebuild list-projects**



**aws codebuild update-project \\**

**--name epiq-edp-dev-build \\**

**--tags key=environment,value=dev key=project,value=edp key=owner,value=data-engineering**

**-------------------------------------------------------------------------------------------------------------------**

