# Week 2 Solution

## Question 1. Load January 2020 data

Using the `etl_web_to_gcs.py` flow that loads taxi data into GCS as a guide, create a flow that loads the green taxi CSV dataset for January 2020 into GCS and run it. Look at the logs to find out how many rows the dataset has.

How many rows does that dataset have?

### Answer
**447,770**

_Explanation_
![result](./answer/q1_result.png)


## Question 2. Scheduling with Cron

Cron is a common scheduling specification for workflows. 

Using the flow in `etl_web_to_gcs.py`, create a deployment to run on the first of every month at 5am UTC. What’s the cron schedule for that?

### Answer
**`0 5 1 * *`**

_Explanation_
We can create a delpoyment and set a schedule in one-line command via CLI.
```
prefect deployment build answer/q2-answer.py:etl_web_to_gcs -n "question-2" \
    --cron "0 5 1 * *" \
    --apply
```
Result: 
![result-a](./answer/q2_result_a.png)

Screenshot on Orion UI dashboard:
![result-b](./answer/q2_result_b.png)

## Question 3. Loading data to BigQuery 

Using etl_gcs_to_bq.py as a starting point, modify the script for extracting data from GCS and loading it into BigQuery. This new script should not fill or remove rows with missing values. (The script is really just doing the E and L parts of ETL).

The main flow should print the total number of rows processed by the script. Set the flow decorator to log the print statement.

Parametrize the entrypoint flow to accept a list of months, a year, and a taxi color.

Make any other necessary changes to the code for it to function as required.

Create a deployment for this flow to run in a local subprocess with local flow code storage (the defaults).

Make sure you have the parquet data files for Yellow taxi data for Feb. 2019 and March 2019 loaded in GCS. Run your deployment to append this data to your BiqQuery table. How many rows did your flow code process?

## Answer
**14,851,920**

_Explanation_

- ingest data for Yellow taxi data for Feb-March 2019
```
python3 answer/q3_answer_gcs.py
```
- register a parameterized gcs-to-gcp flow 
```
prefect deployment build answer/q3_answer_bq.py:etl_parent_flow \
    --name "question-3"
```
- change the parameters in  `etl_parent_flow-deployment.yaml` to: 
```
  color: yellow
  months:
  - 2
  - 3
  year: 2019
```
- apply the new parameters to the deployment 
```
prefect deployment apply etl_parent_flow-deployment.yaml
```
- start the agent 
```
prefect agent start -q default  
```
- go to the Orion UI to run the deployment
![run-deployment](./answer/q3_run_deployment.png)

- Screenshot on Orion UI dashboard:
![result](./answer/q3_result.png)

## Question 4. Github Storage Block
Using the web_to_gcs script from the videos as a guide, you want to store your flow code in a GitHub repository for collaboration with your team. Prefect can look in the GitHub repo to find your flow code and read it. Create a GitHub storage block from the UI or in Python code and use that in your Deployment instead of storing your flow code locally or baking your flow code into a Docker image.

Note that you will have to push your code to GitHub, Prefect will not push it for you.

Run your deployment in a local subprocess (the default if you don’t specify an infrastructure). Use the Green taxi data for the month of November 2020.

How many rows were processed by the script?

### Answer
**88,605**

_Explanation_
- Modify web_to_gcs code, upload to github
- Create a GitHub storage block from the UI (screenshot)
![storage-block](./answer/q4_github_block.png)


- Create and run deployment
```
prefect deployment build answer/q4_answer.py:etl_web_to_gcs_parent -n "question-4" \
    --params='{"color": "green", "months": [11], "year": "2020"}' \
    --storage-block="github/ingest-data" \
    --apply

prefect deployment run 'etl-web-to-gcs-parent/question-4'

```
- Map agent to the currently running deployment 
```
prefect agent start -q default
```

- Get the number of row counts

![result](./answer/q4_result.png)


## Question 5. Email or Slack notifications

Q5. It’s often helpful to be notified when something with your dataflow doesn’t work as planned. Choose one of the options below for creating email or slack notifications.

The hosted Prefect Cloud lets you avoid running your own server and has Automations that allow you to get notifications when certain events occur or don’t occur. 

Create a free forever Prefect Cloud account at app.prefect.cloud and connect your workspace to it following the steps in the UI when you sign up. 

Set up an Automation that will send yourself an email when a flow run completes. Run the deployment used in Q4 for the Green taxi data for April 2019. Check your email to see the notification.

Alternatively, use a Prefect Cloud Automation or a self-hosted Orion server Notification to get notifications in a Slack workspace via an incoming webhook. 

Join my temporary Slack workspace with [this link](https://join.slack.com/t/temp-notify/shared_invite/zt-1odklt4wh-hH~b89HN8MjMrPGEaOlxIw). 400 people can use this link and it expires in 90 days. 

In the Prefect Cloud UI create an [Automation](https://docs.prefect.io/ui/automations) or in the Prefect Orion UI create a [Notification](https://docs.prefect.io/ui/notifications/) to send a Slack message when a flow run enters a Completed state. Here is the Webhook URL to use: https://hooks.slack.com/services/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

Test the functionality.

Alternatively, you can grab the webhook URL from your own Slack workspace and Slack App that you create. 


How many rows were processed by the script?

### Answer
**`514,392`**

_Explanation_
- create account and workspace here https://app.prefect.cloud/
- go to your profile > settings > API Keys, add new API Keys. this api-key will be used to login to prefect using CLI.
- then, change prefect local configurations to prefect cloud, here are the steps: 
```
$ prefect cloud login -k <your-prefect-api-key-login> --workspace "<your-workspace-name>"

# copy PREFECT_API_URL from url browser
$ prefect config set PREFECT_API_URL="https://app.prefect.cloud/account/<account-id>/workspace/<workspace-id>"

$ prefect config set PREFECT_API_KEY="<prefect-api-key>"
```
- create GCP, GCS and github blocks, as what you've made in Orion UI

- On Prefect Cloud UI > Automation, create automations. I setup 3 automations: 2 will notify to a slack channel, 1 will notify to my email.
![automations](./answer/q5_automations.png)


- Create and run a deployment to ingest Green Taxi data at April, 2019. Then map an agent to it.
- Turns out, there is an email notification coming when the deployment in a `running` or `complete` state. Hence, no notification in slack channel.


![completed-flow](./answer/q5_succeed_flow.png)


![email-notification](./answer/q5_email_notification.png)


- here are the total number of the processed rows 

![result](./answer/q5_result.png)


## Question 6. Secrets

Prefect Secret blocks provide secure, encrypted storage in the database and obfuscation in the UI. Create a secret block in the UI that stores a fake 10-digit password to connect to a third-party service. Once you’ve created your block in the UI, how many characters are shown as asterisks (*) on the next page of the UI?

### Answer
**8**

_Explanation_

I created the secret block from the UI and here is the result:
![result](./answer/q6_result.png)
