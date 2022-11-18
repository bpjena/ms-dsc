
`STARTING INSTRUCTIONS` 
- Follow workings and details [here](https://github.com/bpjena/ms-dsc/blob/master/dsc680_dw_pipeline_infra/DSC680_8.2_Week8_Milestone_3_Workings_Report_Binay%20Jena.pdf)

## deng-jobs local setup (Mac)  

Pre-Requisites :
- python interpreter 3.10+ _install [`poetry`](https://python-poetry.org/docs/#installation) for dependencies_
- docker account [Sign Up : **free single user license**](https://hub.docker.com/) | Sign-In to _[Docker Desktop](https://www.docker.com/products/docker-desktop/) in `running` state_
- 1-Password vault access


NOTE : 
- `deng-jobs` requires `.env` file for the docker env to be up
- ref `.local` for local mac specific command, which correspond to `dags` files for _Airflow job/tasks_
- `.env` file (`*/deng-jobs/.env`) helps enable credentials as environment vars. 
- ref **_[ 1-Password ]_** `.env` contents as below
```html
REDSHIFT_USERNAME=xxxx
REDSHIFT_PASSWORD=xxxx
AWS_ACCESS_KEY_ID=xxxx
AWS_SECRET_ACCESS_KEY=xxxx
AWS_SESSION_TOKEN=xxxx
AWS_SECURITY_TOKEN=xxxx
```
To build and run jobs  locally

```
% git clone https://github.com/bpjena/ms-dsc.git && cd dsc680_project 
% make env-up
```
and then... 
- it brings up container `/app` path 
- command examples
  - env values
```commandline
deng@9481aee6ef8a:/app$ env
```
-  - run task
```commandline
deng@09ac496b9f1e:/app$ /bin/bash -c "PYTHONPATH=. python python/s3_to_redshift/sfdc_s3_file_import.py"
```
------------

## References
- `MWAA-Airflow-101`
  - airflow `MWAA local setup` [MWAA-Airflow - LOCAL setup](https://github.com/aws/aws-mwaa-local-runner)
  - [Docker env in MWAA](https://medium.com/@sohflp/how-to-work-with-airflow-docker-operator-in-amazon-mwaa-5c6b7ad36976)
  - [Best practice - Performance tuning MWAA](https://docs.aws.amazon.com/mwaa/latest/userguide/best-practices-tuning.html)
