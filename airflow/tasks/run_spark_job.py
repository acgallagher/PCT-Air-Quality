import boto3
from dotenv import dotenv_values
from emr_serverless import EMRServerless

# load config
config = dotenv_values("opt/airflow/configs/configs.env")

EMRServerless()


def run_spark_job(script: str, *args):
    """
    EMR Serverless spark job template
    https://github.com/aws-samples/emr-serverless-samples/tree/main/examples/python-api
    """
    emr_serverless_client = boto3.client(
        "emr-serverless",
        region_name="us-east-1",
        aws_access_key_id=config["aws_access_key_id"],
        aws_secret_access_key=config["aws_secret_access_key"],
    )

    response = emr_serverless_client.create_application(
        name="pct-aq-spark", releaseLabel="emr-6.6.0", type="SPARK"
    )

    print(
        "Created application {name} with application id {applicationId}. Arn: {arn}".format_map(
            response
        )
    )

    applicationId = response["applicationId"]

    emr_serverless_client.start_application(applicationId=applicationId)

    try:
        response = emr_serverless_client.start_job_run(
            applicationId=applicationId,
            executionRoleArn="arn:aws:iam::188237326080:role/EMRServerlessS3RuntimeRole",
            jobDriver={
                "sparkSubmit": {
                    "entryPoint": f"s3://pct-air-quality/scripts/{script}",
                    "entryPointArguments": args,
                    "sparkSubmitParameters": "--conf spark.executor.cores=1 --conf spark.executor.memory=4g --conf spark.driver.cores=1 --conf spark.driver.memory=4g --conf spark.executor.instances=1",
                }
            },
        )

        job_run_id = response["jobRunId"]

    finally:
        # Shut down and delete your application
        """
        job_done = False
        while not job_done:
            jr_response = emr_serverless_client.get_job_run(
                applicationId=applicationId, jobRunId=job_run_id
            )
            job_done = jr_response.get("state") in [
                "SUCCESS",
                "FAILED",
                "CANCELLING",
                "CANCELLED",
            ]
        """

        emr_serverless_client.stop_application(applicationId=applicationId)
        emr_serverless_client.delete_application(applicationId=applicationId)
