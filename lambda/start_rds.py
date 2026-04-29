import boto3

rds = boto3.client("rds")
DB_INSTANCE_ID = "etl"

def lambda_handler(event, context):

    status = rds.describe_db_instances(DBInstanceIdentifier = DB_INSTANCE_ID)["DBInstances"][0]["DBInstanceStatus"]

    if status != "available":
        rds.start_db_instance(DBInstanceIdentifier=DB_INSTANCE_ID)

    return f"RDS status: {status}"