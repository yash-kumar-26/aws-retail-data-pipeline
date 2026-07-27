import json
import logging
import urllib.parse
import boto3

# ---------------------------------------------------------
# Logger setup
# Why: anything logged here automatically flows into
# CloudWatch Logs without extra configuration.
# ---------------------------------------------------------
logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3_client = boto3.client("s3")

# Destination prefix (folder) inside the same bucket
RAW_PREFIX = "raw/"

# Allowed file extensions per problem statement
ALLOWED_EXTENSIONS = (".csv", ".parquet")


def lambda_handler(event, context):
    """
    Triggered by S3 ObjectCreated event on the landing/ prefix.
    Validates the uploaded file, then copies it to raw/.
    """

    try:
        # S3 event can contain multiple records in a single invocation
        for record in event["Records"]:
            bucket_name = record["s3"]["bucket"]["name"]

            # Object keys can contain spaces/special chars URL-encoded
            object_key = urllib.parse.unquote_plus(
                record["s3"]["object"]["key"], encoding="utf-8"
            )

            logger.info(f"New file detected: s3://{bucket_name}/{object_key}")

            # -----------------------------------------------
            # VALIDATION STEP 1: File extension check
            # Why: problem statement only allows CSV or Parquet
            # -----------------------------------------------
            if not object_key.lower().endswith(ALLOWED_EXTENSIONS):
                logger.warning(
                    f"REJECTED: {object_key} has invalid extension. "
                    f"Only {ALLOWED_EXTENSIONS} are allowed."
                )
                continue  # skip this file, move to next record if any

            # -----------------------------------------------
            # VALIDATION STEP 2: File is not empty
            # Why: an empty file would break the Glue cleansing
            # job downstream, so catch it here early instead.
            # -----------------------------------------------
            head = s3_client.head_object(Bucket=bucket_name, Key=object_key)
            file_size = head["ContentLength"]

            if file_size == 0:
                logger.warning(f"REJECTED: {object_key} is empty (0 bytes).")
                continue

            # -----------------------------------------------
            # BUILD DESTINATION KEY
            # Why: we preserve just the filename, but change
            # the prefix from landing/ to raw/
            # -----------------------------------------------
            filename = object_key.split("/")[-1]
            destination_key = f"{RAW_PREFIX}{filename}"

            # -----------------------------------------------
            # COPY FILE landing/ -> raw/
            # -----------------------------------------------
            copy_source = {"Bucket": bucket_name, "Key": object_key}
            s3_client.copy_object(
                Bucket=bucket_name,
                CopySource=copy_source,
                Key=destination_key,
            )

            logger.info(
                f"SUCCESS: Copied {object_key} -> {destination_key} "
                f"({file_size} bytes)"
            )

        return {
            "statusCode": 200,
            "body": json.dumps("File validation and copy completed."),
        }

    except Exception as e:
        # Logging the full error so it's visible in CloudWatch
        # for debugging, and re-raising so Lambda marks this
        # invocation as FAILED (needed later for SNS failure alerts)
        logger.error(f"ERROR processing file: {str(e)}")
        raise e
