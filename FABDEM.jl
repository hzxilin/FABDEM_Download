using HTTP
using AWSS3
using AWS
using AWS: @service
@service S3


# Configuration for AWS 
const AWS_REGION = "us-east-2"  # Replace with your AWS region
const S3_BUCKET = "fabdem.download.test–use2-az985539793478–x-s3"  # Replace with your S3 bucket name
const DOWNLOAD_URL = "https://data.bris.ac.uk/datasets/s5hqmjcdj8yo2ibzi9b4ew3sn/N10E110-N20E120_FABDEM_V1-2.zip"  # Replace with your file URL
const S3_KEY = "N10E110-N20E120_FABDEM_V1-2.zip"  # Desired S3 object key

test = HTTP.get(DOWNLOAD_URL, StreamOpen=true)
aws = global_aws_config(; region="us-east-2")
s3_put(aws, "fabdem.download.test","test",test.body)


