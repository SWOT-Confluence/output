# output

The Output module appends results from each stage of the Confluence workflow
to a new version of the SoS.

Each stage requiring storage has a class and the run type is determined by
the command line argument so that the new version gets uploaded to the 
correct location in the SoS S3 bucket.

Output writes data from various EFS mounts that hold the intermediate data results from each module to continent-level SoS result NetCDF files. 

## installation

Build a Docker image: `docker build -t output .`

## execution

**Command line arguments:**

- -i: index to locate continent in JSON file
- -c: Name of the continent JSON file
- -r: run type for workflow execution: 'constrained' or 'unconstrained'
- -m: List of modules to gather output data for: "hivdi", "metroman", "moi", "momma", "neobam", "prediagnostics", "priors", "sad", "sic4dvar", "swot", "validation", "offline"

**Execute a Docker container:**

AWS credentials will need to be passed as environment variables to the container so that `output` may access AWS infrastructure to generate JSON files.

```bash
# Credentials
export aws_key=XXXXXXXXXXXXXX
export aws_secret=XXXXXXXXXXXXXXXXXXXXXXXXXX

# Docker run command
docker run --rm --name output -e AWS_ACCESS_KEY_ID=$aws_key -e AWS_SECRET_ACCESS_KEY=$aws_secret -e AWS_DEFAULT_REGION=us-west-2 -e AWS_BATCH_JOB_ARRAY_INDEX=3 -v /mnt/output:/data output:latest -i "-235" -c continent.json -r constrained -m hivdi metroman moi momma neobam offline postdiagnostics prediagnostics priors sad sic4dvar swot validation
```

## tests

1. Run the unit tests: `python3 -m unittest discover tests`

## deployment

There is a script to deploy the Docker container image and Terraform AWS infrastructure found in the `deploy` directory.

Script to deploy Terraform and Docker image AWS infrastructure

REQUIRES:

- jq (<https://jqlang.github.io/jq/>)
- docker (<https://docs.docker.com/desktop/>) > version Docker 1.5
- AWS CLI (<https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html>)
- Terraform (<https://developer.hashicorp.com/terraform/tutorials/aws-get-started/install-cli>)

Command line arguments:

[1] registry: Registry URI
[2] repository: Name of repository to create
[3] prefix: Prefix to use for AWS resources associated with environment deploying to
[4] s3_state_bucket: Name of the S3 bucket to store Terraform state in (no need for s3:// prefix)
[5] profile: Name of profile used to authenticate AWS CLI commands

Example usage: ``./deploy.sh "account-id.dkr.ecr.region.amazonaws.com" "container-image-name" "prefix-for-environment" "s3-state-bucket-name" "confluence-named-profile"`

Note: Run the script from the deploy directory.
