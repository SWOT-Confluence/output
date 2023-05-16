# output

The Output module appends results from each stage of the Confluence workflow
to a new version of the SoS.

Each stage requiring storage has a class and the run type is determined by
the command line argument so that the new version gets uploaded to the 
correct location in the SoS S3 bucket.

Output writes data from various EFS mounts that hold the intermediate data results from each module to continent-level SoS result NetCDF files. 

# installation

Build a Docker image: `docker build -t output .`

# execution

**Command line arguments:**
- -i: index to locate continent in JSON file
- -c: Name of the continent JSON file
- -r: run type for workflow execution: 'constrained' or 'unconstrained'
- -m: List of modules to gather output data for: "hivdi", "metroman", "moi", "momma", "neobam", "prediagnostics", "priors", "sad", "sic4dvar", "swot", "validation", "offline"

**Execute a Docker container:**

AWS credentials will need to be passed as environment variables to the container so that `output` may access AWS infrastructure to generate JSON files.

```
# Credentials
export aws_key=XXXXXXXXXXXXXX
export aws_secret=XXXXXXXXXXXXXXXXXXXXXXXXXX

# Docker run command
docker run --rm --name output -e AWS_ACCESS_KEY_ID=$aws_key -e AWS_SECRET_ACCESS_KEY=$aws_secret -e AWS_DEFAULT_REGION=us-west-2 -e AWS_BATCH_JOB_ARRAY_INDEX=3 -v /mnt/output:/data output:latest -i "-235" -c continent.json -r constrained -m hivdi metroman moi momma neobam offline postdiagnostics prediagnostics priors sad sic4dvar swot validation
```

# tests

1. Run the unit tests: `python3 -m unittest discover tests`