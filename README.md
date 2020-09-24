# AWS Micro ETL pipeline

This repository contains code to help you process and periodically refresh a small amount of data coming from one or more sources.  

In addition, you will be able to configure a Python environment to build and deploy your own micro ETL pipeline using your own source of data.

The complete solution includes: AWS Lambda to handle the micro ETL process, an Amazon S3 bucket to store the processed data a local Jupyter notebook to inspect the data and the AWS SAM cli to build and deploy the pipeline.

## Prerequisites
For this walkthrough, you should have the following prerequisites:

- An [AWS account](https://aws.amazon.com/)
- [AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-configure.html) installed and configured
- [Python](https://www.python.org/downloads/) (3.8 preferable)
- [Conda o Miniconda](https://docs.conda.io)


## What’s a micro ETL pipeline? 
It is a short process that you can schedule frequently to handle a small volume of data.  Sometimes, you need to ingest, transform and load only a subset of a larger dataset without using expensive and complex computational resources, that’s where the notion of micro ETL comes to the rescue. 


## Contributing 

See CONTRIBUTING file.

## License

This library is licensed under the MIT-0 License. See the LICENSE file.