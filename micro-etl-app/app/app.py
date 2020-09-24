#
# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0
#
# Permission is hereby granted, free of charge, to any person obtaining a copy of this
# software and associated documentation files (the "Software"), to deal in the Software
# without restriction, including without limitation the rights to use, copy, modify,
# merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
# permit persons to whom the Software is furnished to do so.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
# INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A
# PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
# HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
# OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
# SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
#

import boto3
import logging
import pandas as pd
import os
import requests
import io

# Environment variables
URL = os.environ['Url']
S3_BUCKET = os.environ['S3Bucket']
LOG_LEVEL = os.environ['LogLevel']
FILENAME = os.environ['Filename']

# Log settings
logger = logging.getLogger()
logger.setLevel(LOG_LEVEL)

# Function to support partial requests
def range_header(start=0, end=0):    
    if start < 0 and end == 0:
        byte_range = 'bytes=%s' % (start)
    else:        
        byte_range = 'bytes=%s-%s' % (start, end)

    return {'Range': byte_range}

# Lambda function handler
def lambda_handler(event, context):
    logger.info('## EVENT')
    logger.info(event)
    
    columns = ['Date', 'Region_Name', 'Area_Code', 'Detached_Average_Price',
       'Detached_Index', 'Detached_Monthly_Change', 'Detached_Annual_Change',
       'Semi_Detached_Average_Price', 'Semi_Detached_Index',
       'Semi_Detached_Monthly_Change', 'Semi_Detached_Annual_Change',
       'Terraced_Average_Price', 'Terraced_Index', 'Terraced_Monthly_Change',
       'Terraced_Annual_Change', 'Flat_Average_Price', 'Flat_Index',
       'Flat_Monthly_Change', 'Flat_Annual_Change']
    
    # Request to get the last 2000000 bytes to get the most recent data in the CSV skipping the first row
    # Implies that the value are in ascending order with most recent at the end of the file
    res = requests.get(URL, headers=range_header(-2000000), allow_redirects=True)
    df = pd.read_csv(io.StringIO(res.content.decode('utf-8')), engine='python', error_bad_lines=False, names=columns, skiprows=1)
    logger.info('## NUMBER OF ELEMENTS')
    logger.info(df.size)
    
    # Extract only values in a specified time range
    start_date = '2018-01-01'
    end_date = '2018-12-31'
    date_range = (df['Date'] >= start_date) & (df['Date'] <= end_date)
    df = df[date_range]
    logger.info('## NUMBER OF ELEMENTS IN THE RANGE')
    logger.info(df.size)

    # Save files into S3
    url = 's3://{}/{}'.format(S3_BUCKET, FILENAME)
    df.to_csv(url)
    logger.info('## FILE PATH')
    logger.info(url)