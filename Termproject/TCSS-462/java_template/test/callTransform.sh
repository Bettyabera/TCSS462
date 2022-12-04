#!/bin/bash

# JSON object to pass to Lambda Function
json={"\"bucketname\"":\"test.bucket.termproject\"","\"filename\"":\"100\u0020Sales\u0020Records.csv\""}
echo $json | jq
echo ""
echo "Invoking Lambda function using AWS CLI"
time output=`aws lambda invoke --invocation-type RequestResponse --function-name Transform --region us-east-2 --payload $json /dev/stdout | head -n 1 | head -c -2 ; echo`
echo ""
echo "JSON RESULT:"
echo $output | jq
echo ""
