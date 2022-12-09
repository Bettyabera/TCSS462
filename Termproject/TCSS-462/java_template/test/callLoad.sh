json={\"service\":2,"\"bucketname\"":\"test.bucket.termproject\"","\"filename\"":\"test.csv\""}
echo $json | jq
echo ""
echo "Invoking Lambda function Load using AWS CLI"
time output=`aws lambda invoke --invocation-type RequestResponse --function-name Load --region us-east-2 --payload $json /dev/stdout | head -n 1 | head -c -2 ; echo`
echo ""
echo "JSON RESULT:"
echo $output | jq
echo ""
