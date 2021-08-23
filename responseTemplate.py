import json

def responseTemplate(statusCode, response):
    if (statusCode == 200):
        return {
            "statusCode": 200,
            "headers": {
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Credentials': True
            },
            "body": json.dumps(response)
        }
    if (statusCode == 400):
        return {
            "statusCode": 400,
            "headers": {
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Credentials': True
            },
            "body": json.dumps({
                "ErrorText": "Missing field in body: " + response
            })
        }
