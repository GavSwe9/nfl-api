import boto3
import base64
import json
from botocore.exceptions import ClientError

def getSecret():

    secret_name = "farm/mysql"
    region_name = "us-east-1"

    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        print(e);
        return None;
    else:
        if 'SecretString' in get_secret_value_response:
            secret = get_secret_value_response['SecretString']
            return secret;
        else:
            decoded_binary_secret = base64.b64decode(get_secret_value_response['SecretBinary'])
            return decoded_binary_secret;
            
def getDbPass():
    secret = getSecret();
    return json.loads(secret)["password"];
