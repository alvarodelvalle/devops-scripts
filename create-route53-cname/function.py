import json
import re
import boto3
import urllib3
from botocore.exceptions import ClientError


class Conn:
    def __init__(self, service, service_type):
        if service_type == 'client':
            self.client = boto3.client(service)
        else:
            self.client = boto3.resource('s3')

    def connection(self):
        return self.client

class Route53:
    def __init__(self):
        self.client = Conn('route53', 'client').connection()

    def change_resource_record_sets(self, action, hostedzone, source, target):
        """
        Update records in Route53
        :param action:
        :param hostedzone:
        :param source:
        :param target:
        :return:
        """
        try:
            response = self.client.change_resource_record_sets(
                HostedZoneId=hostedzone,
                ChangeBatch={
                    'Comment': 'CNAME %s -> %s' % (source, target),
                    'Changes': [{
                        'Action': action,
                        'ResourceRecordSet': {
                            'Name': source,
                            'Type': 'CNAME',
                            'TTL': 300,
                            'ResourceRecords': [{'Value': target}]
                        }
                    }]
                }
            )
            print(f'Response: {response}')
        except ClientError as e:
            print(e)
            raise

    def get_hosted_zone_id(self, source: str):
        """
        Get the hosted zone ID from a given DNS.
        :param source: The resource property that indicates the DNS to be used as a filter to look up hosted zones.
        :return: hosted_zone_id: The ID associated with the hosted zone per the give DNS
        """
        ldns = re.split(r'\.', source)
        dns = f'{ldns[-3]}.{ldns[-2]}.'
        hosted_zone_id = ''
        try:
            hosted_zones_list = self.client.list_hosted_zones_by_name(
                DNSName=dns,
            )
            hosted_zones = hosted_zones_list['HostedZones']
            for z in hosted_zones:
                for _, v in z.items():
                    if v == dns:
                        print(f'Found DNS Hosted Zone ID in object: {z}')
                        hosted_zone_id = re.split(r'/', z.get('Id'))[-1]
                        break
                break

            if hosted_zone_id != '':
                print(f'Found ID ({hosted_zone_id}) for source {source}')
                return hosted_zone_id
            else:
                print(f'Did not find ID {hosted_zone_id} for source {source}')
        except ClientError as e:
            print(f'ERROR: Did not return hosted zone id for {source}: {e}')
            raise


def send_response(event, context, response_status, response_data):
    """
    Sends a 'SUCCESS' or 'FAILED' response to the event callback URL
    :param event:
    :param context:
    :param response_status:
    :param response_data:
    :return:
    """
    data = json.dumps({
        'Status': response_status,
        'Reason': 'See the details in CloudWatch Log Stream: ' + context.log_stream_name,
        'PhysicalResourceId': context.log_stream_name,
        'StackId': event['StackId'],
        'RequestId': event['RequestId'],
        'LogicalResourceId': event['LogicalResourceId'],
        'Data': response_data
    }).encode('utf-8')
    print(f'Send response to ${event["ResponseURL"]}')
    print(data)
    http = urllib3.PoolManager()
    r = http.request(
        'PUT',
        event['ResponseURL'],
        headers={
            'Content-Type': 'application/json'
        },
        body=data
    )
    print(f'Response: {r}')


def lambda_handler(event, context):
    """
    SNS events contain a wrapper around the Lambda event. Unpack the lambda event from SNS and handle Route53 updates
    according to request type.  See create_event.json for an example of a CREATE request.
    :param event: The SNS event payload.
    :param context: Context of the handler.
    :return:
    """

    print(f'SNS Event: ${json.dumps(event)}')
    event = json.loads(event['Records'][0]['Sns']['Message'])
    print(f'Lambda Event: ${json.dumps(event)}')

    try:
        request_type = event['RequestType']
        source = event['ResourceProperties']['Source']
        target = event['ResourceProperties']['Target']
        route53 = Route53()
        hostedzone = route53.get_hosted_zone_id(source)

        if request_type == 'Create':
            print(f'Creating CNAME {source} -> {target} in {hostedzone}')
            route53.change_resource_record_sets('UPSERT', hostedzone, source, target)
        elif request_type == 'Update':
            oldsource = event['OldResourceProperties']['Source']
            oldtarget = event['OldResourceProperties']['Target']
            print(f'Deleting old CNAME {oldsource} -> {oldtarget} in {hostedzone}')
            route53.change_resource_record_sets('DELETE', hostedzone, oldsource, oldtarget)
            print(f'Creating new CNAME {source} -> {target} in {hostedzone}')
            route53.change_resource_record_sets('UPSERT', hostedzone, source, target)
        elif request_type == 'Delete':
            print(f'Deleting CNAME {source} -> {target} in {hostedzone}')
            route53.change_resource_record_sets('DELETE', hostedzone, source, target)
        else:
            print("Unexpected Request Type")
            raise Exception("Unexpected Request Type")

        print("Completed successfully")
        response_status = 'SUCCESS'
        response_data = {}
        send_response(event, context, response_status, response_data)

    except SystemExit as e:
        print("Error:", e)
        response_status = 'FAILED'
        response_data = {}
        send_response(event, context, response_status, response_data)
