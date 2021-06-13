import boto3


class Conn:
    def __init__(self, service, service_type, region=None):
        if service_type == 'client':
            self.client = boto3.client(service, region_name=region)
        else:
            self.client = boto3.resource('s3')

    def connection(self):
        return self.client
