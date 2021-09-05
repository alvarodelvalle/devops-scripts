import re

import boto3
from botocore.exceptions import ClientError


class Conn:
    def __init__(self, service, service_type=None, region_name=None):
        if service_type == 'client':
            self.client = boto3.client(service, region_name)
        else:
            self.client = boto3.resource('s3')

    def connection(self):
        return self.client


class S3:
    def __init__(self):
        self.client = Conn('s3', region_name=region).connection()

    def empty_s3_buckets(self, prefix: str = None):
        """
        Delete objects from a bucket. This prepares it for terraform to delete.
        :param prefix: Use this to target buckets with the given prefix.
        :return:
        """
        try:
            buckets = self.client.buckets.all()
            if prefix is None:
                for bucket in buckets:
                    response = bucket.objects.delete()
                    print(f'INFO: bucket.objects.delete() deleted {response} - {response}')
            else:
                r = re.compile(prefix)
                for bucket in (x for x in buckets if r.match(x.name)):
                    response = bucket.objects.delete()
                    print(f'INFO: bucket.objects.delete() deleted {response} - {response}')
            return print(f'Emptied s3 buckets {buckets.names}')
        except ClientError as e:
            print(f'ERROR: {e}')
            raise


class ECS:
    def __init__(self):
        self.client = Conn('ecs', 'client', region).connection()

    def turn_down_ecs_services(self):
        try:
            lclusters = self.client.list_clusters().get('clusterArns')
            for c in lclusters:
                lservices = self.client.list_services(
                    cluster=c,
                ).get('serviceArns')
                for s in lservices:
                    s = s.split("/")[-1]
                    self.client.update_service(
                        cluster=c,
                        desiredCount=0,
                        service=s,
                    )

                    try:
                        self.client.delete_service(
                            cluster=c,
                            service=s,
                        )

                    except ClientError as e:
                        print(f'ERROR: {e}')
                        raise

        except ClientError as e:
            print(f'ERROR: {e}')
            raise


class ECR:
    def __init__(self):
        self.client = Conn('ecr', 'client', region).connection()

    def empty_ecr_repositories(self):
        try:
            lrepos = self.client.describe_repositories().get('repositories')
            print(f'INFO: found {len(lrepos)} repositories')
            for r in lrepos:
                repo_name = r.get('repositoryName')
                limages = self.client.list_images(repositoryName=repo_name).get('imageIds')
                for i in limages:
                    tag = i.get('imageTag')
                    response = self.client.batch_delete_image(repositoryName=repo_name, imageIds=[{'imageTag': tag}])
                    print(f'INFO: deleted image {response.get("imageIds")[0]} from repository {repo_name}')
        except ClientError as e:
            print(f'ERROR: {e}')
            raise


class IAM:
    def __init__(self):
        self.client = Conn('iam', 'client', region).connection()

    def remove_roles_from_profiles(self, instance_profiles):
        pass


class CloudFront:
    def __init__(self):
        self.client = Conn('cloudfront', 'client', region).connection()

    @staticmethod
    def _update_something_across_cache_behaviors(distribution_ids):
        for d in distribution_ids:
            cb = d.get('CacheBehaviors').get('Items')
            for behavior in (y for y in d.get('CacheBehaviors').get('Items') if y.get('LambdaFunctionAssociations').get('Quantity') > 0):
                pass

    def _is_distro_ready(self, update_response):
        distro_id = update_response.get('Distribution').get('Id')
        distro_status = update_response.get('Distribution').get('Status')
        while distro_status is 'InProgress':
            try:
                distro_status = self.client.get_distribution(Id=distro_id).get('Distribution').get('Status')
            except ClientError as e:
                print(f'ERROR: {e}')
                raise
        return True

    def remove_cache_behavior(self):
        try:
            distrolist = self.client.list_distributions().get('DistributionList')
            if not distrolist.get('IsTruncated'):
                distros = distrolist.get('Items')
                for d in distros:
                    distro_id = d.get('Id')
                    get_config = self.client.get_distribution_config(Id=distro_id)
                    config = get_config.get('DistributionConfig')
                    etag = get_config.get('ETag')
                    print(f'INFO: going to use this distribution configuration as the request payload \n{config}')
                    config['CacheBehaviors'] = {
                        'Quantity': 0
                    }
                    response = self.client.update_distribution(
                        DistributionConfig=config,
                        Id=distro_id,
                        IfMatch=etag
                    )
                    print(f'{response}')

            else:
                print(f'ERROR: IsTruncated is true and this functionality is not built yet!')
                raise
        except ClientError as e:
            print(f'ERROR: {e}')
            raise


if __name__ == "__main__":
    region = "us-east-1"
    S3().empty_s3_buckets(prefix='policymap-')
    ECS().turn_down_ecs_services()
    ECR().empty_ecr_repositories()
    CloudFront().remove_cache_behavior()
