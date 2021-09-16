import logging
import subprocess
from difflib import SequenceMatcher

import boto3
import json

from botocore.config import Config
from botocore.exceptions import ClientError

regions = ['us-west-2', 'us-west-1', 'us-east-2', 'us-east-1']


class BCOLORS:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'


class AWSCONNECTION:
    def __init__(self, service_type: str, service_name: str, region=None) -> None:
        if service_type == "client":
            self.connection = boto3.client(service_name, region_name=region)

    def get_connection(self):
        return self.connection


class LAMBDAS:
    def __init__(self, connection):
        self.connection = connection
        self.parameters = {
            "FunctionVersion": 'ALL'
        }
        self.lambda_functions_found = []

    def make_request(self):
        response = self.get_lambdas(**self.parameters)

        if "NextMarker" in [_x for _x in response]:
            for functions in response["Functions"]:
                self.lambda_functions_found.append(functions)

            self.parameters["Marker"] = response["NextMarker"]

            self.make_request()
        else:
            for functions in response["Functions"]:
                self.lambda_functions_found.append(functions)

        return self.lambda_functions_found

    def get_lambdas(self, **kwargs) -> dict:
        response = self.connection.list_functions(
            **kwargs
        )

        return response


class Organizations:
    member_accounts = []

    def __init__(self, connection):
        self.connection = connection

    def get_members(self, **kwargs):
        response = self.connection.list_accounts(**kwargs)

        if 'NextToken' in [_x for _x in response]:
            for accounts in response['Accounts']:
                self.member_accounts.append(accounts)
            kwargs['NextToken'] = response['NextToken']
            self.get_members()
        else:
            for accounts in response['Accounts']:
                self.member_accounts.append(accounts)

        return self.member_accounts


class STS:
    def __init__(self, connection):
        self.connection = connection

    def assume_role(self, **kwargs):
        response = {}
        try:
            response = self.connection.assume_role(**kwargs)
        except ClientError as e:
            if e.response['Error']['Code'] == 'AccessDenied':
                print(f'{BCOLORS.WARNING}Cannot assume role{BCOLORS.ENDC}:{e}')
                return response
            else:
                raise e
        return response


def main():
    aws_lambda = AWSCONNECTION("client", "lambda", 'us-west-2')
    aws_orgs = AWSCONNECTION("client", "organizations")
    aws_sts = AWSCONNECTION('client', 'sts')
    org_functions = []
    gh_functions = []

    # GitHub CLI
    try:
        print(f'{BCOLORS.BOLD}Checking GitHub CLI availability...{BCOLORS.ENDC}')
        gh_process = subprocess.Popen(['gh', 'auth', 'status'],
                                      stdout=subprocess.PIPE,
                                      universal_newlines=True)
        out = gh_process.stdout.readlines()
        return_code = gh_process.poll()
        if return_code == 1:
            raise NameError(f'Please authenticate via the GH CLI')
        gh_process = subprocess.Popen(['gh', 'repo', 'list', 'newseasons',
                                       '-L', '100',
                                       '--json', 'name',
                                       '--json', 'primaryLanguage',
                                       '--json', 'updatedAt',
                                       '--json', 'pushedAt'],
                                      stdout=subprocess.PIPE,
                                      universal_newlines=True)
        out = gh_process.stdout.readlines()
        return_code = gh_process.poll()
        if return_code == 1:
            raise NameError(f'GH CLI returned an exit code')

        repos = json.loads(out[0])
    except SystemError as e:
        raise e

    # Get accounts
    org = Organizations(aws_orgs.get_connection())
    accounts = org.get_members()
    for account in accounts:
        print(f'Account: {account}\n')

        # assume OAA role
        sts = STS(aws_sts.get_connection())
        role = sts.assume_role(
            RoleArn=f'arn:aws:iam::{account["Id"]}:role/LambdaReadOnly',
            RoleSessionName='get-lambdas-python')
        if role == {}:
            continue

        # for each region get lambdas
        for region in regions:
            print(f'\t Region: {region}\n')
            try:
                config = Config(
                    retries={
                        'max_attempts': 10,
                        'mode': 'standard'
                    }
                )
                client = boto3.client(
                    service_name='lambda',
                    aws_access_key_id=role['Credentials']['AccessKeyId'],
                    aws_secret_access_key=role['Credentials']['SecretAccessKey'],
                    aws_session_token=role['Credentials']['SessionToken'],
                    region_name=region,
                    config=config,
                )

                # Get Lambdas
                lambdas = LAMBDAS(client)
                functions = lambdas.make_request()

                if len(functions) >= 1:
                    print(f'\t\tFunctions: ({len(functions)})')
                    for f in functions:
                        gh_details = []
                        for r in repos:
                            ratio = 0.0
                            try:

                                s = SequenceMatcher(lambda x: x == " ",
                                                    r['name'],
                                                    f['FunctionName'])
                                ratio = s.ratio()
                                if ratio >= 0.6:
                                    gh_details.append({
                                        'MatchRatio': ratio,
                                        'RepoName': r['name'],
                                        'PrimaryLanguage': r['primaryLanguage']['name'],
                                        'UpdatedAt': r['updatedAt'],
                                        'PushedAt': r['pushedAt']
                                    })

                                    continue

                            except TypeError as e:
                                if e.args[0] == "'NoneType' object is not subscriptable":
                                    gh_details.append({
                                        'MatchRatio': ratio,
                                        'RepoName': r['name'],
                                        'PrimaryLanguage': None,
                                        'UpdatedAt': r['updatedAt'],
                                        'PushedAt': r['pushedAt']
                                    })
                                    continue
                                else:
                                    print(f'{BCOLORS.WARNING}{e}{BCOLORS.ENDC}')
                                    continue

                        f['GHRepos'] = gh_details

                        print(f'\t\t {BCOLORS.OKCYAN}FunctionName={BCOLORS.ENDC}{f["FunctionName"]} '
                              f'{BCOLORS.OKCYAN}Runtime={BCOLORS.ENDC}{f["Runtime"]} '
                              f'{BCOLORS.OKCYAN}Version={BCOLORS.ENDC}{f["Version"]} '
                              f'{BCOLORS.OKCYAN}LastModified={BCOLORS.ENDC}{f["LastModified"]} '
                              f'{BCOLORS.OKCYAN}GHRepos={BCOLORS.ENDC}{f["GHRepos"]}')

                        org_functions.append(f)
                        if len(f['GHRepos']):
                            gh_functions.append(f)

            except ClientError as e:
                raise e

    with open('GFH-Functions.txt', 'w') as f:
        f.write(json.dumps(gh_functions))
    return org_functions


if __name__ == '__main__':
    main()
