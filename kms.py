import csv

import re

from botocore.exceptions import ClientError

import boto


def get_keys_from_file(file):
    """
    Reads a file and creates a list of dictionaries.
    :param file: Filename relative to project root.
    :return: lkeys - a list of dictionaries
        [{
            Key: '00484545-2000-4111-9000-611111111111',
            Region: 'us-east-1'
        },
        {
            Key: '00484545-2000-4111-9000-622222222222',
            Region: 'us-east-1'
        }]
    """
    lkeys = []
    regex = '[0-9A-Za-z]{8}-[0-9A-Za-z]{4}-4[0-9A-Za-z]{3}-[89ABab][0-9A-Za-z]{3}-[0-9A-Za-z]{12}'
    with open(file, newline='') as csvfile:
        reader = csv.reader(csvfile, delimiter=' ', quotechar='|')
        for row in reader:
            if len(row) > 3 and re.search(regex, row[2]):
                lkeys.append({'Key': row[2], 'Region': row[0]})
        return lkeys


class Kms:
    def __init__(self):
        self.client = boto.Conn('kms', 'client').connection()

    def bulk_rotate_keys(self, keys):
        """
        Takes a list dictionary and updates keys to rotate every year
        :param keys: A list of KMS keys with respective region
        :return: None
        """
        for k in keys:
            region = k.get('Region').replace(':', '')
            if self.client.meta.region_name != region:
                self.client = boto.Conn('kms', 'client', region).connection()
            try:
                r = self.client.enable_key_rotation(
                    KeyId=k.get('Key')
                )
                print(f'Response: {r}')
            except ClientError as e:
                print(e)


def main():
    cmk_keys = get_keys_from_file('tractmanager-mt2-prod.csv')
    k = Kms()
    try:
        k.bulk_rotate_keys(cmk_keys)
    except SystemExit as e:
        print(e)


if __name__ == "__main__":
    main()
