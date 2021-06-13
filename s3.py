import boto

class S3():
    def __init__(self):
        self.client = boto.Conn('s3', 'client').connection()

    def get_bucket_keys(self, *args):
        r = self.client.list_buckets()
        for b in r:
            r = self.client.get_bucket_encryption(Bucket=b)
            print(f'[INFO] Bucket: {b} has a key of {r["ServerSideEncryptionConfiguration"]["Rules"][0]["ApplyServerSideEncryptionByDefault"]["KMSMasterKeyID"]}')

    def get_buckets(self):
        r = self.client.list_buckets()
        print("Buckets: ")
        for b in r["Buckets"]:
            print(f'{b["Name"]} : {b["CreationDate"]}')

def main():
    s = S3()
    # s.get_bucket_keys('mt2-mhmd-storage')
    s.get_buckets()


if __name__ == "__main__":
    main()
