import boto3
import botocore
import os
from time import sleep, time
from collections import namedtuple

AWSVolume = namedtuple('AWSVolume', ['id', 'vtype', 'size', 'attachments'])
GP3Config = namedtuple('GP3Config', ['iops', 'throughput'])


def newGP3Config(iops: int = 3000, throughput: int = 125) -> GP3Config:
    return GP3Config(
        iops=iops,
        throughput=throughput
    )


class GPConverter:
    def __init__(self, GP3Config: GP3Config, ExcludeVpcs: list = [], OnlyVpcs: list = []):
        self.tokens = 200
        self.converted = []
        self.failures = []
        self.start_time = time()
        self.excluded_vpcs = ExcludeVpcs
        self.only_vpcs = OnlyVpcs
        self.GP3Config = GP3Config

        if len(self.only_vpcs) > 0 and len(self.excluded_vpcs) > 0:
            raise ID10TException(
                """
                You are using both 'only' and 'exclude' VPC filters.
                Perhaps rethink your approach.
                """
            )

        try:
            self.region = os.environ["GP_REGION"]
        except KeyError:
            raise MissingVariableException(
                """
                Missing required environment variable(s):
                    GP_REGION
                """
            )

        self.client = boto3.client("ec2", self.region)
        self._get_volumes()._find_gp2()._filter_vpcs()

    def _get_volumes(self) -> object:
        result = self.client.describe_volumes()
        r = list()

        if len(result["Volumes"]) > 0:
            for volume in result["Volumes"]:
                r.append(
                    AWSVolume(
                        volume["VolumeId"],
                        volume["VolumeType"],
                        volume["Size"],
                        volume["Attachments"]
                    )
                )

        self._volumes = r

        return self

    def _find_gp2(self) -> object:
        if len(self._volumes) == 0:
            return self

        r = list()

        for vol in self._volumes:
            if vol.vtype == "gp2":
                r.append(vol)

        self._volumes = r

        return self

    def _exclude_vpcs(self) -> object:
        if len(self.excluded_vpcs) == 0:
            return self

        if len(self._volumes) == 0:
            return self

        r = list()
        iids = list()
        # map instance id to list of volumes
        ivmap = dict()

        for vol in self._volumes:
            if len(vol.attachments) > 0:
                iid = vol.attachments[0]['InstanceId']

                if iid in ivmap:
                    ivmap[iid].append(vol)
                else:
                    ivmap[iid] = [vol]

                iids.append(iid)

        ir = self.client.describe_instances(
            InstanceIds=iids
        )
        instances = ir['Reservations'][0]['Instances']

        for instance in instances:
            if instance['VpcId'] in self.excluded_vpcs:
                continue
            else:
                r.extend(ivmap[instance['InstanceId']])

        self._volumes = r

        return self

    # TODO: Implement
    def _only_vpcs(self) -> object:
        if len(self.only_vpcs) == 0:
            return self

        if len(self._volumes) == 0:
            return self

        r = list()
        iids = list()
        # map instance id to list of volumes
        ivmap = dict()

        for vol in self._volumes:
            if len(vol.attachments) > 0:
                iid = vol.attachments[0]['InstanceId']

                if iid in ivmap:
                    ivmap[iid].append(vol)
                else:
                    ivmap[iid] = [vol]

                iids.append(iid)

        ir = self.client.describe_instances(
            InstanceIds=iids
        )
        instances = ir['Reservations'][0]['Instances']

        for instance in instances:
            if instance['VpcId'] in self.only_vpcs:
                r.extend(ivmap[instance['InstanceId']])
            else:
                continue

        self._volumes = r

        return self

    def _filter_vpcs(self) -> object:
        return self._exclude_vpcs()._only_vpcs()

    @property
    def volumes(self) -> list:
        return self._volumes

    def _handle_out_of_tokens(self):
        # capture duration script has been running
        # add 5 tokens per second run with margin for error
        seconds_run = int(time() - self.start_time)
        self.tokens += int(seconds_run * 0.8) * 5
        # wait 1 second, add 5 more
        sleep(1)
        self.tokens += 5
        # reset start time
        self.start_time = time()

    def _handle_iteration(self, index: int) -> tuple:
        self.tokens -= 1
        index += 1

        if index + 1 == len(self._volumes):
            return True, index
        else:
            return False, index

    def convert_volumes(self):
        if len(self._volumes) == 0:
            print("Nothing to do!")
            return

        done = False
        index = 0

        while self.tokens > 0:
            print("Modifying {}...".format(self._volumes[index]))
            try:
                result = self.client.modify_volume(
                    VolumeId=self._volumes[index].id,
                    VolumeType='gp3',
                    Iops=self.GP3Config.iops,
                    Throughput=self.GP3Config.throughput
                )

                done, index = self._handle_iteration(index)
                if done:
                    break

                if self.tokens == 0:
                    self._handle_out_of_tokens()
            except botocore.exceptions.ClientError as error:
                code = error.response['Error']['Code']
                print("[ERROR] {}".format(code))

                self.failures.append(self._volumes[index])

                done, index = self._handle_iteration(index)
                if done:
                    break

                if self.tokens == 0:
                    self._handle_out_of_tokens()

                continue

        if done:
            return


class MissingVariableException(Exception):
    pass


class ID10TException(Exception):
    pass

# gc = GPConverter(
#     GP3Config = newGP3Config(),
#     ExcludeVpcs = [
#         "vpc-00dd9ef731f6a8c4a"
#     ]
# )

# gc = GPConverter(
#     GP3Config = newGP3Config(),
#     OnlyVpcs = [
#         "vpc-00dd9ef731f6a8c4a"
#     ]
# )

# gc.convert_volumes()