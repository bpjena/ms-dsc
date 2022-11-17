"""
This code was borrowed from
https://github.com/apache/airflow/blob/master/airflow/contrib/hooks/aws_hook.py
and has been updated accordingly to work for D-Eng
"""

import boto3
from python.util.base import BaseHook


class AwsHook(BaseHook):
    """
    Interact with AWS.
    This class is a thin wrapper around the boto3 python library.
    """

    SERVICE_NAME = "aws"

    def __init__(
        self,
        aws_access_key_id=None,
        aws_secret_access_key=None,
        aws_session_token=None,
        region_name="us-east-1",
        verify=None,
        context=None,
    ):
        super().__init__(context=context)

        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.aws_session_token = aws_session_token
        self.region_name = region_name
        self.verify = verify
        self._client = None
        self._resource = None

    @property
    def client(self):
        if self._client is None:
            client = self.get_client_type(self.SERVICE_NAME, self.region_name)
            self._client = client
        else:
            client = self._client
        return client

    def _get_credentials(self, region_name, endpoint_url=None):

        return (
            boto3.session.Session(
                aws_access_key_id=self.aws_access_key_id,
                aws_secret_access_key=self.aws_secret_access_key,
                aws_session_token=self.aws_session_token,
                region_name=region_name,
            ),
            endpoint_url,
        )

    def get_client_type(self, client_type, region_name=None, config=None):
        region_name = self.region_name if region_name is None else region_name
        return boto3.client(client_type, region_name=region_name, config=config, verify=self.verify)

    def get_caller_identity(self):
        return self.get_client_type("sts").get_caller_identity()

    def get_resource_type(self, resource_type, region_name=None, config=None):
        """ Get the underlying boto3 resource using boto3 session"""
        region_name = self.region_name if region_name is None else region_name
        return boto3.resource(
            resource_type, region_name=region_name, config=config, verify=self.verify
        )

    def get_session(self, region_name=None):
        """Get the underlying boto3.session."""
        session, _ = self._get_credentials(region_name)
        return session

    def get_credentials(self, region_name=None):
        """Get the underlying `botocore.Credentials` object.
        This contains the following authentication attributes: access_key, secret_key and token.
        """
        session, _ = self._get_credentials(region_name)
        # Credentials are refreshable, so accessing your access key and
        # secret key separately can lead to a race condition.
        # See https://stackoverflow.com/a/36291428/8283373
        return session.get_credentials().get_frozen_credentials()

    def expand_role(self, role):
        """
        If the IAM role is a role name, get the Amazon Resource Name (ARN) for the role.
        If IAM role is already an IAM role ARN, no change is made.
        :param role: IAM role name or ARN
        :return: IAM role ARN
        """
        if "/" in role:
            return role
        else:
            return self.get_client_type("iam").get_role(RoleName=role)["Role"]["Arn"]
