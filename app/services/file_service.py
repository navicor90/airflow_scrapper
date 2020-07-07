import boto3
import os
import ntpath


class CloudFileService(object):
    def save(self, full_filename, cloud_path):
        raise Exception("Not implemented yet")


class S3CloudFileService(CloudFileService):
    session = boto3.Session(
        aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'],
        aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY']
    )

    def save(self, full_filename, cloud_path):
        s3 = self.session.resource('s3')
        bucket = 'inmocore-properties'
        filename = ntpath.basename(full_filename)
        s3_filename = f'{cloud_path}/{filename}'
        print(f"Uploading:{full_filename} to: {s3_filename}")
        s3.meta.client.upload_file(full_filename, bucket, s3_filename)
