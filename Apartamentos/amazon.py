import boto3

class Amazon:

    def __init__(self, aws_key_id, aws_secret_key ) -> None:
        self.aws_key_id = aws_key_id
        self.aws_secret_key = aws_secret_key

    def getAwsSession(self):
        session = boto3.Session(aws_access_key_id=self.aws_key_id, aws_secret_access_key=self.aws_secret_key, region_name='us-east-2')
        return session
    
    def getS3Client(self):
        session = self.getAwsSession()
        s3 = session.client('s3')
        return s3
    
    def listObjectsFromBucket(self, bucket_name):
        s3 = self.getS3Client()
        response = s3.list_objects_v2(Bucket=bucket_name)
        return response
    
    def downloadS3File(self, bucket_name, file_name, save_as):
        s3 = self.getS3Client()
        s3.download_file(bucket_name, file_name, save_as)

    def uploadFile(self, bucket_name, file_name, s3_filename):
        s3 = self.getS3Client()
        s3.upload_file(file_name, bucket_name, s3_filename)


