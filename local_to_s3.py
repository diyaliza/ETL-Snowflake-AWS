import boto3
s3_resource = boto3.resource('s3','us-west-2')

def s3_upload(file_name, fold, bkt):
    s3_bucket = s3_resource.Bucket(name=bkt)
    if True:
        s3_bucket.upload_file(
            Filename = file_name,
            Key = fold + '/' + file_name
        )
    return True

if __name__=='__main__':
    file_name = 'Product_Dim.csv'
    s3_folder = 'raw_data'
    bucket = 'snowflake-project-etl'

    status = s3_upload(file_name, s3_folder, bucket)
    if(status):
        print('Data saved')
    else:
        print('Error')