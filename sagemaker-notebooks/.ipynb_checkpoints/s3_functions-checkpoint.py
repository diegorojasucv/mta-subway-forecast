import boto3
import pandas as pd
from io import StringIO

bucket_name = 'mta-subway'
s3_client = boto3.client('s3')

def read_s3_csv_to_dataframe(file_key):
    """Reads a CSV file from S3 and returns it as a pandas DataFrame.

    This function connects to an S3 bucket and reads a specified CSV file
    into a pandas DataFrame.

    Args:
        file_key (str): The S3 key (path) of the file to read.

    Returns:
        pd.DataFrame: A DataFrame containing the contents of the CSV file.

    Raises:
        Exception: If there is an error connecting to S3 or reading the file.
    """
    
    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=file_key)
        file_content = response['Body'].read().decode('utf-8')
        data = pd.read_csv(StringIO(file_content), parse_dates=["created_date"])
        return data
    except Exception as e:
        raise Exception(f"Error reading file {file_key} from S3: {e}")

def save_dataframe_to_s3(dataframe, bucket_name, file_key):
    """Saves a pandas DataFrame as a CSV file in an S3 bucket.

    This function converts a pandas DataFrame to a CSV string and uploads it
    to a specified S3 bucket.

    Args:
        dataframe (pd.DataFrame): The DataFrame to be saved.
        bucket_name (str): The name of the S3 bucket.
        file_key (str): The S3 key (path) where the CSV file will be saved.

    Raises:
        Exception: If there is an error uploading the file to S3.
    """
    csv_buffer = StringIO()
    dataframe.to_csv(csv_buffer, index=False)

    try:
        s3_client.put_object(
            Bucket=bucket_name,
            Key=file_key,
            Body=csv_buffer.getvalue()
        )
        print(f"DataFrame successfully saved to s3://{bucket_name}/{file_key}")
    except Exception as e:
        raise Exception(f"Error saving DataFrame to S3: {e}")