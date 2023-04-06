import os
import json
import tempfile
import pandas as pd
from flask import Flask, request
from google.cloud import storage

app = Flask(__name__)

# Cloud Run service to convert CSV to compressed Parquet
@app.route('/', methods=['POST'])
def csv_to_parquet():
    content_type = request.headers.get('Content-Type')
    if(content_type=='application/json'):
        gcs_json = json.loads(request.data)
    else:
        return "Checking for JSON failed"

    file_path = gcs_json['file_path']
    file_name = gcs_json['file_name']
    bucket_name = gcs_json['bucket_name']

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_name)

    _, temp_local_filename = tempfile.mkstemp()
    blob.download_to_filename(temp_local_filename)

    print('File downloaded to {}.'.format(temp_local_filename))

    df = pd.read_csv(temp_local_filename)

    df['full_name'] = df['First Name'] + ' ' + df['Last Name']
    df['total_marks'] = df['Maths'] + df['Science'] + df['English'] + df['History']

    _, converted_file_name = tempfile.mkstemp(suffix=".parquet")

    df.to_parquet(
        converted_file_name,
        index=False,
        compression="gzip"
    )

    print('File converted to compressed Parquet format.')

    # Upload the converted Parquet file to GCS
    converted_bucket_name = "bucket-to-store-parquet-in-gcp"
    converted_blob_name = os.path.splitext(file_name)[0] + '.parquet'
    converted_file_path = "gs://{}/{}".format(converted_bucket_name, converted_blob_name)

    print('Uploading converted file to {}.'.format(converted_file_path))

    converted_bucket = storage_client.bucket(converted_bucket_name)
    converted_blob = converted_bucket.blob(converted_blob_name)
    converted_blob.upload_from_filename(converted_file_name)

    print('File uploaded to {} contents from {}'.format(converted_file_path, converted_file_name))

    # Delete the temporary file
    os.remove(temp_local_filename)
    os.remove(converted_file_name)

    return "Success"


if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))
    app.run(debug=False, host='0.0.0.0', port=port)
