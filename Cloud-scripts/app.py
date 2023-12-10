from flask import Flask, render_template, request, jsonify
from google.cloud import storage
import os

app = Flask(__name__)

# Replace with your GCS bucket name and path
GCS_BUCKET_NAME = 'your-gcs-bucket'
GCS_PATH = 'uploads/'

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/upload', methods=['POST'])
def upload():
    try:
        # Get the uploaded file and user ID
        file = request.files['fileInput']
        user_id = request.form['userID']

        if not file:
            return jsonify({'error': 'No file provided'})

        # Save the file to a temporary location
        temp_file_path = '/tmp/{}'.format(file.filename)
        file.save(temp_file_path)

        # Generate a unique filename based on user ID and the original filename
        unique_filename = f'{user_id}_{file.filename}'

        # Upload the file to GCS
        upload_to_gcs(temp_file_path, GCS_PATH + unique_filename)

        return jsonify({'message': 'Upload successful'})

    except Exception as e:
        return jsonify({'error': str(e)})

def upload_to_gcs(local_path, gcs_path):
    client = storage.Client()
    bucket = client.bucket(GCS_BUCKET_NAME)
    blob = bucket.blob(gcs_path)
    blob.upload_from_filename(local_path)

    # Delete the temporary local file
    os.remove(local_path)

if __name__ == '__main__':
    app.run(debug=True)
