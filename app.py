import pymysql
from flask import Flask, request, redirect, url_for, render_template
import boto3
import os

app = Flask(__name__)

# Database configuration
RDS_HOST = 'appdb.cbqqmkwsgaqb.us-east-2.rds.amazonaws.com'
RDS_USER = 'admin'
RDS_PASSWORD = 'taylorsfinalproject'
RDS_DB = 'appdb'

# S3 configuration
S3_BUCKET = 'appbucket25'

# SNS configuration
SNS_TOPIC_ARN = 'arn:aws:sns:us-east-2:471112585115:appsns'
AWS_REGION = 'us-east-2'  # e.g., 'us-west-2'

# Initialize AWS clients
s3_client = boto3.client('s3', region_name=AWS_REGION)
sns_client = boto3.client('sns', region_name=AWS_REGION)

def create_table_if_not_exists():
    """Create the table if it doesn't exist."""
    conn = pymysql.connect(host=RDS_HOST, user=RDS_USER, password=RDS_PASSWORD, db=RDS_DB)
    try:
        with conn.cursor() as cur:
            cur.execute("""
            CREATE TABLE IF NOT EXISTS uploaded_files (
                id INT AUTO_INCREMENT PRIMARY KEY,
                filename VARCHAR(255) NOT NULL,
                s3_url TEXT NOT NULL,
                email_sent BOOLEAN DEFAULT FALSE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """)
        conn.commit()
    finally:
        conn.close()

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/upload', methods=['POST'])
def upload():
    # Ensure the table exists
    create_table_if_not_exists()

    file = request.files['file']
    filename = file.filename
    file_path = os.path.join('/tmp', filename)
    file.save(file_path)

    # Upload file to S3
    s3_client.upload_file(file_path, S3_BUCKET, "files/" + filename)
    
    # Generate S3 URL
    s3_url = f"https://{S3_BUCKET}.s3.amazonaws.com/files/{filename}"

    # Store file info in RDS
    conn = pymysql.connect(host=RDS_HOST, user=RDS_USER, password=RDS_PASSWORD, db=RDS_DB)
    try:
        with conn.cursor() as cur:
            cur.execute("""
            INSERT INTO uploaded_files (filename, s3_url) VALUES (%s, %s);
            """, (filename, s3_url))
        conn.commit()
    finally:
        conn.close()

    # Send notification email via SNS
    sns_message = f"A new file has been uploaded. You can access it here: {s3_url}"
    sns_client.publish(TopicArn=SNS_TOPIC_ARN, Message=sns_message, Subject='File Uploaded Notification')

    return redirect(url_for('index'))

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
