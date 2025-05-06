import json
import boto3
import pymysql
import os

# Configuration
S3_BUCKET = "appbucket25"
RDS_ENDPOINT = "appdb.cbqqmkwsgaqb.us-east-2.rds.amazonaws.com"
RDS_USER = "admin"
RDS_PASSWORD = "taylorsfinalproject"
RDS_DBNAME = "appdb"
AWS_REGION = "us-east-2"
SNS_TOPIC_ARN = "arn:aws:sns:us-east-2:471112585115:newSNStest2"

# Initialize clients
s3_client = boto3.client('s3', region_name=AWS_REGION)
ses_client = boto3.client('ses', region_name=AWS_REGION)
sns_client = boto3.client('sns', region_name=AWS_REGION)

def get_db_connection():
    return pymysql.connect(
        host=RDS_ENDPOINT,
        user=RDS_USER,
        password=RDS_PASSWORD,
        database=RDS_DBNAME
    )

def send_sns_notification(message):
    sns_client.publish(TopicArn=SNS_TOPIC_ARN, Message=message)

def lambda_handler(event, context):
    filename = event['filename']
    emails = event['emails']
    file_url = event['file_url']
    recipient_email = event.get('recipient_email')  # Email of the person who clicked the link

    if recipient_email:
        # Track email clicks
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT clicked_emails, email_list FROM filemetadata WHERE filename = %s", (filename,))
        result = cur.fetchone()
        clicked_emails = json.loads(result[0])
        email_list = json.loads(result[1])

        if recipient_email not in clicked_emails:
            clicked_emails.append(recipient_email)
            cur.execute("UPDATE filemetadata SET clicked_emails = %s WHERE filename = %s", (json.dumps(clicked_emails), filename))
            conn.commit()

        if set(clicked_emails) == set(email_list):
            # All recipients have accessed the file, delete it
            s3_client.delete_object(Bucket=S3_BUCKET, Key=f'files/{filename}')
            cur.execute("DELETE FROM filemetadata WHERE filename = %s", (filename,))
            conn.commit()
            
            # Send SNS notification for file deletion
            message = f"File '{filename}' has been deleted from S3 after all recipients accessed it."
            send_sns_notification(message)

        conn.close()
        return {
            'statusCode': 200,
            'body': json.dumps('File access updated successfully')
        }
    else:
        # Send email with file link
        subject = f"File '{filename}' has been uploaded"
        body_text = f"You can access the file '{filename}' using the following link: {file_url}"
        body_html = f"""
        <html>
        <head></head>
        <body>
          <h1>File '{filename}' has been uploaded</h1>
          <p>You can access the file using the following link: <a href="{file_url}">{file_url}</a></p>
        </body>
        </html>
        """
        for email in emails:
            ses_client.send_email(
                Source=os.environ['SENDER_EMAIL'],
                Destination={'ToAddresses': [email]},
                Message={
                    'Subject': {'Data': subject},
                    'Body': {
                        'Text': {'Data': body_text},
                        'Html': {'Data': body_html}
                    }
                }
            )

        # Send SNS notification for file upload
        message = f"File '{filename}' has been uploaded to S3 and shared with: {', '.join(emails)}"
        send_sns_notification(message)

        return {
            'statusCode': 200,
            'body': json.dumps('Emails sent successfully!')
        }
