import os

from datetime import datetime, timezone
from os import path
from subprocess import run
from sys import exit
from time import sleep

import boto3
from mysql import connector

# BEGIN: Configuration
TEMP_PATH = ''

AWS_ACCESS_KEY = ''
AWS_ACCESS_SECRET = ''
AWS_S3_BUCKET = ''
AWS_S3_PREFIX = ''

MYSQL_CONTAINER = ''
MYSQL_HOST = ''
MYSQL_PORT = '3306'
MYSQL_ROOT_PASSWORD = ''
# END: Configuration

def get_connection():
    connection = connector.connect(
        user='root',
        password=MYSQL_ROOT_PASSWORD,
        host=MYSQL_HOST,
        port=MYSQL_PORT)
    return connection

def flush_logs():
    connection = get_connection()
    try:
        cursor = connection.cursor(buffered=True)
        cursor.execute('FLUSH LOGS')
        return True
    finally:
        if connection:
            connection.close()
    
    return False

def find_backup_files():
    connection = get_connection()
    try:
        cursor = connection.cursor(buffered=True)
        cursor.execute('SHOW BINARY LOGS')
        all_logs = cursor.fetchall()
        
        cursor = connection.cursor(buffered=True)
        cursor.execute('SHOW MASTER STATUS')
        current_log = cursor.fetchone()
        current_log = current_log[0]
        
        return [item[0] for item in all_logs if item[0] != current_log]
    finally:
        if connection:
            connection.close()
    
    return None

def prune_logs():
    connection = get_connection()
    try:
        cursor = connection.cursor(buffered=True)
        cursor.execute('PURGE BINARY LOGS BEFORE NOW()')
        return True
    finally:
        if connection:
            connection.close()
    
    return False

def upload_s3(files):
    s3_client = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY, aws_secret_access_key=AWS_ACCESS_SECRET)
    for log_file in files:
        object_name = '{0}/{1}/{2}'.format(AWS_S3_PREFIX, datetime.now(timezone.utc).strftime("%Y/%m/%d"), log_file)
        file_name = '{0}/{1}'.format(TEMP_PATH, log_file)
        print('Uploading {0} to s3://{1}/{2}'.format(file_name, AWS_S3_BUCKET, object_name))
        
        try:
            s3_client.upload_file(file_name, AWS_S3_BUCKET, object_name)
        except ClientError as e:
            print('Failed to upload {0}'.format(file_name))
            return False
    
    return True

def main():
    try:
        os.mkdir(TEMP_PATH)
    except OSError as e:
        print(e)
    
    print('Flushing logs')
    if not flush_logs():
        print('Unable to flush logs')
        exit(1)
    
    print('Finding logs to backup')
    logs = find_backup_files()
    if not logs:
        print('Unable to determine logs to backup')
        exit(1)
    
    print('Copying log files to local directory')
    for log_file in logs:
        result = run(['docker', 'cp', '{0}:/var/lib/mysql/{1}'.format(MYSQL_CONTAINER, log_file), '{0}/'.format(TEMP_PATH)])
        if result.returncode != 0:
            print('Unable to copy \'{0}\'.'.format(log_file))
            exit(1)
    
    print('Uploading log files')
    upload_result = upload_s3(logs)
    
    print('Removing copied log files from local')
    for log_file in logs:
        os.unlink(path.join(TEMP_PATH, log_file))
    
    if upload_result:
        print('Removing backed up log files from mysql')
        sleep(2)
        prune_logs()
    else:
        print('Not removing log files from mysql due to upload failure')
        exit(1)

if __name__ == '__main__':
    main()

