"""
このプログラムは、Azure BlobストレージとAmazon S3の内容を比較するためのものです。
以下の手順で動作します：
1. Azure BlobストレージとAmazon S3のファイルリストを取得します。
2. 各ファイルの存在とContent-Typeを比較します。
3. 違いがある場合はログに記録します。
"""

from azure.storage.blob import BlobServiceClient
import boto3
import logging
import configparser

# 設定ファイルの読み込み
config = configparser.ConfigParser()
config.read('config.ini')

# Azure Blob Storageの設定
azure_connect_str = config['Azure']['ConnectionString']
azure_blob_storage_name = config['Azure']['BlobStorageName']

# AWS S3の設定
s3_bucket_name = config['AWS']['S3BucketName']
aws_access_key_id = config['AWS']['AccessKeyId']
aws_secret_access_key = config['AWS']['SecretAccessKey']
aws_region = config['AWS']['Region']

# ログ設定
log_file_path = f'./{s3_bucket_name}_compare_log.txt'
open(log_file_path, 'w').close()  # ログファイルを空にする
logging.basicConfig(filename=log_file_path, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Azure SDKの詳細なログを抑制
azure_logger = logging.getLogger('azure')
azure_logger.setLevel(logging.WARNING)

# Azure Blob Storageクライアントの初期化
try:
    blob_service_client = BlobServiceClient.from_connection_string(azure_connect_str)
    logging.info('Azure Blob Storageクライアントの初期化に成功しました。')
except Exception as e:
    logging.error(f'Azure Blob Storageクライアントの初期化中にエラーが発生しました: {e}')
    exit(1)

# AWS S3クライアントの初期化
try:
    s3_client = boto3.client(
        's3',
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        region_name=aws_region
    )
    logging.info('S3クライアントの初期化に成功しました。')
except Exception as e:
    logging.error(f'S3クライアントの初期化中にエラーが発生しました: {e}')
    exit(1)

# Azure Blobストレージのファイルリストを取得
azure_blobs = {}
processed_blobs = 0
print(f'Azure Blob: {processed_blobs}...', end='', flush=True)
try:
    containers = blob_service_client.list_containers()
    for container in containers:
        container_client = blob_service_client.get_container_client(container.name)

        # コンテナのアクセスレベルを確認
        container_properties = container_client.get_container_properties()
        if container_properties.public_access != 'container':
            logging.info(f"コンテナ '{container.name}' はパブリックアクセスが 'container' 以外のためスキップします。")
            continue

        blobs = container_client.list_blobs()
        for blob in blobs:
            blob_client = container_client.get_blob_client(blob)
            properties = blob_client.get_blob_properties()
            content_type = properties.content_settings.content_type
            azure_blobs[f'{container.name}/{blob.name}'] = content_type
            
            processed_blobs += 1
            if processed_blobs % 100 == 0:
                print(f'{processed_blobs}...', end='', flush=True)
    logging.info('Azure Blobストレージのファイルリストを取得しました。')
except Exception as e:
    logging.error(f'Azure Blobストレージのファイルリスト取得中にエラーが発生しました: {e}')
    exit(1)

# AWS S3のファイルリストを取得（ページネーション対応）
s3_blobs = {}
processed_objects = 0
print(f'AWS S3: {processed_objects}...', end='', flush=True)
try:
    paginator = s3_client.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=s3_bucket_name):
        for obj in page.get('Contents', []):
            s3_key = obj['Key']
            head_response = s3_client.head_object(Bucket=s3_bucket_name, Key=s3_key)
            content_type = head_response['ContentType']
            s3_blobs[s3_key] = content_type
            
            processed_objects += 1
            if processed_objects % 100 == 0:
                print(f'{processed_objects}...', end='', flush=True)
    logging.info('AWS S3のファイルリストを取得しました。')
except Exception as e:
    logging.error(f'AWS S3のファイルリスト取得中にエラーが発生しました: {e}')
    exit(1)

# azure_blobsとs3_blobsをソートしてファイルに書き出す
azure_blobs_file_path = './list_azure_blobs.txt'
s3_blobs_file_path = './list_s3_blobs.txt'

with open(azure_blobs_file_path, 'w') as azure_file:
    for blob_name in sorted(azure_blobs.keys()):
        azure_file.write(f'{blob_name}: {azure_blobs[blob_name]}\n')

with open(s3_blobs_file_path, 'w') as s3_file:
    for s3_key in sorted(s3_blobs.keys()):
        s3_file.write(f'{s3_key}: {s3_blobs[s3_key]}\n')

# ファイルの存在とContent-Typeを比較
all_success = True
processed_compare = 0
print(f'Comparing: {processed_compare}...', end='', flush=True)
for blob_name, azure_content_type in azure_blobs.items():
    s3_key = blob_name.replace('$root/', '')
    if s3_key not in s3_blobs:
        logging.error(f'ファイルがS3に存在しません: {s3_key}')
        all_success = False
    elif s3_blobs[s3_key] != azure_content_type:
        logging.error(f'Content-Typeが一致しません: {s3_key}, Azure: {azure_content_type}, S3: {s3_blobs[s3_key]}')
        all_success = False
    
    processed_compare += 1
    if processed_compare % 100 == 0:
        print(f'{processed_compare}...', end='', flush=True)

# for s3_key in s3_blobs:
#     if s3_key not in azure_blobs:
#         logging.error(f'ファイルがAzure Blobストレージに存在しません: {s3_key}')
#         all_success = False

if all_success:
    logging.info('比較が完了しました。全て成功しました。')
    print(f'比較が完了しました。全て成功しました。詳細はログファイル「{log_file_path}」を確認してください。')
else:
    logging.info('比較が完了しました。一部が不一致でした。')
    print(f'比較が完了しました。一部が不一致でした。詳細はログファイル「{log_file_path}」を確認してください。')
