"""
このプログラムは、Azure Blobストレージの内容を全てローカルにダウンロードするためのものです。
以下の手順で動作します：
1. Azure Blob Storageに接続するための接続文字列を使用して、BlobServiceClientを作成します。
2. ストレージアカウント内の全てのコンテナを取得します。
3. 各コンテナ内のBlobをリストし、指定されたプレフィックスでフィルタリングします。
4. 各Blobをローカルディレクトリにダウンロードします。
5. ダウンロードしたBlobのプロパティを取得し、メタデータファイルとして保存します。
"""

from azure.storage.blob import BlobServiceClient
import os
import logging
import configparser

# 設定ファイルの読み込み
config = configparser.ConfigParser()
config.read('config.ini')

# Azure Blob Storageの接続文字列
connect_str = config['Azure']['ConnectionString']

# Blobストレージ名を指定
blob_storage_name = config['Azure']['BlobStorageName']

# ログ設定
log_file_path = f'./{blob_storage_name}_download_log.txt'
logging.basicConfig(filename=log_file_path, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Azure SDKの詳細なログを抑制
azure_logger = logging.getLogger('azure')
azure_logger.setLevel(logging.WARNING)

try:
    # BlobServiceClientを作成
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)

    # 全てのコンテナを取得
    containers = blob_service_client.list_containers()
    logging.info('全てのコンテナを取得しました。')
except Exception as e:
    logging.error(f'BlobServiceClientの作成またはコンテナの取得中にエラーが発生しました: {e}')
    exit(1)

# リトライ用のファイル
retry_file_path = f'./{blob_storage_name}_download_retry.txt'

# リトライ用のBlobリストを読み込む
retry_blobs = set()
if os.path.exists(retry_file_path):
    with open(retry_file_path, 'r') as retry_file:
        retry_blobs = set(line.strip() for line in retry_file)
else:
    open(retry_file_path, 'w').close()

# ダウンロード先のローカルディレクトリ
local_path = f'.\{blob_storage_name}'

if not os.path.exists(local_path):
    os.makedirs(local_path)

processed_blobs = 0

for container in containers:
    try:
        container_client = blob_service_client.get_container_client(container.name)
        
        # コンテナのアクセスレベルを確認
        container_properties = container_client.get_container_properties()
        if container_properties.public_access != 'container':
            logging.info(f"コンテナ '{container.name}' はパブリックアクセスが 'container' 以外のためスキップします。")
            continue
        
        blobs = container_client.list_blobs()
        logging.info(f"コンテナ '{container.name}' のBlobをリストしました。")
        
        # デバッグ用にBlobリストをログに出力
        # blob_names = [blob.name for blob in blobs]
        # logging.info(f'コンテナ '{container.name}' のBlobリスト: {blob_names}')
        
    except Exception as e:
        logging.error(f"コンテナ '{container.name}' の処理中にエラーが発生しました: {e}")
        continue
    
    for blob in blobs:
        try:
            blob_client = container_client.get_blob_client(blob)
            download_file_path = os.path.join(local_path, container.name, blob.name.replace('/', '\\'))
            
            # ローカルディレクトリを作成
            local_dir = os.path.dirname(download_file_path)
            os.makedirs(local_dir, exist_ok=True)
            
            with open(download_file_path, 'wb') as download_file:
                download_file.write(blob_client.download_blob().readall())
            
            # Blobのプロパティを取得して保存
            properties = blob_client.get_blob_properties()
            content_type = properties.content_settings.content_type
            with open(download_file_path + '.metadata', 'w') as metadata_file:
                metadata_file.write(f'Content-Type: {content_type}\n')
            
            if blob.name in retry_blobs:
                retry_blobs.remove(blob.name)
            logging.info(f"Blob '{blob.name}' をダウンロードし、メタデータを保存しました。")

        except Exception as e:
            logging.error(f"Blob '{blob.name}' のダウンロード中にエラーが発生しました: {e}")
            with open(retry_file_path, mode='a') as retry_file:
                retry_file.write(f'{blob.name}\n')
            continue
        
        processed_blobs += 1
        if processed_blobs % 100 == 0:
            print(f'{processed_blobs}...', end='', flush=True)

# リトライ用のファイルを更新
if retry_blobs:
    with open(retry_file_path, 'w') as retry_file:
        for blob_name in retry_blobs:
            retry_file.write(f'{blob_name}\n')
    logging.info('一部のBlobのダウンロードに失敗しました。リトライ用のファイルを更新しました。')
    print(f'一部のBlobのダウンロードに失敗しました。リトライ用のファイルを更新しました。詳細はログファイル「{log_file_path}」を確認してください。')
else:
    if os.path.exists(retry_file_path):
        os.remove(retry_file_path)
    logging.info('全てのBlobのダウンロードに成功しました。')
    print(f'全てのBlobのダウンロードに成功しました。詳細はログファイル「{log_file_path}」を確認してください。')
