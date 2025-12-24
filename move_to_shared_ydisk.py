import requests
import urllib.parse
import time
import sys

# Настройки
TOKEN = 'your_oauth_token_here'  # Ваш OAuth токен
VD_HASH = 'your_virtual_disk_hash'  # hash общего диска, например 'abc123'
SOURCE_PATH = 'disk:/'  # Корень личного диска
TARGET_PATH = f'vd:{VD_HASH}:disk:/'  # Корень общего диска
BASE_URL = 'https://cloud-api.yandex.net/v1/disk'
HEADERS = {
    'Authorization': f'OAuth {TOKEN}',
    'Content-Type': 'application/json'
}

def api_call(method, endpoint, data=None):
    """Выполняет API-запрос с обработкой ошибок."""
    url = f'{BASE_URL}/{endpoint}'
    response = requests.request(method, url, headers=HEADERS, json=data)
    if response.status_code == 202:
        operation = response.json()['href']
        while True:
            op_status = requests.get(operation, headers=HEADERS).json()
            if op_status['status'] == 'success':
                return op_status
            time.sleep(1)
    response.raise_for_status()
    return response.json()

def list_resources(path, recursive=True):
    """Получает список ресурсов рекурсивно."""
    params = {'path': path, 'fields': '_embedded.items(name,path,type,size)', 'limit': 1000}
    if recursive:
        params['fields'] += ',_embedded.items(_embedded.items)'
    items = []
    while True:
        resp = api_call('GET', 'resources', params)
        items.extend(resp.get('_embedded', {}).get('items', []))
        if '_links' not in resp.get('_embedded', {}):
            break
        params['offset'] = resp['_embedded']['_links']['next']['offset']
    return items

def ensure_folder(path):
    """Создает папку, если не существует."""
    try:
        api_call('PUT', f'resources?path={urllib.parse.quote(path)}')
    except requests.exceptions.HTTPError as e:
        if e.response.status_code != 409:  # Уже существует
            raise

def move_resource(src_path, dst_path):
    """Перемещает ресурс."""
    data = {'from': src_path, 'path': dst_path, 'overwrite': 'true'}
    return api_call('POST', f'virtual-disks/resources/move', data)

def transfer_tree(items, base_src, base_dst):
    """Рекурсивно переносит структуру."""
    folders = {}
    for item in items:
        if item['type'] == 'dir':
            folders[item['path']] = item['name']
    
    # Создаем папки
    for folder_path in sorted(folders, reverse=True):
        rel_path = folder_path.replace(base_src, '', 1)
        target_folder = f'{base_dst}{rel_path}'
        ensure_folder(target_folder)
    
    # Перемещаем файлы
    for item in items:
        if item['type'] == 'file':
            rel_path = item['path'].replace(base_src, '', 1)
            target_file = f'{base_dst}{rel_path}'
            print(f'Перемещаю: {item["path"]} -> {target_file}')
            move_resource(item['path'], target_file)

def main():
    print('Получаю список файлов...')
    items = list_resources(SOURCE_PATH)
    print(f'Найдено {len(items)} элементов')
    
    transfer_tree(items, SOURCE_PATH, TARGET_PATH)
    print('Перенос завершен!')

if __name__ == '__main__':
    main()
