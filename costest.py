from libcloud.storage.types import Provider
from libcloud.storage.providers import get_driver
import os
import requests

def gen_file(path, size, attach_size=0):
    _file = open(path, 'w')
    _file.seek(1024 * 1024 * size + attach_size - 3)
    _file.write('cos')
    _file.close()


def gen_file_small(path, size):
    _file = open(path, 'w')
    _file.write('x'*size)
    _file.close()


cls = get_driver(Provider.TENCENT_COS)

cos_bucket = "examplebucket-1250000000" 
cos_endpoint = 'cos.ap-beijing.myqcloud.com'
cos_secret_id = "AKID**"
cos_secret_key = "**"

cos_bucket =  os.environ["COS_DEV_BUCKET"]
cos_endpoint = 'cos.%s.myqcloud.com' % os.environ["COS_DEV_REGION"]
cos_secret_id = os.environ["COS_DEV_SECRET_ID"]
cos_secret_key = os.environ["COS_DEV_SECRET_KEY"]

driver = cls(key=cos_secret_id, secret=cos_secret_key, host=cos_endpoint)

try:
    driver.create_container(container_name=cos_bucket)
except Exception as e:
    print(e)

container = driver.get_container(container_name=cos_bucket)

# 生成50MB的文件测试分块上传下载
file_path = 'testfile_50MB'
gen_file(file_path, 50)

print("=== upload_object_via_stream")
with open(file_path, "rb") as iterator:
    obj = driver.upload_object_via_stream(
        iterator=iterator, container=container, object_name=file_path
    )
    obj = driver.get_object(container_name=cos_bucket, object_name=file_path)
    success = driver.download_object(obj=obj, destination_path=file_path, overwrite_existing=True)
    assert success

print("=== upload_object")
obj = driver.upload_object(file_path=file_path, container=container, object_name=file_path)

print("=== get_object_cdn_url")
obj = driver.get_object(container_name=cos_bucket, object_name=file_path)
url = driver.get_object_cdn_url(obj=obj)
print(url)

resp = requests.get(url=url)
print(resp.headers)

# data_stream = driver.download_object_as_stream(obj=obj)
# for data in data_stream:
#     print(data)

print("=== download_object_range")
success = driver.download_object_range(obj=obj, destination_path=file_path, start_bytes=0, end_bytes=100, overwrite_existing=True)
assert success

print("=== download_object_range_as_stream")
data_stream = driver.download_object_range_as_stream(obj=obj, start_bytes=0, end_bytes=100)
for data in data_stream:
    print(data)

print("=== delete_object")
success = driver.delete_object(obj=obj)
if success:
    print("delete_object success")
else:
    print("delete_object failure")

if os.path.exists(file_path):
    os.remove(file_path)

# 生成100B的文件测试简单上传下载
file_path = 'testfile_100B'
gen_file_small(file_path, 100)

print("=== upload_object_via_stream")
with open(file_path, "rb") as iterator:
    obj = driver.upload_object_via_stream(
        iterator=iterator, container=container, object_name=file_path
    )
    print("upload_object success")
    obj = driver.get_object(container_name=cos_bucket, object_name=file_path)
    success = driver.download_object(obj=obj, destination_path=file_path, overwrite_existing=True)
    if success:
        print("download_object success")
    else:
        print("download_object failure")
    
print("=== upload_object")
obj = driver.upload_object(file_path=file_path, container=container, object_name=file_path)

print("=== get_object_cdn_url")
obj = driver.get_object(container_name=cos_bucket, object_name=file_path)
url = driver.get_object_cdn_url(obj=obj)
print(url)

resp = requests.get(url=url)
print(resp.headers)

print("=== delete_object")
success = driver.delete_object(obj=obj)
if success:
    print("delete_object success")
else:
    print("delete_object failure")

if os.path.exists(file_path):
    os.remove(file_path)

# 查询对象列表
print('=== list objects')
obj_iter = driver.iterate_container_objects(container=container, prefix="test")
for obj in obj_iter:
    print(obj)

# 查询桶列表
print('=== list buckets')
container_iter = driver.iterate_containers()
for container in container_iter:
    print(container)

