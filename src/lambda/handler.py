import json, os, time, boto3, gzip, io
s3 = boto3.client('s3')
def _gzip_bytes(data: bytes) -> bytes:
    buf = io.BytesIO()
    with gzip.GzipFile(fileobj=buf, mode='wb') as gz:
        gz.write(data)
    return buf.getvalue()
def handler(event, context):
    log_bucket = os.environ.get('LOG_BUCKET') or os.environ.get('S3_BUCKET')
    log_prefix = os.environ.get('LOG_PREFIX', 'logs/lambda')
    ts = int(time.time() * 1000)
    recs = []
    for r in event.get('Records', []):
        s3e = r.get('s3', {})
        recs.append({'bucket': s3e.get('bucket', {}).get('name'), 'key': s3e.get('object', {}).get('key'), 'ts': ts})
    payload = json.dumps({'records': recs}).encode('utf-8')
    k = f"{log_prefix}/event_{ts}.json.gz"
    s3.put_object(Bucket=log_bucket, Key=k, Body=_gzip_bytes(payload))
    return {'ok': True, 'log_object': f's3://{log_bucket}/{k}', 'count': len(recs)}
