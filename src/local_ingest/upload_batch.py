#!/usr/bin/env python3
import argparse, pathlib, boto3
def upload_dir(s3, bucket, local_dir, s3_prefix):
    for path in pathlib.Path(local_dir).rglob('*.csv'):
        rel = path.relative_to(local_dir).as_posix()
        key = f"{s3_prefix}/{rel}"
        s3.upload_file(str(path), bucket, key)
        print(f"Uploaded: s3://{bucket}/{key}")
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--bucket', required=True)
    ap.add_argument('--date', required=True, help='YYYY-MM-DD partition to upload')
    ap.add_argument('--raw-prefix', default='raw')
    args = ap.parse_args()
    base = pathlib.Path(__file__).parents[2] / 'sample_data' / 'raw'
    for dom in ['customer','transactions']:
        local = base / dom / f'ingest_date={args.date}'
        if local.exists():
            s3 = boto3.client('s3')
            upload_dir(s3, args.bucket, str(local), f"{args.raw_prefix}/{dom}")
        else:
            print(f'Skip missing: {local}')
if __name__ == '__main__':
    main()
