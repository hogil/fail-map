#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
S3 공통 유틸리티
- create_s3_client: boto3 S3 클라이언트 생성 (Bucket A/B 공용)
- decode_best_effort: 멀티 인코딩 디코드
"""

import boto3
from botocore.config import Config


def create_s3_client(cfg):
    """
    cfg에 bucket_name, region_name, aws_access_key_id,
    aws_secret_access_key, endpoint_url, max_pool_connections 필드가 있으면 동작.
    PipelineConfig / BucketBConfig 둘 다 사용 가능.
    """
    return boto3.session.Session().client(
        "s3",
        region_name=cfg.region_name or None,
        aws_access_key_id=cfg.aws_access_key_id or None,
        aws_secret_access_key=cfg.aws_secret_access_key or None,
        endpoint_url=cfg.endpoint_url or None,
        config=Config(
            max_pool_connections=cfg.max_pool_connections,
            retries={'max_attempts': 8, 'mode': 'adaptive'},
        ),
        use_ssl=False,
    )


def decode_best_effort(b: bytes) -> str:
    for enc in ("utf-8", "cp949", "euc-kr", "latin1"):
        try:
            return b.decode(enc)
        except:
            pass
    return b.decode("utf-8", errors="ignore")
