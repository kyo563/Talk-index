from __future__ import annotations

import json
import os
from datetime import UTC, datetime

import boto3

from crawler.services.favorites import build_aggregates, dump_json


def _require_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise RuntimeError(f"{name} が未設定です。GitHub Secrets を確認してください。")
    return value


def _read_unique_votes(s3, bucket: str) -> list[dict[str, object]]:
    votes: list[dict[str, object]] = []
    token: str | None = None

    while True:
        kwargs = {"Bucket": bucket, "Prefix": "favorites/unique/", "MaxKeys": 1000}
        if token:
            kwargs["ContinuationToken"] = token
        response = s3.list_objects_v2(**kwargs)
        for item in response.get("Contents", []):
            key = str(item.get("Key") or "")
            if not key.endswith(".json"):
                continue
            body = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
            try:
                row = json.loads(body.decode("utf-8"))
            except json.JSONDecodeError:
                continue
            if isinstance(row, dict):
                votes.append(row)

        if not response.get("IsTruncated"):
            break
        token = response.get("NextContinuationToken")

    return votes


def _put_json(s3, bucket: str, key: str, payload: dict[str, object]) -> None:
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=dump_json(payload),
        ContentType="application/json; charset=utf-8",
        CacheControl="public, max-age=60",
    )


def main() -> None:
    account_id = _require_env("R2_ACCOUNT_ID")
    access_key_id = _require_env("R2_ACCESS_KEY_ID")
    secret_access_key = _require_env("R2_SECRET_ACCESS_KEY")
    bucket = _require_env("R2_BUCKET_NAME")

    s3 = boto3.client(
        "s3",
        endpoint_url=f"https://{account_id}.r2.cloudflarestorage.com",
        aws_access_key_id=access_key_id,
        aws_secret_access_key=secret_access_key,
        region_name="auto",
    )

    votes = _read_unique_votes(s3, bucket)
    aggregates = build_aggregates(votes, now_utc=datetime.now(UTC))

    _put_json(s3, bucket, "favorites/aggregates/all_time.json", aggregates["all_time"])
    _put_json(s3, bucket, "favorites/aggregates/hall_of_fame.json", aggregates["hall_of_fame"])
    _put_json(
        s3,
        bucket,
        "favorites/aggregates/recent_recommendations.json",
        aggregates["recent_recommendations"],
    )

    for week_key, payload in aggregates["weekly"].items():
        _put_json(s3, bucket, f"favorites/aggregates/weekly/{week_key}.json", payload)

    _put_json(s3, bucket, "favorites/exports/current_ranking.json", aggregates["current_ranking"])
    snapshot_date = str(aggregates["daily_snapshot"]["snapshotDate"])
    _put_json(
        s3,
        bucket,
        f"favorites/exports/daily_snapshot/{snapshot_date}.json",
        aggregates["daily_snapshot"],
    )

    print(f"favorites aggregate rebuilt: unique_votes={len(votes)}")


if __name__ == "__main__":
    main()
