#!/usr/bin/env python3
"""
TikTok Research API scraper — pulls post-level metadata (caption + all other
available fields) for a list of TikTok accounts ("parties").

Based on:
  https://developers.tiktok.com/docs/en/research-api-get-started
  Video query endpoint:  POST https://open.tiktokapis.com/v2/research/video/query/
  Token endpoint:        POST https://open.tiktokapis.com/v2/oauth/token/

Requirements
------------
  - Approved TikTok Research API access (client_key / client_secret) with the
    `research.data.basic` scope.
  - Set credentials as environment variables before running:
        export TIKTOK_CLIENT_KEY="..."
        export TIKTOK_CLIENT_SECRET="..."
  - pip install requests

Usage
-----
  python tiktok_research_scraper.py \
      --accounts accounts.txt \
      --start-date 20240101 \
      --end-date 20240131 \
      --out tiktok_posts.csv

  accounts.txt: one TikTok username per line (no @, no URL — just the handle).

Notes on API limits (per TikTok's docs at time of writing)
------------------------------------------------------------
  - A single query's date window (end_date - start_date) cannot exceed 30 days.
    This script automatically splits a longer requested range into <=30-day
    windows and issues one query per window.
  - The `username` filter with the IN operator accepts a limited batch size;
    this script batches accounts (default 20 per request, configurable) and
    issues one query per (account batch, date window) combination.
  - max_count per request is capped at 100; pagination uses `cursor` /
    `has_more` / `search_id` as documented and is handled automatically.
  - Client access tokens are valid for 2 hours; the script refreshes
    automatically.
  - The Research API also enforces a daily query quota per project — if you
    have many accounts and/or a long date range, expect this to take
    multiple days or require quota increases. The script will surface a
    clear error if the daily quota is exhausted (rather than silently
    retrying forever).

This script does not execute any requests on its own here — it's provided as
code for you to run with your own approved credentials.
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import os
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Iterable, Iterator

import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("tiktok_research_scraper")

TOKEN_URL = "https://open.tiktokapis.com/v2/oauth/token/"
VIDEO_QUERY_URL = "https://open.tiktokapis.com/v2/research/video/query/"

# All fields the Research API can return for a video (comma-separated in the
# `fields` query parameter). Trim this list if you don't need everything.
ALL_FIELDS = [
    "id",
    "video_description",
    "create_time",
    "region_code",
    "share_count",
    "view_count",
    "like_count",
    "comment_count",
    "music_id",
    "hashtag_names",
    "username",
    "effect_ids",
    "playlist_id",
    "voice_to_text",
    "is_stem_verified",
    "video_duration",
    "favorites_count",
    "hashtag_info_list",
    "sticker_info_list",
    "effect_info_list",
    "video_mention_list",
    "video_label",
    "video_tag",
]

MAX_COUNT_PER_REQUEST = 100  # API max
DATE_WINDOW_DAYS = 30  # API max span per query (end_date - start_date)
DEFAULT_USERNAME_BATCH_SIZE = 20  # batch size for the IN condition on username
MAX_RETRIES = 5
RETRY_BACKOFF_SECONDS = 5


@dataclass
class TikTokResearchClient:
    client_key: str
    client_secret: str
    _access_token: str | None = field(default=None, init=False, repr=False)
    _token_expiry: float = field(default=0.0, init=False, repr=False)
    session: requests.Session = field(default_factory=requests.Session, init=False)

    def _fetch_access_token(self) -> None:
        resp = self.session.post(
            TOKEN_URL,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            data={
                "client_key": self.client_key,
                "client_secret": self.client_secret,
                "grant_type": "client_credentials",
            },
            timeout=30,
        )
        resp.raise_for_status()
        payload = resp.json()
        self._access_token = payload["access_token"]
        # refresh a little early to avoid using an expired token mid-request
        self._token_expiry = time.time() + payload.get("expires_in", 7200) - 60
        log.info("Fetched new client access token (expires in %ss).", payload.get("expires_in"))

    def _get_access_token(self) -> str:
        if self._access_token is None or time.time() >= self._token_expiry:
            self._fetch_access_token()
        return self._access_token  # type: ignore[return-value]

    def query_videos_page(
        self,
        usernames: list[str],
        start_date: str,
        end_date: str,
        cursor: int = 0,
        search_id: str | None = None,
        fields: Iterable[str] = ALL_FIELDS,
        max_count: int = MAX_COUNT_PER_REQUEST,
    ) -> dict:
        """Fetch a single page of results for a batch of usernames / date window."""
        body = {
            "query": {
                "and": [
                    {
                        "operation": "IN",
                        "field_name": "username",
                        "field_values": usernames,
                    }
                ]
            },
            "start_date": start_date,
            "end_date": end_date,
            "max_count": max_count,
            "cursor": cursor,
        }
        if search_id:
            body["search_id"] = search_id

        params = {"fields": ",".join(fields)}

        for attempt in range(1, MAX_RETRIES + 1):
            token = self._get_access_token()
            resp = self.session.post(
                VIDEO_QUERY_URL,
                params=params,
                headers={
                    "Authorization": f"Bearer {token}",
                    "Content-Type": "application/json",
                },
                data=json.dumps(body),
                timeout=60,
            )

            if resp.status_code == 401:
                # token might have just been invalidated server-side; force refresh once
                log.warning("Got 401, refreshing token and retrying.")
                self._access_token = None
                continue

            if resp.status_code == 429:
                wait = RETRY_BACKOFF_SECONDS * attempt
                log.warning("Rate limited (429). Backing off %ss (attempt %s/%s).", wait, attempt, MAX_RETRIES)
                time.sleep(wait)
                continue

            if resp.status_code >= 500:
                wait = RETRY_BACKOFF_SECONDS * attempt
                log.warning("Server error %s. Backing off %ss (attempt %s/%s).", resp.status_code, wait, attempt, MAX_RETRIES)
                time.sleep(wait)
                continue

            if not resp.ok:
                # Surface quota-exhaustion / bad-request errors immediately rather
                # than retrying blindly.
                log.error("Request failed (%s): %s", resp.status_code, resp.text[:1000])
                resp.raise_for_status()

            return resp.json()

        raise RuntimeError(f"Exceeded max retries ({MAX_RETRIES}) querying videos for usernames={usernames}")

    def query_videos_all_pages(
        self,
        usernames: list[str],
        start_date: str,
        end_date: str,
        fields: Iterable[str] = ALL_FIELDS,
        max_count: int = MAX_COUNT_PER_REQUEST,
    ) -> Iterator[dict]:
        """Yield every video across all pages for this batch/date window."""
        cursor = 0
        search_id: str | None = None
        while True:
            payload = self.query_videos_page(
                usernames=usernames,
                start_date=start_date,
                end_date=end_date,
                cursor=cursor,
                search_id=search_id,
                fields=fields,
                max_count=max_count,
            )
            data = payload.get("data", {})
            videos = data.get("videos", [])
            for video in videos:
                yield video

            if not data.get("has_more"):
                break
            cursor = data.get("cursor", cursor + len(videos))
            search_id = data.get("search_id", search_id)


def chunked(seq: list, size: int) -> Iterator[list]:
    for i in range(0, len(seq), size):
        yield seq[i : i + size]


def date_windows(start_date: str, end_date: str, window_days: int = DATE_WINDOW_DAYS) -> Iterator[tuple[str, str]]:
    """Split [start_date, end_date] (YYYYMMDD strings) into <=window_days chunks."""
    start = datetime.strptime(start_date, "%Y%m%d")
    end = datetime.strptime(end_date, "%Y%m%d")
    if start > end:
        raise ValueError("start_date must be <= end_date")

    cur = start
    while cur <= end:
        window_end = min(cur + timedelta(days=window_days - 1), end)
        yield cur.strftime("%Y%m%d"), window_end.strftime("%Y%m%d")
        cur = window_end + timedelta(days=1)


def load_accounts(accounts_arg: str) -> list[str]:
    """accounts_arg is either a path to a file (one handle per line) or a
    comma-separated string of handles."""
    if os.path.isfile(accounts_arg):
        with open(accounts_arg, "r", encoding="utf-8") as f:
            handles = [line.strip().lstrip("@") for line in f if line.strip()]
    else:
        handles = [h.strip().lstrip("@") for h in accounts_arg.split(",") if h.strip()]
    if not handles:
        raise ValueError("No account handles found.")
    return handles


def flatten_video_record(video: dict) -> dict:
    """CSV-friendly version of a video record: nested lists/dicts -> JSON strings."""
    flat = {}
    for key, value in video.items():
        if isinstance(value, (list, dict)):
            flat[key] = json.dumps(value, ensure_ascii=False)
        else:
            flat[key] = value
    return flat


def run(
    accounts: list[str],
    start_date: str,
    end_date: str,
    out_path: str,
    fields: Iterable[str] = ALL_FIELDS,
    username_batch_size: int = DEFAULT_USERNAME_BATCH_SIZE,
) -> None:
    client_key = os.environ.get("TIKTOK_CLIENT_KEY")
    client_secret = os.environ.get("TIKTOK_CLIENT_SECRET")
    if not client_key or not client_secret:
        raise SystemExit(
            "Set TIKTOK_CLIENT_KEY and TIKTOK_CLIENT_SECRET environment variables first."
        )

    client = TikTokResearchClient(client_key=client_key, client_secret=client_secret)

    fields = list(fields)
    fieldnames = fields  # username is already included in ALL_FIELDS

    total_written = 0
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()

        for win_start, win_end in date_windows(start_date, end_date):
            for batch in chunked(accounts, username_batch_size):
                log.info(
                    "Querying %s accounts for window %s-%s: %s",
                    len(batch), win_start, win_end, ", ".join(batch),
                )
                try:
                    for video in client.query_videos_all_pages(
                        usernames=batch,
                        start_date=win_start,
                        end_date=win_end,
                        fields=fields,
                    ):
                        writer.writerow(flatten_video_record(video))
                        total_written += 1
                except requests.HTTPError as e:
                    log.error(
                        "Failed batch %s for window %s-%s: %s",
                        batch, win_start, win_end, e,
                    )
                    continue

    log.info("Done. Wrote %s video records to %s", total_written, out_path)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument(
        "--accounts",
        required=True,
        help="Path to a file with one TikTok handle per line, or a comma-separated list of handles.",
    )
    p.add_argument("--start-date", required=True, help="YYYYMMDD, UTC.")
    p.add_argument("--end-date", required=True, help="YYYYMMDD, UTC.")
    p.add_argument("--out", default="tiktok_posts.csv", help="Output CSV path.")
    p.add_argument(
        "--fields",
        default=",".join(ALL_FIELDS),
        help="Comma-separated list of response fields to request.",
    )
    p.add_argument(
        "--username-batch-size",
        type=int,
        default=DEFAULT_USERNAME_BATCH_SIZE,
        help="How many usernames to combine per IN-condition query (default 20).",
    )
    return p.parse_args()


def main() -> None:
    args = parse_args()
    accounts = load_accounts(args.accounts)
    fields = [f.strip() for f in args.fields.split(",") if f.strip()]
    run(
        accounts=accounts,
        start_date=args.start_date,
        end_date=args.end_date,
        out_path=args.out,
        fields=fields,
        username_batch_size=args.username_batch_size,
    )


if __name__ == "__main__":
    main()
