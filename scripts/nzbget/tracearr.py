#!/usr/bin/env python3
# tracearr — NZBGet helper script.
#
# Drop into your NZBGet `ScriptDir`, then enable it as both a Queue Script
# and a Post-Processing Script in the NZBGet web UI. It POSTs the
# documented schema to /script/nzbget on the tracearr instance.
#
# Required environment (set via NZBGet "Extension Scripts" config):
#   TRACEARR_URL    e.g. http://tracearr:8080
#   TRACEARR_TOKEN  matches NZBGET_SCRIPT_TOKEN on the tracearr side
#
# NZBGet sets NZBPP_* (post-processing) or NZBNA_* (queue) env vars per
# call; the script picks the right shape.

import json
import os
import sys
import urllib.error
import urllib.request
from datetime import datetime, timezone


def post(url: str, token: str, body: dict) -> int:
    req = urllib.request.Request(
        url.rstrip("/") + "/script/nzbget",
        data=json.dumps(body).encode("utf-8"),
        method="POST",
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=5) as resp:
            return resp.status
    except urllib.error.URLError as e:
        sys.stderr.write(f"tracearr post failed: {e}\n")
        return 1


def main() -> int:
    url = os.environ.get("TRACEARR_URL")
    token = os.environ.get("TRACEARR_TOKEN")
    if not url or not token:
        sys.stderr.write("TRACEARR_URL and TRACEARR_TOKEN must be set\n")
        return 0  # do not break NZBGet pipeline on misconfiguration

    ts = datetime.now(timezone.utc).isoformat()

    if "NZBNA_EVENT" in os.environ:
        # Queue script invocation. We only emit on NZB_ADDED.
        if os.environ["NZBNA_EVENT"] != "NZB_ADDED":
            return 0
        body = {
            "event": "queue_added",
            "download_id": os.environ.get("NZBNA_NZBID", ""),
            "name": os.environ.get("NZBNA_NZBNAME", ""),
            "category": os.environ.get("NZBNA_CATEGORY", ""),
            "size_bytes": int(os.environ.get("NZBNA_FILESIZE", "0") or 0),
            "ts": ts,
        }
    else:
        # Post-processing invocation.
        body = {
            "event": "post_processed",
            "download_id": os.environ.get("NZBPP_NZBID", ""),
            "name": os.environ.get("NZBPP_NZBNAME", ""),
            "category": os.environ.get("NZBPP_CATEGORY", ""),
            "status": os.environ.get("NZBPP_TOTALSTATUS", ""),
            "size_bytes": int(os.environ.get("NZBPP_FILESIZE", "0") or 0),
            "ts": ts,
        }

    post(url, token, body)
    return 93  # NZBGet POSTPROCESS_SUCCESS / queue script no-op


if __name__ == "__main__":
    sys.exit(main())
