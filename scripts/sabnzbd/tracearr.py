#!/usr/bin/env python3
# tracearr — SABnzbd helper script.
#
# Configure as a Notification Script in SABnzbd (Settings → Notifications)
# and select Job/Queue events. SABnzbd invokes the script with positional
# args; we forward to /script/sabnzbd on the tracearr instance.
#
# Required environment:
#   TRACEARR_URL    e.g. http://tracearr:8080
#   TRACEARR_TOKEN  matches SABNZBD_SCRIPT_TOKEN on the tracearr side
#
# SAB notification script signature (legacy):
#   $1=notification_type  $2=title  $3=msg
# We complement that with SAB_* env vars when present.

import json
import os
import sys
import urllib.error
import urllib.request
from datetime import datetime, timezone


def post(url: str, token: str, body: dict) -> int:
    req = urllib.request.Request(
        url.rstrip("/") + "/script/sabnzbd",
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
        return 0

    ts = datetime.now(timezone.utc).isoformat()
    notif = sys.argv[1] if len(sys.argv) > 1 else os.environ.get("SAB_NOTIFICATION_TYPE", "")

    # SAB notification types include "startup", "download", "complete", "failed", ...
    # Map "download" (= queue start) and "complete"/"failed" to our schema.
    if notif == "download":
        event = "queue_added"
        status = ""
    elif notif in ("complete", "failed"):
        event = "post_processed"
        status = "Failed" if notif == "failed" else "Completed"
    else:
        return 0

    size = int(os.environ.get("SAB_BYTES", "0") or 0)
    body = {
        "event": event,
        "download_id": os.environ.get("SAB_NZO_ID", ""),
        "name": os.environ.get("SAB_FILENAME", "") or os.environ.get("SAB_FINAL_NAME", ""),
        "category": os.environ.get("SAB_CAT", ""),
        "status": status,
        "size_bytes": size,
        "ts": ts,
    }
    post(url, token, body)
    return 0


if __name__ == "__main__":
    sys.exit(main())
