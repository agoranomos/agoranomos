"""tiktok_api: what the load of TikTok and ./agoranomos tiktok connect share (ADR-0053): the
version of the TikTok API for Business and the estimate of its sunset, the address of the API, the
setting of the token and its form, and the exchange of an auth_code for a long-term token, with
the standard library alone, so that the exchange imports no dlt.

The exchange runs in the container of the Airflow scheduler, which reaches the internet, as

    python -m agoranomos_pipelines.tiktok_api connect

from the folder of the DAGs. It reads three lines from its standard input, the app ID, the secret
of the app and the auth_code, never from its arguments, which other users of a host can read;
sends them to POST /open_api/v1.3/oauth2/access_token/ once, as a code can be used once; and
prints the token on the first line of its standard output, which the script writes into .env, and
then the advertisers that the token reaches against those of tiktok_ads in
extensions/model/clients.toml. No message shows the secret, the code or the token. What TikTok
answers is data: an advertiser name or a message can carry any text, and nothing here acts on
it."""

from __future__ import annotations

import json
import re
import sys
from collections.abc import Callable, Iterable
from datetime import date
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

# v1.3, for every request, the reports included (ADR-0053): the lists, /advertiser/info/ and the
# sign-in are on v1.3 alone, and the report of v1.3 takes one advertiser through advertiser_id,
# which keeps the decimals of money. TikTok gives v1.3 "August 15, 2022 | TBD" for its sunset
# (Timelines, https://business-api.tiktok.com/portal/docs/timelines/v1.3, as of November 2026), and
# gave "a year to migrate from v1.2 to v1.3". While it gives no date, SUNSET is the estimate of
# ADR-0053, a year after the launch of the Reporting API v2.0 on 23 October 2026; a release
# replaces it with the date of TikTok once TikTok publishes one. A unit test fails from 90 days
# before SUNSET, 25 July 2027, and each load warns from then on (ADR-0032).
API_VERSION = "v1.3"
SUNSET = date(2027, 10, 23)
WARN_DAYS = 90
HOST = "business-api.tiktok.com"
BASE = f"https://{HOST}/open_api/{API_VERSION}"
# The setting of .env that holds the long-term token of the person who authorized the app.
TOKEN = "TIKTOK_ADS_ACCESS_TOKEN"  # noqa: S105 (the name of a variable)
# A token: one word of printable ASCII without a quote, a $, a #, a backslash, a backtick or an
# angle bracket, which .env carries as it is, unquoted. TikTok does not document the form of its
# tokens. ./agoranomos preflight checks the same form, and never the value.
TOKEN_FORM = re.compile(r"[!%&(-;=?-\[\]-_a-~]+")
# The app ID of a developer app: digits (the app_id of /oauth2/access_token/ is a string of them in
# the examples of TikTok). The secret: one word of printable ASCII.
APP_ID_FORM = re.compile(r"[0-9]{1,30}")
SECRET_FORM = re.compile(r"[!-~]{1,512}")
# An auth_code alone, not the address of the redirect that carries it: no &, =, ?, / or #.
CODE_FORM = re.compile(r"[!-\"$-%'-.0-<>@-~]{1,512}")
EXCHANGE = f"{BASE}/oauth2/access_token/"
EXCHANGE_TIMEOUT = 60
# The folder of the settings of the deployment, which compose.yaml mounts read-only.
EXTENSIONS = Path("/opt/agoranomos/extensions/model")
PLATFORM = "tiktok_ads"
ADVERTISER_ID = re.compile(r"[0-9]{1,20}")


class ConnectError(RuntimeError):
    """A step of the exchange that failed, in one line with no secret."""


def scrub(text: str, secrets: Iterable[str]) -> str:
    """The text with each secret as ***, the longest first."""
    for value in sorted((s for s in secrets if s), key=len, reverse=True):
        text = text.replace(value, "***")
    return text


def checked_inputs(app_id: str, secret: str, auth_code: str) -> None:
    """Refuse an app ID, a secret or a code of the wrong form before anything is sent; no message
    shows a value."""
    if not APP_ID_FORM.fullmatch(app_id):
        raise ConnectError("the app ID is the digits of App ID in My Apps > App Detail")
    if not SECRET_FORM.fullmatch(secret):
        raise ConnectError("the secret is Secret in My Apps > App Detail, one word without spaces")
    if not CODE_FORM.fullmatch(auth_code):
        raise ConnectError(
            "the code is the value of auth_code alone, from the address of the redirect after "
            "auth_code= and before the next &, not the whole address"
        )


def exchange(
    app_id: str, secret: str, auth_code: str, opener: Callable | None = None
) -> tuple[str, list[str]]:
    """The long-term token that TikTok gives for the auth_code, and the advertisers that it
    reaches: POST /oauth2/access_token/ with app_id, secret and auth_code as JSON, its only
    Content-Type, which answers {"code": 0, "message": "OK", "data": {"access_token": ...,
    "advertiser_ids": [...], "scope": [...]}} (Obtain a long-term access token,
    https://business-api.tiktok.com/portal/docs/obtain-a-long-term-access-token/v1.3, as of
    November 2026). A code other than 0 is a failure even with HTTP 200 (HTTP status codes, v1.3).
    The request goes once: the code "can be used only once"."""
    checked_inputs(app_id, secret, auth_code)
    secrets = (secret, auth_code)
    body = json.dumps({"app_id": app_id, "secret": secret, "auth_code": auth_code}).encode()
    request = Request(  # noqa: S310 (a constant https address)
        EXCHANGE, data=body, headers={"Content-Type": "application/json"}, method="POST"
    )
    try:
        with (opener or urlopen)(request, timeout=EXCHANGE_TIMEOUT) as response:
            raw = response.read()
    except HTTPError as error:
        raise ConnectError(
            scrub(f"TikTok answered {error.code} {error.reason} to the exchange", secrets)
        ) from None
    except (URLError, OSError) as error:
        reason = getattr(error, "reason", error)
        raise ConnectError(scrub(f"could not reach {HOST}: {reason}", secrets)) from None
    try:
        answer = json.loads(raw)
    except ValueError:
        raise ConnectError("TikTok answered the exchange with something that is not JSON") from None
    if not isinstance(answer, dict):
        raise ConnectError("TikTok answered the exchange with something that is not an object")
    if answer.get("code") != 0:
        message = str(answer.get("message", ""))[:300]
        said = (
            f"TikTok refused the exchange: {message} (code {answer.get('code')}, request "
            f"{answer.get('request_id')}). A code is valid for an hour at most and once: open the "
            "advertiser authorization URL again for a new one"
        )
        raise ConnectError(scrub(said, secrets))
    data = answer.get("data") if isinstance(answer.get("data"), dict) else {}
    token = data.get("access_token")
    if not isinstance(token, str) or not token:
        raise ConnectError("TikTok answered the exchange without a token")
    if not TOKEN_FORM.fullmatch(token):
        raise ConnectError(
            "TikTok gave a token that .env cannot hold as it is, as it has a space, a quote, a $, "
            "a # or a backslash: report it as an issue of agoranomos"
        )
    reached = data.get("advertiser_ids")
    reached = [str(item) for item in reached] if isinstance(reached, list) else []
    return token, reached


def named(extensions: Path | None = None) -> list[str]:
    """The advertisers of tiktok_ads that clients.toml names."""
    # Imported here, so that a mistake in the module of the settings cannot stop the exchange.
    from agoranomos_pipelines.settings.files import load_settings

    return sorted(
        entry["account_id"]
        for entry in load_settings(extensions or EXTENSIONS, None).account_clients
        if entry["platform"] == PLATFORM
    )


def compared(reached: Iterable[str], extensions: Path | None = None) -> list[str]:
    """The lines that compare the advertisers that the token reaches with those of tiktok_ads in
    clients.toml, which the load takes: the IDs of those that both name and of those that the
    token does not reach, and the count of the others, which the load leaves out (ADR-0024)."""
    reached = sorted({item for item in reached if ADVERTISER_ID.fullmatch(item)})
    try:
        wanted = named(extensions)
    except Exception as error:  # any mistake of the file is one line here
        return [
            f"The token reaches {len(reached)} advertisers. clients.toml could not be read, so "
            f"they are not compared with it: {error}"
        ]
    lines = [f"The token reaches {len(reached)} advertisers of TikTok."]
    both = [item for item in wanted if item in reached]
    missing = [item for item in wanted if item not in reached]
    others = len(set(reached) - set(wanted))
    if not wanted:
        lines.append(
            "extensions/model/clients.toml names no advertiser of tiktok_ads yet: name those "
            "to load there"
        )
    if both:
        lines.append("Named in clients.toml and reached, so loaded: " + ", ".join(both))
    if missing:
        lines.append(
            "Named in clients.toml and not reached by this token, so their loads fail: "
            + ", ".join(missing)
            + ". The person who authorized the app needs a role on them, such as Analyst"
        )
    if others:
        lines.append(f"{others} more that clients.toml does not name, which the load leaves out")
    return lines


def connect(
    lines: list[str], opener: Callable | None = None, extensions: Path | None = None
) -> list[str]:
    """The lines to print: the token first, then the comparison with clients.toml."""
    values = [line.strip() for line in lines[:3]]
    if len(values) < 3:
        raise ConnectError("the exchange reads the app ID, the secret and the code, a line each")
    token, reached = exchange(*values, opener=opener)
    return [token, *compared(reached, extensions)]


def main(argv: list[str]) -> int:
    if argv != ["connect"]:
        print("usage: python -m agoranomos_pipelines.tiktok_api connect", file=sys.stderr)
        return 2
    try:
        output = connect(sys.stdin.read().splitlines())
    except ConnectError as error:
        print(f"tiktok connect: {error}", file=sys.stderr)
        return 1
    print("\n".join(output))
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
