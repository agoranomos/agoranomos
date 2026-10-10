"""The files of the proxy image of server mode (ADR-0019), read as text."""

import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
CADDY = REPO_ROOT / "caddy"


def caddyfile() -> str:
    """The Caddyfile without its comment lines."""
    lines = (CADDY / "Caddyfile").read_text(encoding="utf-8").splitlines()
    return "\n".join(line.strip() for line in lines if not line.strip().startswith("#"))


def test_the_image_is_the_pinned_caddy_without_the_capability_of_its_binary():
    dockerfile = (CADDY / "Dockerfile").read_text(encoding="utf-8")
    assert re.search(r"^FROM caddy:2\.\d+\.\d+-alpine@sha256:[0-9a-f]{64}$", dockerfile, re.M)
    assert "RUN setcap -r /usr/bin/caddy" in dockerfile
    assert "COPY Caddyfile /etc/caddy/Caddyfile" in dockerfile


def test_caddy_keeps_no_admin_api_no_saved_configuration_and_no_udp():
    text = caddyfile()
    for option in ("admin off", "persist_config off", "skip_install_trust", "protocols h1 h2"):
        assert option in text.splitlines(), option
    assert "acme_ca {$AGORANOMOS_ACME_CA}" in text.splitlines()


def test_the_site_is_the_address_of_metabase_and_forwards_to_metabase_only():
    text = caddyfile()
    assert "{$AGORANOMOS_SITE_URL} {" in text.splitlines()
    assert re.findall(r"^reverse_proxy (.+)$", text, re.M) == ["metabase:3000"]


def test_the_doors_of_metabase_that_need_no_sign_in_are_closed_by_prefix():
    # A prefix closes every path that begins with the name of a door, in whatever spelling
    # (/api/setup. /api/setup%20 /api/setup;x=1), so none of them reaches the router of Metabase.
    text = caddyfile()
    matchers = dict(re.findall(r"^@(\w+) path (.+)$", text, re.M))
    assert matchers == {
        "setup": "/api/setup*",
        "relays": "/api/analytics-proxy* /api/product-feedback*",
    }
    assert sorted(re.findall(r"^respond @(\w+) 404$", text, re.M)) == ["relays", "setup"]


def test_the_headers_fill_in_only_what_metabase_does_not_set():
    text = caddyfile()
    assert re.findall(r'^header \?(\S+) "(.+)"$', text, re.M) == [
        ("Strict-Transport-Security", "max-age=31536000"),
        ("X-Content-Type-Options", "nosniff"),
        ("Referrer-Policy", "strict-origin-when-cross-origin"),
    ]
