"""Server mode on a running stack: Caddy in front of Metabase (ADR-0019, ADR-0021).

The tests run when .env selects the server profile: in the server job of the Stack workflow, with
AGORANOMOS_TLS=internal and https://agoranomos.localhost, and on a real server with a certificate
of Let's Encrypt. They reach the proxy on the ports that it publishes on this machine, with the name
of the site in TLS and in the Host header, as a browser does. They print statuses, names and
headers, never a body that could hold a secret.
"""

import json

import pytest
from services import SERVER
from stack_helpers import (
    NamesOnly,
    Secret,
    SecretHeaders,
    compose,
    containers,
    http_json,
    plain_request,
    process_uids,
    site_host,
    site_request,
)

pytestmark = pytest.mark.skipif(not SERVER, reason="server mode is off: .env selects no server")

WAREHOUSE_NAME = "Agoranomos warehouse"


def test_http_redirects_to_https_with_the_path_and_the_query(env):
    status, location = plain_request(env, "/question/1?tab=2")
    assert status == 308
    assert location == f"https://{site_host(env)}/question/1?tab=2"


def test_metabase_answers_over_https_with_a_certificate_for_the_site(env):
    status, _, body = site_request(env, "GET", "/api/health")
    assert status == 200
    assert json.loads(body) == {"status": "ok"}


def test_a_name_that_is_not_the_site_gets_no_certificate(env):
    # Airflow, or anything else, under another name of the domain: Caddy has no certificate for
    # it, and the handshake fails.
    with pytest.raises(ConnectionError, match="SSL"):
        site_request(env, "GET", "/", name=f"airflow.{site_host(env)}")


def test_airflow_is_not_behind_the_proxy(env):
    status, _, body = site_request(env, "GET", "/api/v2/monitor/health")
    assert b"metadatabase" not in body, f"Airflow answered through the proxy: HTTP {status}"


def test_the_responses_carry_the_security_headers(env):
    for path in ("/api/health", "/api/setup"):
        _, headers, _ = site_request(env, "GET", path)
        assert headers["strict-transport-security"].startswith("max-age=31536000"), path
        assert headers["x-content-type-options"] == "nosniff", path
        assert "referrer-policy" in headers, path


# Caddy's path matcher is case-insensitive, merges repeated slashes and matches the unescaped
# path (caddyserver.com/docs/caddyfile/matchers#path, read 2026-10-10), so each of these is closed.
# The setup API of Metabase 0.64.1 has one endpoint, POST /api/setup (src/metabase/setup_rest/
# api.clj), so closing it takes nothing from a signed-in admin. Jetty, under Metabase, drops the
# parameters of a path segment (;x=1), so a path that the proxy let through with one would reach
# the setup API: the proxy must close it too.
@pytest.mark.parametrize(
    ("method", "path"),
    [
        ("POST", "/api/setup"),
        ("GET", "/api/setup"),
        ("HEAD", "/api/setup"),
        ("POST", "/api/setup/"),
        ("POST", "/api/setup/validate"),
        ("POST", "//api/setup"),
        ("POST", "/API/SETUP"),
        ("POST", "/api/setup%2Fvalidate"),
        ("POST", "/api/setup;x=1"),
        ("POST", "/api/%73etup"),
        ("POST", "/api/analytics-proxy"),
        ("POST", "/api/analytics-proxy/com.snowplowanalytics.snowplow/tp2"),
        ("POST", "//api/Analytics-Proxy"),
        ("POST", "/api/product-feedback"),
    ],
)
def test_the_doors_of_metabase_that_need_no_sign_in_are_closed_at_the_proxy(env, method, path):
    status, _, _ = site_request(env, method, path, body=None if method == "HEAD" else {})
    assert status == 404


def test_metabase_itself_still_answers_its_setup_api_inside_the_stack(metabase_url):
    # So the 404 above is the proxy's: Metabase refuses a second setup with 403.
    status, _ = http_json("POST", f"{metabase_url}/api/setup", {})
    assert status in (400, 403)


def test_only_the_proxy_publishes_on_every_address_and_only_tcp_80_and_443():
    # 0.0.0.0 and :: are the addresses that Docker reports for a port published on every address
    # of the host: nothing here binds to them.
    on_every_address = set()
    for service, container in containers().items():
        for publisher in container.get("Publishers") or []:
            if publisher.get("PublishedPort") and publisher["URL"] in ("0.0.0.0", "::"):  # noqa: S104
                on_every_address.add((service, publisher["PublishedPort"], publisher["Protocol"]))
    assert {(port, protocol) for _, port, protocol in on_every_address} == {
        (80, "tcp"),
        (443, "tcp"),
    }
    assert {service for service, _, _ in on_every_address} == {"proxy"}


def test_the_proxy_has_no_admin_api_and_no_capability_on_its_binary():
    admin = compose(
        "exec",
        "-T",
        "proxy",
        "curl",
        "--silent",
        "--output",
        "/dev/null",
        "--write-out",
        "%{http_code}",
        "--max-time",
        "5",
        "http://localhost:2019/config/",
        check=False,
    )
    assert admin.stdout == "000"
    assert compose("exec", "-T", "proxy", "getcap", "/usr/bin/caddy").stdout.strip() == ""


def test_the_proxy_runs_as_nobody():
    uids = compose("exec", "-T", "proxy", "sh", "-c", process_uids("caddy")).stdout.split()
    assert uids[:2] == ["Uid:", "65534"]


def test_metabase_knows_the_address_of_the_site(env, metabase_url, session):
    status, properties = http_json("GET", f"{metabase_url}/api/session/properties", headers=session)
    assert status == 200
    assert properties["site-url"] == env["AGORANOMOS_SITE_URL"]


def test_metabase_asks_for_15_characters_and_signs_out_after_a_day_without_activity(
    metabase_url, session
):
    # ./agoranomos init --server sets both (ADR-0019). password-complexity is a public setting;
    # GET /api/setting names a setting that comes from the environment, and hides its value.
    status, properties = http_json("GET", f"{metabase_url}/api/session/properties")
    assert status == 200
    assert properties["password-complexity"] == {"total": 15}
    status, settings = http_json("GET", f"{metabase_url}/api/setting", headers=session)
    assert status == 200
    [timeout] = [item for item in settings if item["key"] == "session-timeout"]
    assert (timeout["is_env_setting"], timeout["env_name"]) == (True, "MB_SESSION_TIMEOUT")


def test_a_question_that_runs_longer_than_a_minute_comes_back_through_the_proxy(env):
    # Caddy 2.11.6 and later have idle timeouts of a minute, and Metabase lets a query run for 20
    # minutes: the answer of a query of 70 seconds must still come back through the proxy.
    status, _, body = site_request(
        env,
        "POST",
        "/api/session",
        {
            "username": env.get("AGORANOMOS_ADMIN_EMAIL") or "admin@example.com",
            "password": env["METABASE_ADMIN_PASSWORD"],
        },
    )
    assert status == 200, f"the admin could not sign in through the proxy: HTTP {status}"
    session = SecretHeaders({"X-Metabase-Session": Secret(json.loads(body)["id"])})
    status, _, body = site_request(env, "GET", "/api/database", headers=session)
    assert status == 200
    databases = json.loads(body, object_hook=NamesOnly)
    items = databases["data"] if isinstance(databases, dict) else databases
    [warehouse] = [item["id"] for item in items if item["name"] == WAREHOUSE_NAME]
    status, _, body = site_request(
        env,
        "POST",
        "/api/dataset",
        {"database": warehouse, "type": "native", "native": {"query": "SELECT pg_sleep(70)"}},
        headers=session,
        timeout=150,
    )
    assert status in (200, 202)
    assert json.loads(body)["status"] == "completed"
