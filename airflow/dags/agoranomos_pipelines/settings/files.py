"""The settings of the model that a deployment keeps in its extensions folder, in the subfolder
model: which client each account belongs to (clients.toml, ADR-0024) and which conversion actions
count as conversions (conversion_actions.toml, ADR-0025); and the clients of the demo, in a file of
the same form in the base package. The files are data: they are parsed as TOML and checked, and
nothing in them runs. A mistake stops the load with one line that names the file and the entry,
before anything is written. The line shows a key or an ID of the file only where it says where the
mistake is, in quotes and cut short when TOML would quote it, and never repeats a value."""

from __future__ import annotations

import json
import os
import re
import stat
import tomllib
import unicodedata
from dataclasses import dataclass
from pathlib import Path
from typing import Any

# The largest settings file, in bytes, as for the overrides of the metadata.
LIMIT = 256 * 1024
# A client ID, as in [clients.<id>]: lower-case letters, digits, - and _.
SLUG = re.compile(r"[a-z0-9][a-z0-9_-]{0,63}")
# The code of a platform, such as demo_search.
PLATFORM = re.compile(r"[a-z][a-z0-9_]{0,63}")
# The ID of the default client, which the model adds for the accounts that no setting names.
RESERVED = frozenset({"unassigned"})
# The categories of the conversion actions of the model (ADR-0025).
CATEGORIES = ("purchase", "lead", "sign_up", "add_to_cart", "app_install", "other")
# The longest name of a client, which Metabase shows, and the longest ID of an account or action.
NAME_LIMIT, ID_LIMIT = 100, 200
# A key that TOML writes without quotes, which the line of a mistake shows as it is; of any other
# key or ID it shows this many characters at most.
BARE = re.compile(r"[A-Za-z0-9_-]{1,64}")
SHOWN = 40
# The characters of Unicode that change the direction of the text around them, and that would
# make a name read otherwise than it is written.
BIDI = frozenset("؜‎‏‪‫‬‭‮⁦⁧⁨⁩")
# Where tomllib says that a mistake is, at the end of its message: the rest of the message can
# quote keys of the file.
PLACE = re.compile(r" \((at line \d+, column \d+|at end of document)\)$")

ACCOUNT_KEYS = {"platform", "account_id", "client"}
ACTION_KEYS = {"platform", "conversion_action_id", "is_primary", "conversion_category"}


class SettingsError(ValueError):
    """A mistake in a settings file, in one line that names the file and the entry."""


@dataclass(frozen=True)
class Settings:
    """The rows of the three tables of raw_settings, each a dict of its columns in their order."""

    clients: tuple[dict[str, Any], ...]
    account_clients: tuple[dict[str, Any], ...]
    conversion_actions: tuple[dict[str, Any], ...]


def shown(value: str) -> str:
    """A key or an ID of a file for the line of a mistake: as it is where TOML would write it
    without quotes; else in quotes, with every character but printable ASCII escaped, and cut after
    SHOWN characters."""
    if BARE.fullmatch(value):
        return value
    # json escapes every character outside the printable ASCII, from a space to a ~.
    quoted = json.dumps(value[:SHOWN], ensure_ascii=True)
    return quoted + ("..." if len(value) > SHOWN else "")


def under(path: str, key: str) -> str:
    """The path of an entry inside another, such as clients.acme.name; a key of the top level of
    a file stands alone."""
    return f"{path}.{shown(key)}" if path else shown(key)


def read_toml(path: Path, name: str) -> dict[str, Any] | None:
    """A settings file as TOML, or None when there is none. Only a regular file is read, so that a
    named pipe cannot block the load, and at most LIMIT bytes of it, whatever its size says."""
    try:
        found = os.stat(path)
    except FileNotFoundError:
        return None
    except OSError as error:
        raise SettingsError(f"{name}: cannot read the file: {type(error).__name__}") from None
    if not stat.S_ISREG(found.st_mode):
        raise SettingsError(f"{name}: a file is expected, and this is not one")
    if found.st_size > LIMIT:
        raise SettingsError(
            f"{name}: the file has {found.st_size} bytes, and it may have {LIMIT} at most"
        )
    try:
        with path.open("rb") as handle:
            data = handle.read(LIMIT + 1)
    except FileNotFoundError:
        # Gone since its size was read: a bind mount of Docker Desktop on macOS can show a file for
        # a moment after it is deleted.
        return None
    except OSError as error:
        raise SettingsError(f"{name}: cannot read the file: {type(error).__name__}") from None
    if len(data) > LIMIT:
        raise SettingsError(
            f"{name}: the file has more than {LIMIT} bytes, and it may have {LIMIT} at most"
        )
    try:
        # utf-8-sig, so that the mark that some editors put at the start of a file does no harm.
        return tomllib.loads(data.decode("utf-8-sig"))
    except UnicodeDecodeError:
        raise SettingsError(f"{name}: the file is not UTF-8") from None
    except tomllib.TOMLDecodeError as error:
        place = PLACE.search(str(error))
        where = f" ({place.group(1)})" if place else ""
        raise SettingsError(f"{name}: the file is not valid TOML{where}") from None
    except ValueError:
        # tomllib leaves the error of an integer of more than the digits that Python converts,
        # 4,300 by default, as it is: a ValueError that is no TOMLDecodeError.
        raise SettingsError(
            f"{name}: the file is not valid TOML: it has an integer with too many digits"
        ) from None
    except RecursionError:
        # tomllib reads a value in a value by recursion: a deep nesting ends Python's.
        raise SettingsError(
            f"{name}: the file is not valid TOML: it is nested too deeply"
        ) from None


def text(where: str, value: Any, limit: int) -> str:
    """A text of one line that says something, within its limit, without the spaces around it."""
    if not isinstance(value, str):
        raise SettingsError(f"{where}: a text in quotes is expected, not {type(value).__name__}")
    found = value.strip()
    if not found:
        raise SettingsError(f"{where}: the text is empty")
    if any(unicodedata.category(c) in ("Cc", "Zl", "Zp") or c in BIDI for c in found):
        raise SettingsError(f"{where}: a text of one line is expected, without control characters")
    if len(found) > limit:
        raise SettingsError(
            f"{where}: the text has {len(found)} characters, and it may have {limit} at most"
        )
    return found


def code(where: str, value: Any) -> str:
    """The code of a platform."""
    found = text(where, value, 64)
    if not PLATFORM.fullmatch(found):
        raise SettingsError(f"{where}: a code of a platform is lower-case letters, digits and _")
    return found


def known(name: str, path: str, entry: Any, allowed: set[str], required: set[str]) -> dict:
    """An entry of a file, a table of TOML with the keys that it allows and those it requires."""
    where = f"{name}: {path}" if path else name
    if not isinstance(entry, dict):
        raise SettingsError(f"{where}: a table of TOML is expected")
    for key in entry:
        if key not in allowed:
            raise SettingsError(
                f"{name}: {under(path, key)}: unknown property; use {', '.join(sorted(allowed))}"
            )
    for key in sorted(required - set(entry)):
        raise SettingsError(f"{where}: {key} is missing")
    return entry


def clients_of(document: dict[str, Any], name: str, origin: str) -> tuple[list[dict], list[dict]]:
    """The clients of a file of clients, and the accounts of each."""
    known(name, "", document, {"clients", "accounts"}, set())
    table = document.get("clients", {})
    if not isinstance(table, dict):
        raise SettingsError(f"{name}: clients: a table of TOML is expected")
    clients, mapped = [], []
    for client_id, entry in table.items():
        if not SLUG.fullmatch(client_id):
            raise SettingsError(
                f"{name}: [clients.{shown(client_id)}]: a client ID is lower-case letters, "
                "digits, - and _, at most 64 characters"
            )
        if client_id in RESERVED:
            raise SettingsError(
                f"{name}: [clients.{client_id}]: the ID is reserved for the default client"
            )
        path = f"clients.{client_id}"
        known(name, path, entry, {"name"}, {"name"})
        client_name = text(f"{name}: {path}.name", entry["name"], NAME_LIMIT)
        clients.append({"client_id": client_id, "client_name": client_name, "origin": origin})
    accounts = document.get("accounts", [])
    if not isinstance(accounts, list):
        raise SettingsError(f"{name}: accounts: an array of tables is expected, as [[accounts]]")
    seen = set()
    for index, entry in enumerate(accounts, start=1):
        path = f"accounts[{index}]"
        known(name, path, entry, ACCOUNT_KEYS, ACCOUNT_KEYS)
        platform = code(f"{name}: {path}.platform", entry["platform"])
        account_id = text(f"{name}: {path}.account_id", entry["account_id"], ID_LIMIT)
        client = text(f"{name}: {path}.client", entry["client"], 64)
        if not SLUG.fullmatch(client):
            raise SettingsError(
                f"{name}: {path}.client: a client ID is lower-case letters, digits, - and _"
            )
        if client not in table:
            raise SettingsError(
                f"{name}: {path}: the client {client} is not in [clients] of this file"
            )
        if (platform, account_id) in seen:
            raise SettingsError(f"{name}: {path}: {platform} {shown(account_id)} is listed twice")
        seen.add((platform, account_id))
        mapped.append(
            {"platform": platform, "account_id": account_id, "client_id": client, "origin": origin}
        )
    return clients, mapped


def conversion_actions_of(document: dict[str, Any], name: str) -> list[dict[str, Any]]:
    """The conversion actions of a file of conversion actions, each primary or not."""
    known(name, "", document, {"actions"}, set())
    actions = document.get("actions", [])
    if not isinstance(actions, list):
        raise SettingsError(f"{name}: actions: an array of tables is expected, as [[actions]]")
    found, seen = [], set()
    for index, entry in enumerate(actions, start=1):
        path = f"actions[{index}]"
        known(name, path, entry, ACTION_KEYS, {"platform", "conversion_action_id", "is_primary"})
        platform = code(f"{name}: {path}.platform", entry["platform"])
        action = text(
            f"{name}: {path}.conversion_action_id", entry["conversion_action_id"], ID_LIMIT
        )
        if not isinstance(entry["is_primary"], bool):
            raise SettingsError(f"{name}: {path}.is_primary: true or false is expected")
        category = entry.get("conversion_category")
        if category is not None and category not in CATEGORIES:
            raise SettingsError(
                f"{name}: {path}.conversion_category: one of {', '.join(CATEGORIES)} is expected"
            )
        if (platform, action) in seen:
            raise SettingsError(f"{name}: {path}: {platform} {shown(action)} is listed twice")
        seen.add((platform, action))
        found.append(
            {
                "platform": platform,
                "conversion_action_id": action,
                "is_primary": entry["is_primary"],
                "conversion_category": category,
                "origin": "extensions",
            }
        )
    return found


def load_settings(extensions: Path, demo_clients: Path | None) -> Settings:
    """The settings of the deployment in the folder extensions (the subfolder model of the
    extensions folder), and the clients of the demo, each checked; a missing file is no settings."""
    clients, mapped, actions = [], [], []
    document = read_toml(extensions / "clients.toml", "clients.toml")
    if document is not None:
        found, accounts = clients_of(document, "clients.toml", "extensions")
        clients += found
        mapped += accounts
    if demo_clients is not None:
        name = f"the demo's {demo_clients.name}"
        document = read_toml(demo_clients, name)
        if document is not None:
            found, accounts = clients_of(document, name, "demo")
            clients += found
            mapped += accounts
    document = read_toml(extensions / "conversion_actions.toml", "conversion_actions.toml")
    if document is not None:
        actions += conversion_actions_of(document, "conversion_actions.toml")
    return Settings(tuple(clients), tuple(mapped), tuple(actions))
