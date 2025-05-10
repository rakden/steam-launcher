#!/usr/bin/env python

import datetime
import http.client
import json
import logging
import os
import pathlib
import random
import re
import sqlite3
import subprocess
import time
import tomllib
from typing import TypedDict, cast
import urllib.request

LOGLEVEL = "INFO"
CONFIG_FILE_NAME = "config.toml"

DB_NAME = "steam_launcher.db"
DB_TABLE_NAME = "games"

API_URL_STEAM = "https://store.steampowered.com/api/appdetails?appids="
CMD_USER_SELECT = ["rofi", "-dmenu", "-i", "-matching", "normal", "-p", " "]
CMD_LAUNCH_GAME = ["steam", "-applaunch"]


class Config(TypedDict):
    libraryfolders_path: str
    blacklist: list[int]


def read_config(filepath: pathlib.Path) -> Config:
    with open(filepath, "rb") as file:
        config_data = tomllib.load(file)
    return cast(Config, cast(object, config_data))


def get_timestamp() -> datetime.datetime:
    return datetime.datetime.now()


def adapt_datetime(val: datetime.datetime) -> str:
    return val.isoformat()


def convert_datetime(val: bytes) -> datetime.datetime:
    return datetime.datetime.fromisoformat(val.decode())


sqlite3.register_adapter(datetime.datetime, adapt_datetime)
sqlite3.register_converter("timestamp", convert_datetime)


def setup_db(con: sqlite3.Connection) -> None:
    with con:
        _ = con.execute(
            f"CREATE TABLE IF NOT EXISTS {DB_TABLE_NAME}(appid INTEGER PRIMARY KEY, name TEXT, timestamp timestamp)"
        )
    logging.debug("database setup")


def get_file_appids(file_path: pathlib.Path) -> list[int]:
    appids: list[int] = list()
    appid_re = re.compile(r"^\"(\d+)\".*\"\d+\"$")
    with open(file_path) as file:
        for line in file:
            text = line.strip()
            match = appid_re.fullmatch(text)
            if match:
                appids.append(int(match.group(1)))

    logging.debug(f"{len(appids)} file appids: {appids}")
    return appids


def get_db_appids(con: sqlite3.Connection) -> list[int]:
    res = con.execute(f"SELECT appid FROM {DB_TABLE_NAME}")
    appids = list(map(lambda x: int(x[0]), res.fetchall()))  # pyright: ignore[reportAny]
    logging.debug(f"{len(appids)} database appids: {appids}")
    return appids


def get_new_appids(file_appids: set[int], db_appids: set[int]) -> list[int]:
    diff = list(file_appids.difference(db_appids))
    logging.debug(f"{len(diff)} differing appids: {diff}")
    return diff


def get_old_appids(file_appids: set[int], db_appids: set[int]) -> list[int]:
    diff = list(db_appids.difference(file_appids))
    logging.debug(f"{len(diff)} old appids: {diff}")
    return diff


def strip_blacklisted_appids(appids: set[int], blacklist: set[int]) -> list[int]:
    ids = appids
    logging.debug(f"{len(ids)} appids before stripping blacklisted items: {list(ids)}")
    ids = ids.difference(blacklist)
    logging.debug(f"{len(ids)} appids after stripping blacklisted items: {list(ids)}")
    return list(ids)


def request_app_name(appid: int, max_tries: int = 3) -> str:
    for i in range(1, max_tries + 1):
        res: http.client.HTTPResponse = urllib.request.urlopen(
            f"{API_URL_STEAM}{appid}"
        )
        if not res.status == 200:
            raise RuntimeError(
                f"Status {res.status} when getting name of appid {appid}"
            )
        res_obj = json.loads(res.read())  # pyright: ignore[reportAny]
        success: bool = res_obj[str(appid)]["success"]
        if not success:
            delay = 1.0 + random.random() * (i**2)
            logging.warning(
                f"attempt {i}: could not request name for appid {appid}. retrying in {delay}"
            )
            time.sleep(delay)
            continue
        name = str(res_obj[str(appid)]["data"]["name"]).strip()  # pyright: ignore[reportAny]
        logging.info(f"name for appid {appid}: {name}")
        return name

    return ""


def update_db(con: sqlite3.Connection, appids: list[int]) -> None:
    logging.debug(f"update database with appids: {appids}")
    entries: list[tuple[int, str, datetime.datetime]] = list()
    for appid in appids:
        app_name = request_app_name(appid)
        if len(app_name) == 0:
            logging.error(f"unable to request name for appid {appid}")
            continue
        entry = (appid, app_name, get_timestamp())
        logging.debug(f"database entry: {entry}")
        entries.append(entry)
        time.sleep(random.random() * 5)

    with con:
        cur = con.executemany(f"INSERT INTO {DB_TABLE_NAME} VALUES(?, ?, ?)", entries)
        logging.info(f"added {cur.rowcount} entries to database: {entries}")


def clean_db(con: sqlite3.Connection, appids: list[int]) -> None:
    logging.debug(f"clean database appids: {appids}")
    entries = list(map(lambda x: (str(x),), appids))
    with con:
        cur = con.executemany(f"DELETE FROM {DB_TABLE_NAME} WHERE appid = ?", entries)
        logging.info(f"removed {cur.rowcount} entries to database: {entries}")


def get_game_entries(con: sqlite3.Connection) -> dict[str, int]:
    entries: dict[str, int] = dict()
    with con:
        for row in con.execute(  # pyright: ignore[reportAny]
            f"SELECT name, appid FROM {DB_TABLE_NAME} ORDER BY timestamp DESC NULLS LAST"
        ):
            entries[str(row[0])] = int(row[1])  # pyright: ignore[reportAny]

    logging.debug(f"game entries from database: {entries}")
    return entries


def select_entry(entries: list[str]) -> str:
    assert len(entries) > 0, "no entries provided"
    logging.debug(f"rofi entries: {entries}")
    rofi_input = "\n".join(entries)
    process = subprocess.run(
        [*CMD_USER_SELECT],
        input=rofi_input,
        capture_output=True,
        text=True,
    )
    selection = process.stdout.strip()
    logging.debug(f"user selection: '{selection}'")
    return selection


def launch_game(appid: int, con: sqlite3.Connection) -> None:
    logging.debug(f"launching appid: {appid}")
    process = subprocess.run([*CMD_LAUNCH_GAME, str(appid)])
    if process.returncode != 0:
        logging.critical(f"could not launch appid: {appid}")
        process.check_returncode()
    else:
        with con:
            timestamp = get_timestamp()
            cur = con.execute(
                f"UPDATE {DB_TABLE_NAME} SET timestamp = ? WHERE appid = ?",
                (timestamp, appid),
            )
            logging.info(
                f"updated {cur.rowcount} timestamp for appid {appid}: {adapt_datetime(timestamp)}"
            )


def main(
    db_con: sqlite3.Connection,
    lib_path: pathlib.Path | None,
    blacklist: list[int] | None = None,
) -> None:
    setup_db(db_con)

    if lib_path:
        file_appids = set(get_file_appids(lib_path))
        db_appids = set(get_db_appids(db_con))
        new_appids = get_new_appids(file_appids, db_appids)
        old_appids = get_old_appids(file_appids, db_appids)
        if blacklist:
            new_appids = strip_blacklisted_appids(set(new_appids), set(blacklist))
        if new_appids:
            update_db(db_con, new_appids)
        if old_appids:
            clean_db(db_con, old_appids)
    else:
        logging.info("skipping database update")

    games = get_game_entries(db_con)
    if not games:
        logging.critical("no entries in database")
        return
    selection = select_entry(list(games.keys()))
    if not selection:
        logging.info("user selection empty; returning")
        return

    appid = games[selection]
    logging.debug(f"appid of selected game: '{appid}'")

    launch_game(appid, db_con)


if __name__ == "__main__":
    level = str(os.environ.get("LOGLEVEL", LOGLEVEL)).upper()
    logging.basicConfig(
        format="%(asctime)s - %(levelname)s - %(message)s",
        level=level,
    )
    logging.debug("started")

    base_path = pathlib.Path(os.path.realpath(__file__)).parent
    logging.debug(f"base path: {base_path}")

    config_path = base_path / CONFIG_FILE_NAME
    logging.debug(f"config file path: {config_path}")
    assert config_path.is_file(), f"no config file at {config_path}"

    config = read_config(config_path)
    logging.debug(f"read config: {config}")

    libraryfolders_path = pathlib.Path(
        os.path.expandvars(config.get("libraryfolders_path", ""))
    ).resolve()
    logging.debug(f"libraryfolders.vdf path: {libraryfolders_path}")
    if not (libraryfolders_path.is_file() and libraryfolders_path.suffix == ".vdf"):
        logging.warning(f"invalid libraryfolders path: '{libraryfolders_path}'")
        libraryfolders_path = None

    db_path = base_path / DB_NAME
    logging.debug(f"database path: {db_path}")
    con = sqlite3.connect(db_path, detect_types=sqlite3.PARSE_DECLTYPES)
    assert db_path.is_file(), f"no database file at {db_path}"

    blacklist = config.get("blacklist", [])
    logging.debug(f"blacklist: '{blacklist}'")

    main(con, libraryfolders_path, blacklist)

    logging.debug("closing database connection")
    con.close()
    logging.debug("database connection closed")

    logging.debug("exiting")
