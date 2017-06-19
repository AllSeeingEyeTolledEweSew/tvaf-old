import better_bencode
import collections
import contextlib
import errno
import itertools
import logging
import re
import os
import urllib

import btn
import plex.scanners.Common.Stack as plex_stack
import plex.scanners.Common.VideoFiles as plex_video_files
import importlib
plex_series_scanner = importlib.import_module(
    "plex.scanners.Series.Plex Series Scanner")
from yatfs import inodb as inodb_lib


PLEX_EPISODE_REGEXPS = tuple(
    re.compile(r, flags=re.IGNORECASE)
    for r in plex_series_scanner.episode_regexps)
PLEX_DATE_REGEXPS = tuple(
    re.compile(r) for r in plex_series_scanner.date_regexps)
PLEX_STANDALONE_EPISODE_REGEXPS = tuple(
    re.compile(r) for r in plex_series_scanner.standalone_episode_regexs)
PLEX_JUST_EPISODE_REGEXPS = tuple(
    re.compile(r, flags=re.IGNORECASE)
    for r in plex_series_scanner.just_episode_regexs)
PLEX_STARTS_WITH_EPISODE_NUMBER_REGEX = re.compile(r"^[0-9]+[ -]")

TVDB_EPISODE_PATH_REGEX = re.compile(
    r"(?P<path>/tvdb_series=(?P<series>\d+)/tvdb_season=(?P<season>\d+)/"
    r"tvdb_episode=(?P<episode>\d+))")
BTN_TORRENT_ENTRY_PATH_REGEX = re.compile(
    r"/btn_torrent_entry_id=(?P<torrent_entry_id>\d+)$")

BTN_FULL_SEASON_REGEX = re.compile(r"Season (?P<season>\d+)$")
BTN_EPISODE_REGEX = re.compile(r"S(?P<season>\d+)(?P<episodes>(E\d+)+)$")
BTN_EPISODE_PART_REGEX = re.compile(r"E(?P<episode>\d+)")
BTN_DATE_EPISODE_REGEX = re.compile(
    r"(?P<y>\d{4})[\.-](?P<m>\d\d)[\.-](?P<d>\d\d)$")
BTN_SEASON_PARTIAL_REGEXES = (
    re.compile(r"Season (?P<season>\d+)"),
    re.compile(r"(?P<season>\d{4})[\.-]\d\d[\.-]\d\d"),
    re.compile(r"S(?P<season>\d+)(E\d+)+"))
VERSION_PATH_REGEX = re.compile(
    r"(?P<base>.+)/version_id=(?P<version_id>[^/]+)$")


def log():
    return logging.getLogger(__name__)


class FileInfo(object):

    def __init__(self, path, length):
        self.path = path
        self.length = length


class MediaItem(object):

    def __init__(self, details, parts):
        self.details = details
        self.parts = parts


class FileNamesScanner(object):

    def __init__(self, file_infos, known_season=None, known_strings=None):
        self.file_infos = file_infos
        self.should_stack = True
        self.known_season = known_season
        self.known_strings = known_strings or []

    def scan_filename(self, name):
        log().debug("input: %s", name)
        name, ext = os.path.splitext(name)
        
        if not any(rx.search(name) for rx in itertools.chain(
                PLEX_EPISODE_REGEXPS[:-1],
                PLEX_STANDALONE_EPISODE_REGEXPS)):
            log().debug("...trying dates")
            for rx in PLEX_DATE_REGEXPS:
                m = rx.search(name)
                if not m:
                    continue
                log().debug("......matched date: %s", rx)

                y, m, d = (
                    int(m.group("year")), int(m.group("month")),
                    int(m.group("day")))
                yield dict(date="%04d-%02d-%02d" % (y, m, d))
                return

        _, year = plex_video_files.CleanName(name)
        if year != None:
            log().debug("...removing year: %s", year)
            name = name.replace(str(year), "XXXX")

        for s in self.known_strings:
            name = re.sub(re.escape(s), " ", name, flags=re.IGNORECASE)

        cleanName, _ = plex_video_files.CleanName(name)

        log().debug("...cleaned name to: %s", name)

        for i, rx in enumerate(PLEX_EPISODE_REGEXPS):
            m = rx.search(name)
            if not m:
                continue
            s = m.group("season")
            s = 0 if s.lower() == "sp" else int(s)
            e_start = int(m.group("ep"))
            e_end = e_start
            if "secondEp" in m.groupdict() and m.group("secondEp"):
                e_end = int(m.group("secondEp"))

            log().debug("......matches %s: (%s, %s, %s)", rx, s, e_start,
                    e_end)

            if i == len(PLEX_EPISODE_REGEXPS) - 1:
                if s == 0:
                    log().debug(".........weak rx season 0, ignoring")
                    continue
                if self.known_season is not None and s != self.known_season:
                    if PLEX_STARTS_WITH_EPISODE_NUMBER_REGEX.match(name):
                        log().debug(".........weak rx season mismatch looks like episode, ignoring")
                        continue
                    log().debug(".........weak rx season mismatch assuming 100s")
                    e_start = s * 100 + e_start
                    if e_end:
                        e_end = s * 100 + e_end
                    s = None
                      
            for e in range(e_start, e_end + 1):
                d = dict(episode=e, filename=cleanName)
                if s is not None:
                    d["season"] = s
                if e_start != e_end:
                    d["range"] = (
                        e / (e_end - e_start + 1),
                        (e + 1) / (e_end - e_start + 1))
                yield d
            return

        name = cleanName

        log().debug("...further cleaned to: %s", name)

        for i, rx in enumerate(PLEX_JUST_EPISODE_REGEXPS):
            m = rx.search(name)
            if not m:
                continue
            e = int(m.group("ep"))
            log().debug("......matched %s: %s", rx, e)
            s = None
            if self.known_season:
                s = self.known_season
                if e >= 100 and e // 100 == s:
                    log().debug(".........matched known season 100s")
                    e = e % 100

            if i == 0:
                self.should_stack = False

            d = dict(episode=e, filename=name)
            if s is not None:
                d["season"] = s
            yield d
            return

        log().debug("...got nothing.")
        yield dict(filename=name)

    def scan(self):
        mis = []
        for fi in self.file_infos:
            name = os.path.basename(fi.path)
            for d in self.scan_filename(name):
                mis.append(MediaItem(d, [fi.path]))
        if self.should_stack:
            plex_stack.Scan(None, None, mis, None)
        return mis


def get_episodes_by_date(tvafdb, series_id, date):
    paths = tvafdb.search((
        ("tvdb_episode_series_id", series_id),
        ("tvdb_episode_firstAired", date)))
    season_paths = set()
    for path in paths:
        m = TVDB_EPISODE_PATH_REGEX.match(path)
        if m:
            e = int(m.group("episode"))
            s = int(m.group("season"))
            season_paths.add((e, s, m.group("path")))
    def key(i):
        e, s, p = i
        return (s == 0, s, e)
    return list(p for e, s, p in sorted(season_paths, key=key))


def raw_torrent_file_infos(raw_torrent):
    info = better_bencode.loads(raw_torrent)[b"info"]
    if b"files" in info:
        return [
            {b"path": [info[b"name"]] + f[b"path"],
             b"length": f[b"length"]}
            for f in info[b"files"]]
    return [{
        b"path": [info[b"name"]],
        b"length": info[b"length"]}]


class BTNTorrentEntryScanner(object):

    def __init__(self, tvafdb, torrent_entry):
        self.tvafdb = tvafdb
        self.torrent_entry = torrent_entry

        self.known_season = btn_group_season(torrent_entry.group)
        self.exact_season = BTN_FULL_SEASON_REGEX.match(
            self.torrent_entry.group.name)

    def translate_details(self, details, filepaths):
        base_path = btn_season_group_labeled_base_path(
            self.torrent_entry.group)
        if "date" in details:
            if self.torrent_entry.group.series.tvdb_id:
                paths = get_episodes_by_date(
                    self.tvafdb, self.torrent_entry.group.series.tvdb_id,
                    details["date"])
                fallback_match = None
                for path in paths:
                    if self.known_season is not None:
                        m = TVDB_EPISODE_PATH_REGEX.match(path)
                        s = int(m.group("season"))
                        if s != self.known_season:
                            if not fallback_match:
                                fallback_match = path
                            continue
                    return path
                log().debug(
                    "te %d %s: %s: date %s had no good matches",
                    self.torrent_entry.id, self.torrent_entry.group.name,
                    filepaths, details["date"])
                if fallback_match is not None:
                    log().debug(
                        "te %d %s: %s: date %s falling back to %s",
                        self.torrent_entry.id, self.torrent_entry.group.name,
                        filepaths, details["date"], fallback_match)
                    return fallback_match
            return base_path + "/date=" + details["date"]
        if "episode" in details:
            if ("season" in details and self.exact_season and 
                    self.known_season != details["season"]):
                log().debug(
                    "te %d %s: %s: matched season %d", self.torrent_entry.id,
                    self.torrent_entry.group.name, filepaths,
                    details["season"])
                details["season"] = self.known_season
            if self.exact_season:
                if self.torrent_entry.group.series.tvdb_id:
                    key = "tvdb_episode"
                else:
                    key = "btn_episode"
            elif "filename" in details:
                return base_path + "/filename=" + details["filename"]
            else:
                key = "episode"
            return base_path + "/" + key + "=" + str(details["episode"])
        if "filename" in details:
            return base_path + "/filename=" + details["filename"]

    def scan(self):
        file_infos = []
        for fi in raw_torrent_file_infos(self.torrent_entry.raw_torrent):
            path = os.path.join(*fi[b"path"]).decode()
            if self.torrent_entry.container not in ("", "---"):
                _, ext = os.path.splitext(path)
                ext = ext.lower()
                ext = {".mpg": ".mpeg"}.get(ext) or ext
                if ext != "." + self.torrent_entry.container.lower():
                    continue
            file_infos.append(FileInfo(path, fi[b"length"]))

        if self.torrent_entry.group.category == "Episode":
            parts = [fi.path for fi in file_infos]
            for path in btn_episode_group_labeled_paths(
                    self.tvafdb, self.torrent_entry.group):
                yield MediaItem(path, parts)

        if self.torrent_entry.group.category == "Season":
            known_strings = [
                self.torrent_entry.codec, self.torrent_entry.resolution,
                self.torrent_entry.source]

            if self.torrent_entry.source in (
                    "Bluray", "BD50", "BDRip", "BRRip"):
                known_strings.extend([
                    "blu-ray", "bluray", "blu ray", "blu_ray", "blu.ray",
                    "blue-ray", "blueray", "blue ray", "blue_ray",
                    "blue.ray"])
            if self.torrent_entry.source in ("WEB-DL", "WEBRip"):
                known_strings.append("WEB")

            scanner = FileNamesScanner(
                file_infos, known_season=self.known_season,
                known_strings=known_strings)
            mis = scanner.scan()

            for mi in mis:
                mi.details = self.translate_details(mi.details, mi.parts)
                yield mi


def btn_series_path(series):
    if series.tvdb_id:
        return "/tvdb_series=%d" % series.tvdb_id
    else:
        return "/btn_series=%d" % series.id


def btn_season_group_labeled_base_path(group):
    assert group.category == "Season", group
    series_path = btn_series_path(group.series)
    season_key = "tvdb_season" if group.series.tvdb_id else "btn_season"
    m = BTN_FULL_SEASON_REGEX.match(group.name)
    if m:
        s = int(m.group("season"))
        return "%s/%s=%d" % (series_path, season_key, s)
    s = btn_group_season(group)
    if s is not None:
        return "%s/%s=%d/btn_season_group=%d" % (
                series_path, season_key, s, group.id)
    return "%s/btn_season_group=%d" % (series_path, group.id)


def btn_episode_group_labeled_paths(tvafdb, group):
    assert group.category == "Episode", group
    series_path = btn_series_path(group.series)
    season_key = "tvdb_season" if group.series.tvdb_id else "btn_season"
    episode_key = "tvdb_episode" if group.series.tvdb_id else "btn_episode"
    m = BTN_EPISODE_REGEX.match(group.name)
    if m:
        s = int(m.group("season"))
        episodes = m.group("episodes")
        episodes = [
            int(e)
            for e in BTN_EPISODE_PART_REGEX.findall(episodes)]
        if not any(e == 0 for e in episodes):
            for e in episodes:
                yield "%s/%s=%d/%s=%d" % (
                    series_path, season_key, s, episode_key, e)
            return
    m = BTN_DATE_EPISODE_REGEX.match(group.name)
    if m:
        y, m, d = (int(m.group("y")), int(m.group("m")), int(m.group("d")))
        date = "%04d-%02d-%02d" % (y, m, d)
        if group.series.tvdb_id:
            paths = get_episodes_by_date(tvafdb, group.series.tvdb_id, date)
            if paths:
                real_paths = [
                    p for p in paths
                    if int(TVDB_EPISODE_PATH_REGEX.match(p).group("season"))
                    != 0]
                if len(real_paths) > 1:
                    log().debug(
                        "group %d: (%d, %s) -> multiple episodes: %s",
                        group.id, group.series.tvdb_id, date, paths)
                yield paths[0]
                return
        yield "%s/%s=%d/date=%s" % (series_path, season_key, y, date)
        return
    s = btn_group_season(group)
    if s is not None:
        yield "%s/%s=%d/btn_episode_group=%d" % (
                series_path, season_key, s, group.id)
        return
    yield "%s/btn_episode_group=%d" % (series_path, group.id)


def btn_group_labeled_base_paths(tvafdb, group):
    if group.category == "Episode":
        for path in btn_episode_group_labeled_paths(tvafdb, group):
            yield path

    if group.category == "Season":
        yield btn_season_group_labeled_base_path(group)


def btn_group_season(group):
    for rx in BTN_SEASON_PARTIAL_REGEXES:
        m = rx.match(group.name)
        if m:
            return int(m.group("season"))


@contextlib.contextmanager
def begin(db):
    db.cursor().execute("begin immediate")
    try:
        yield
    except:
        db.cursor().execute("rollback")
        raise
    else:
        db.cursor().execute("commit")


class Syncer(object):

    def __init__(self, tvafdb, btnapi):
        self.tvafdb = tvafdb
        self.btnapi = btnapi

    def paths_for_tvdb_season(self, series_id, s):
        episode_paths = self.tvafdb.search((
            ("tvdb_episode_seriesId", series_id),
            ("tvdb_episode_airedSeason", s)))
        episodes = set()
        for path in episode_paths:
            m = TVDB_EPISODE_PATH_REGEX.match(path)
            if m:
                episodes.add(int(m.group("episode")))
        for e in episodes:
            yield "/tvdb_series=%d/tvdb_season=%d/tvdb_episode=%d" % (
                series_id, s, e)

    def season_torrent_entry_mediaitems(self, te):
        if te.container in ("VOB", "ISO", "M2TS"):
            path = btn_season_group_labeled_base_path(te.group)
            yield MediaItem(path, ["."])
            return

        if te.raw_torrent_cached:
            scanner = BTNTorrentEntryScanner(self.tvafdb, te)
            for mi in scanner.scan():
                yield mi
            return

        if te.group.series.tvdb_id:
            m = BTN_FULL_SEASON_REGEX.match(te.group.name)
            if m:
                s = int(m.group("season"))
                for path in self.paths_for_tvdb_season(
                        te.group.series.tvdb_id, s):
                    yield MediaItem(path, None)

    def episode_torrent_entry_mediaitems(self, te):
        if te.raw_torrent_cached:
            scanner = BTNTorrentEntryScanner(self.tvafdb, te)
            for mi in scanner.scan():
                yield mi
        else:
            for path in btn_episode_group_labeled_paths(self.tvafdb, te.group):
                yield MediaItem(path, None)

    def torrent_entry_mediaitems(self, te):
        if te.group.category == "Episode":
            return self.episode_torrent_entry_mediaitems(te)
        if te.group.category == "Season":
            return self.season_torrent_entry_mediaitems(te)

    def torrent_entry_version_mediaitems(self, te):
        for mi in self.torrent_entry_mediaitems(te):
            vd = {"btn_torrent_entry_id": te.id}
            if te.group.category == "Season":
                if not mi.parts:
                    vd["files"] = "resolveme"
                elif mi.parts == ["."]:
                    pass
                else:
                    assert te.raw_torrent_cached
                    path_to_index = {
                        os.path.join(*fi[b"path"]).decode(): i
                        for i, fi in enumerate(
                            raw_torrent_file_infos(te.raw_torrent))}
                    vd["files"] = ",".join(
                        str(path_to_index[p]) for p in mi.parts)
            version_id = urllib.urlencode(sorted(vd.iteritems()))
            mi = MediaItem(
                mi.details + "/version_id=%s" % version_id, mi.parts)
            yield mi

    def delete_torrent_entry_at_path(self, path, ts):
        log().info("Deleting torrent entry data at %s.", path)
        data = self.tvafdb.get(path)
        delete_keys = set(
            k for k in data.iterkeys()
            if (k.startswith("btn_torrent_entry_") or
                k.startswith("bittorrent_")))
        self.tvafdb.delete(path, keys=delete_keys, timestamp=ts)

    def delete_version_at_path(self, path, ts):
        log().info("Deleting version at %s.", path)
        data = self.tvafdb.get(path)
        delete_keys = set(
            k for k in data.iterkeys()
            if k.startswith("version_"))
        self.tvafdb.delete(path, keys=delete_keys, timestamp=ts)

    def delete_torrent_entry(self, id, ts):
        for existing_path in self.tvafdb.search(btn_torrent_entry_id=id):
            self.delete_torrent_entry_at_path(existing_path, ts)
        for existing_path in self.tvafdb.search(
                version_bittorrent_path="/btn_torrent_entry_id=%d" % id):
            self.delete_version_at_path(existing_path, ts)

    def update_torrent_entry(self, id, ts):
        te = self.btnapi._from_db(id)
        path = "/btn_torrent_entry_id=%d" % id
        path_to_mi = {
            mi.details: mi for mi in self.torrent_entry_version_mediaitems(te)}
        for existing_path in self.tvafdb.search(
                version_bittorrent_path="/btn_torrent_entry_id=%d" % id):
            if existing_path not in path_to_mi:
                self.delete_version_at_path(existing_path, ts)
        for existing_path in self.tvafdb.search(btn_torrent_entry_id=id):
            if existing_path != path:
                self.delete_torrent_entry_at_path(existing_path, ts)
        data = {
            "btn_torrent_entry_id": te.id,
            "btn_torrent_entry_codec": te.codec,
            "btn_torrent_entry_container": te.container,
            "bittorrent_leechers": te.leechers,
            "btn_torrent_entry_origin": te.origin,
            "btn_torrent_entry_release_name": te.release_name,
            "btn_torrent_entry_resolution": te.resolution,
            "bittorrent_seeders": te.seeders,
            "bittorrent_snatches": te.snatched,
            "bittorrent_size": te.size,
            "btn_torrent_entry_source": te.source,
            "bittorrent_info_hash": te.info_hash.lower(),
            "btn_torrent_entry_time": te.time,
            "btn_torrent_entry_raw_torrent_cached": te.raw_torrent_cached,
        }
        self.tvafdb.update(path, data, timestamp=ts)
        for version_path, mi in path_to_mi.iteritems():
            version_id = VERSION_PATH_REGEX.match(
                version_path).group("version_id")
            version_data = {
                "version_id": version_id,
                "version_bittorrent_path": "/btn_torrent_entry_id=%d" % id,
                "version_bittorrent_files": mi.parts,
            }
            self.tvafdb.update(version_path, version_data, timestamp=ts)

    def delete_group_at_path(self, path, ts):
        log().info("Deleting group data at %s.", path)
        data = self.tvafdb.get(path)
        delete_keys = set(
            k for k in data.iterkeys()
            if k.startswith("btn_group_"))
        self.tvafdb.delete(path, keys=delete_keys, timestamp=ts)

    def update_group(self, id, ts):
        group = btn.Group._from_db(self.btnapi, id)
        paths = set(btn_group_labeled_base_paths(self.tvafdb, group))
        paths = set(
            p for p in paths
            if re.match(r".*/btn_(episode|season)_group=\d+$", p))
        for existing_path in self.tvafdb.search(btn_group_id=id):
            if existing_path not in paths:
                self.delete_group_at_path(existing_path, ts)
        for path in paths:
            data = {
                "btn_group_category": group.category,
                "btn_group_id": id,
                "btn_group_name": group.name}
            self.tvafdb.update(path, data, timestamp=ts)

    def delete_series_at_path(self, path, ts):
        log().info("Deleting series data at %s.", path)
        data = self.tvafdb.get(path)
        delete_keys = set(
            k for k in data.iterkeys()
            if k.startswith("btn_series_"))
        self.tvafdb.delete(path, keys=delete_keys, timestamp=ts)

    def update_series(self, id, ts):
        series = btn.Series._from_db(self.btnapi, id)
        path = btn_series_path(series)
        for existing_path in self.tvafdb.search(btn_series_id=id):
            if existing_path != path:
                self.delete_series_at_path(existing_path, ts)
        data = {
            "btn_series_id": id,
            "btn_series_banner_url": series.banner,
            "btn_series_poster_url": series.poster,
            "btn_series_youtube_trailer_url": series.youtube_trailer,
        }
        if not series.tvdb_id:
            data["btn_series_name"] = series.name
        self.tvafdb.update(path, data, timestamp=ts)

    def sync(self):
        with begin(self.tvafdb.db):
            btn_ts = self.tvafdb.get_global("btn_sync_btn_timestamp") or 0
            old_tvaf_ts = self.tvafdb.get_global(
                "btn_sync_tvaf_timestamp") or 0
            update_tvdb_series_id_seasons = set()
            log().info("Getting list of updated tvdb seasons.")
            for crud in self.tvafdb.feed(
                    timestamp=old_tvaf_ts, keys=["tvdb_episode_id"]):
                m = TVDB_EPISODE_PATH_REGEX.match(crud.path)
                if m:
                    series = int(m.group("series"))
                    season = int(m.group("season"))
                    update_tvdb_series_id_seasons.add((series, season))
            log().info(
                "Will update %d seasons due to changed episodes.",
                len(update_tvdb_series_id_seasons))
            log().info("Getting list of air-date-updated series.")
            update_tvdb_series_ids = set()
            for crud in self.tvafdb.feed(
                    timestamp=old_tvaf_ts, keys=["tvdb_episode_firstAired"]):
                m = TVDB_EPISODE_PATH_REGEX.match(crud.path)
                if m:
                    series = int(m.group("series"))
                    update_tvdb_series_ids.add(series)
            if update_tvdb_series_ids:
                log().info(
                    "Will update %d tvdb series due to updated air dates.",
                    len(update_tvdb_series_ids))
            with self.btnapi.db:
                update_torrent_entry_ids = set()
                update_group_ids = set()
                update_series_ids = set()
                delete_torrent_entry_ids = set()

                for crud in self.btnapi.feed(timestamp=btn_ts):
                    if crud.action == crud.ACTION_DELETE:
                        if crud.type == crud.TYPE_TORRENT_ENTRY:
                            delete_torrent_entry_ids.add(crud.id)
                    else:
                        if crud.type == crud.TYPE_TORRENT_ENTRY:
                            update_torrent_entry_ids.add(crud.id)
                        elif crud.type == crud.TYPE_GROUP:
                            update_group_ids.add(crud.id)
                        elif crud.type == crud.TYPE_SERIES:
                            update_series_ids.add(crud.id)

                if update_tvdb_series_ids:
                    c = self.btnapi.db.cursor()
                    c.execute(
                        "create temp table tvdb_id ("
                        "tvdb_id integer not null primary key)")
                    c.executemany(
                        "insert into temp.tvdb_id (tvdb_id) values (?)",
                        [(i,) for i in update_tvdb_series_ids])
                    c.execute(
                        "select series.id from temp.tvdb_id "
                        "left join series "
                        "where series.tvdb_id = temp.tvdb_id.tvdb_id "
                        "and not deleted")
                    for id, in c:
                        update_series_ids.add(id)
                    c.execute("drop table temp.tvdb_id")

                if update_series_ids:
                    c = self.btnapi.db.cursor()
                    c.execute(
                        "create temp table series_id ("
                        "series_id integer not null primary key)")
                    c.executemany(
                        "insert into temp.series_id (series_id) values (?)",
                        [(i,) for i in update_series_ids])
                    c.execute(
                        "select id from temp.series_id "
                        "left join torrent_entry_group "
                        "where torrent_entry_group.series_id = "
                        "temp.series_id.series_id "
                        "and not deleted")
                    for id, in c:
                        update_group_ids.add(id)
                    c.execute("drop table temp.series_id")

                if update_group_ids:
                    c = self.btnapi.db.cursor()
                    c.execute(
                        "create temp table group_id ("
                        "group_id integer not null primary key)")
                    c.executemany(
                        "insert into temp.group_id (group_id) values (?)",
                        [(i,) for i in update_group_ids])
                    c.execute(
                        "select id from temp.group_id "
                        "left join torrent_entry "
                        "where torrent_entry.group_id = "
                        "temp.group_id.group_id "
                        "and not deleted")
                    for id, in c:
                        update_torrent_entry_ids.add(id)
                    c.execute("drop table temp.group_id")

                if update_tvdb_series_id_seasons:
                    c = self.btnapi.db.cursor()
                    c.execute(
                        "create temp table tvdb_id_group_name ("
                        "tvdb_id integer not null, "
                        "group_name text not null, "
                        "primary key (tvdb_id, group_name)) "
                        "without rowid")
                    c.executemany(
                        "insert into temp.tvdb_id_group_name "
                        "(tvdb_id, group_name) values "
                        "(?, ?)",
                        [(i, "Season %d" % s)
                         for i, s in update_tvdb_series_id_seasons])
                    c.execute(
                        "select torrent_entry.id from temp.tvdb_id_group_name "
                        "left join series "
                        "inner join torrent_entry_group "
                        "inner join torrent_entry "
                        "where torrent_entry_group.id = "
                        "torrent_entry.group_id "
                        "and series.id = torrent_entry_group.series_id "
                        "and series.tvdb_id = temp.tvdb_id_group_name.tvdb_id "
                        "and torrent_entry_group.name = "
                        "temp.tvdb_id_group_name.group_name "
                        "and not torrent_entry.deleted")
                    for id, in c:
                        update_torrent_entry_ids.add(id)
                    c.execute("drop table temp.tvdb_id_group_name")

                tvaf_ts = self.tvafdb.tick()
                if update_series_ids:
                    log().info("Updating %d series.", len(update_series_ids))
                for id in update_series_ids:
                    self.update_series(id, tvaf_ts)
                if update_group_ids:
                    log().info("Updating %d groups.", len(update_group_ids))
                for id in update_group_ids:
                    self.update_group(id, tvaf_ts)
                if delete_torrent_entry_ids:
                    log().info(
                        "Deleting %d torrent entries.",
                        len(delete_torrent_entry_ids))
                for id in delete_torrent_entry_ids:
                    self.delete_torrent_entry(id, tvaf_ts)
                if update_torrent_entry_ids:
                    log().info(
                        "Updating %d torrent entries.",
                        len(update_torrent_entry_ids))
                for id in update_torrent_entry_ids:
                    self.update_torrent_entry(id, tvaf_ts)

                btn_ts = self.btnapi.get_changestamp()

            self.tvafdb.set_global("btn_sync_btn_timestamp", btn_ts)
            self.tvafdb.set_global("btn_sync_tvaf_timestamp", tvaf_ts)


class FsSyncer(object):

    def __init__(self, tvafdb, inodb, btnapi, mountpoint):
        self.tvafdb = tvafdb
        self.inodb = inodb
        self.btnapi = btnapi
        self.mountpoint = mountpoint
        self.update_torrent_entry_ids = set()

    def visit_crud_unlocked(self, crud):
        m = BTN_TORRENT_ENTRY_PATH_REGEX.match(crud.path)
        if m:
            id = int(m.group("torrent_entry_id"))
            if crud.action == crud.ACTION_DELETE:
                self.delete_torrent_entry_ids.add(id)
            else:
                self.update_torrent_entry_ids.add(id)
            return

    def mkdir_p(self, path):
        parent = inodb_lib.ROOT_INO
        ino = parent
        for name in self.inodb._split(path):
            parent = ino
            try:
                ino = self.inodb.lookup_ino(parent, name)
            except OSError as e:
                if e.errno == errno.ENOENT:
                    ino = self.inodb.mkdir_ino(parent, name, 0o555, 0, 0)
                else:
                    raise
            yield parent, name, ino

    def update_torrent_entry_id_unlocked(self, id):
        te = self.btnapi.getTorrentByIdCached(id)
        assert te
        assert te.raw_torrent_cached
        hash = te.info_hash.lower()
        os.symlink(
            os.path.basename(te.raw_torrent_path),
            os.path.join(
                os.path.dirname(te.raw_torrent_path), "%s.torrent" % hash))
        base_path = "/by-id/%d" % id
        for i, fi in enumerate(raw_torrent_file_infos(te.raw_torrent)):
            path = os.path.join(*fi[b"path"]).decode()
            path = os.path.join(base_path, path)
            for idx, (parent, name, ino) in enumerate(
                    self.mkdir_p(os.path.dirname(path))):
                if idx != 0:
                    self.inodb.setattr_ino(
                        ino, st_ctime=te.time, st_mtime=te.time)
                parent = ino
            try:
                ino = self.inodb.mkfile_ino(
                    parent, os.path.basename(path), 0o444, hash, i,
                    fi[b"length"], 0, 0)
            except OSError as e:
                if e.errno != errno.EEXIST:
                    raise e
                ino = self.inodb.lookup(path)
            self.inodb.setattr_ino(ino, st_ctime=te.time, st_mtime=te.time)

    def unlink_recursive(self, parent, name, ino):
        for child_name, child_ino in self.inodb.readdir_ino(ino):
            self.unlink_recursive(ino, child_name, child_ino)
        self.inodb.unlink_ino(parent, name)

    def delete_torrent_entry_id_unlocked(self, id):
        path = "/by-id/%d" % id
        try:
            parent, name, ino = self.inodb.lookup_dirent(path)
        except OSError as e:
            if e.errno == errno.ENOENT:
                return
            raise e
        self.unlink_recursive(parent, name, ino)

    def sync_inodb_unlocked(self):
        last_ts = self.inodb.get_global("tvaf_timestamp")
        log().info("Getting list of updated BTN torrent entries.")
        for crud in self.tvafdb.feed(keys=(
                "btn_torrent_entry_id",
                "btn_torrent_entry_raw_torrent_cached"), timestamp=last_ts):
            self.visit_crud_unlocked(crud)
        num_no_te = 0
        num_torrent_not_cached = 0
        if self.delete_torrent_entry_ids:
            log().info(
                "Deleting %d torrent entries.",
                len(self.delete_torrent_entry_ids))
        for id in set(self.update_torrent_entry_ids):
            te = self.btnapi.getTorrentByIdCached(id)
            if not te:
                num_no_te += 1
                self.update_torrent_entry_ids.remove(id)
                self.delete_torrent_entry_ids.add(id)
            if not te.raw_torrent_cached:
                num_torrent_not_cached += 1
                self.update_torrent_entry_ids.remove(id)
                self.delete_torrent_entry_ids.add(id)
        if num_no_te:
            log().info(
                "Deleting %d torrent entries due to not being in the db.",
                num_no_te)
        if num_torrent_not_cached:
            log().info(
                "Deleting %d torrent entries due to raw torrent not cached.",
                num_torrent_not_cached)
        if self.update_torrent_entry_ids:
            log().info(
                "Updating %d torrent entries.",
                len(self.update_torrent_entry_ids))
        for id in self.update_torrent_entry_ids:
            self.update_torrent_entry_id_unlocked(id)
        for id in self.delete_torrent_entry_ids:
            self.delete_torrent_entry_id_unlocked(id)
        self.inodb.set_global("tvaf_timestamp", self.tvafdb.get_timestamp())

    def sync_uris_unlocked(self):
        log().info("Getting list of updated scanned BTN versions.")
        last_ts = self.tvafdb.get_global("btn_fs_sync_tvaf_timestamp")
        tvaf_ts = self.tvafdb.tick()
        update_version_paths = set()
        for crud in self.tvafdb.feed(keys=(
                "version_bittorrent_path", "version_bittorrent_files"),
                timestamp=last_ts):
            if crud.action != crud.ACTION_UPDATE:
                continue
            bt_path = self.tvafdb.get(crud.path, "version_bittorrent_path")
            m = BTN_TORRENT_ENTRY_PATH_REGEX.match(bt_path)
            if not m:
                continue
            update_version_paths.add(crud.path)
        log().info("Updating %d file uris.", len(update_version_paths))
        for path in update_version_paths:
            bt_path = self.tvafdb.get(path, "version_bittorrent_path")
            m = BTN_TORRENT_ENTRY_PATH_REGEX.match(bt_path)
            id = int(m.group("torrent_entry_id"))
            files = self.tvafdb.get(path, "version_bittorrent_files")
            if files is not None:
                full_paths = [
                    os.path.join(self.mountpoint, "by-id", "%d" % id, f)
                    for f in files]
                file_uris = ["file://%s" % p for p in full_paths]
            else:
                file_uris = None
            self.tvafdb.update(
                path, {"version_uris_file": file_uris}, timestamp=tvaf_ts)
        self.tvafdb.set_global("btn_fs_sync_tvaf_timestamp", tvaf_ts)

    def sync(self):
        self.delete_torrent_entry_ids = set()
        self.update_torrent_entry_ids = set()
        with begin(self.tvafdb.db):
            with self.btnapi.db:
                with begin(self.inodb.db):
                    self.sync_inodb_unlocked()
            self.sync_uris_unlocked()
