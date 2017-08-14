import contextlib
import logging
import os
import re
import threading
import urlparse
import xml.etree.ElementTree as ElementTree

import apsw
import requests
import yaml


LIBRARY_PATH = os.path.join(
    "Library", "Application Support", "Plex Media Server", "Plug-in Support",
    "Databases", "com.plexapp.plugins.library.db")
PREFERENCES_PATH = os.path.join(
    "Library", "Application Support", "Plex Media Server", "Preferences.xml")
SCANNER_NAME = "Plex Media Scanner"
REAL_SCANNER_NAME = "Plex Media Scanner.real"

CONFIG_NAME = "tvaf_config.yaml"

METADATA_TYPE_MOVIE = 1
METADATA_TYPE_SHOW = 2
METADATA_TYPE_SEASON = 3
METADATA_TYPE_EPISODE = 4
METADATA_TYPE_ARTIST = 8
METADATA_TYPE_ALBUM = 9
METADATA_TYPE_TRACK = 10
METADATA_TYPE_EXTRA = 12

EXTRA_TYPE_TRAILER = 1

METADATA_TYPE_TVAF_BASE = 51600
METADATA_TYPE_TVAF_MOVIE_BG_ANALYZING = METADATA_TYPE_TVAF_BASE + 1
METADATA_TYPE_TVAF_SHOW_BG_ANALYZING = METADATA_TYPE_TVAF_BASE + 2
METADATA_TYPE_TVAF_SEASON_BG_ANALYZING = METADATA_TYPE_TVAF_BASE + 3
METADATA_TYPE_TVAF_EPISODE_BG_ANALYZING = METADATA_TYPE_TVAF_BASE + 4
METADATA_TYPE_TVAF_BG_ANALYZING_MAX = METADATA_TYPE_TVAF_BASE + 5
METADATA_TYPE_BG = 51650

MEDIA_ANALYSIS_VERSION = 5

AGENT_LOCALMEDIA = "com.plexapp.agents.localmedia"
AGENT_TVDB = "com.plexapp.agents.thetvdb"

STUB_FILE = "/dev/null"


PATH_REGEX = re.compile(
    r"/(?P<series_key>(btn|tvdb)_series)=(?P<series>\d+)"
    r"(/(?P<season_key>(btn|tvdb)_season)=(?P<season>-?\d+))?"
    r"(/(?P<btn_group_key>btn_(season|episode)_group)=(?P<btn_group>\d+))?"
    r"(/(?P<file_key>(tvdb|btn)_episode|episode|date|filename)="
    r"(?P<file_value>[^=/]+))?")


def log():
    return logging.getLogger(__name__)


@contextlib.contextmanager
def begin(db, mode=None):
    db.cursor().execute("begin %s" % (mode or ""))
    try:
        yield
    except:
        db.cursor().execute("rollback")
        raise
    else:
        db.cursor().execute("commit")


def get_default_plex_home():
    return os.getenv("HOME")


def get_default_db():
    home = get_default_plex_home()
    path = os.path.join(home, LIBRARY_PATH)
    # No SQLITE_OPEN_CREATE
    db = apsw.Connection(path, flags=apsw.SQLITE_OPEN_READWRITE)
    db.setbusytimeout(5000)
    return db


def get_section_tvaf(db, library_section_id):
    c = db.cursor().execute(
        "select root_path from section_locations where library_section_id = ?",
        (library_section_id,))
    for row in c:
        path = os.path.join(row[0], CONFIG_NAME)
        if not os.path.exists(path):
            continue
        return Config(path)


def get_metadata_item_tvaf(db, metadata_item_id):
    row = db.cursor().execute(
        "select library_section_id from metadata_items where id = ?",
        (metadata_item_id,)).fetchone()
    if row:
        library_section_id = row[0]
        return get_section_tvaf(db, library_section_id)


def path_to_default_guid(path):
    m = PATH_REGEX.match(path)
    assert m, path
    if (m.group("series_key") == "tvdb_series" and
            m.group("season_key") in ("tvdb_season", None) and
            m.group("file_key") in ("tvdb_episode", None) and
            m.group("btn_group_key") is None):
        guid = AGENT_TVDB + "://%d" % int(m.group("series"))
        if m.group("season") is not None:
            guid += "/%d" % int(m.group("season"))
            if m.group("file_value") is not None:
                guid += "/%d" % int(m.group("file_value"))
    else:
        guid = AGENT_LOCALMEDIA + "://tvaf%s" % path
    u = urlparse.urlparse(guid)
    qd = urlparse.parse_qs(u.query)
    if "lang" not in qd:
        qd["lang"] = "en"
    qs = urlparse.urlencode(qd)
    return urlparse.urlunparse(
        (u.scheme, u.netloc, u.path, u.params, qs, u.fragment))


def path_to_local_guid(path):
    m = PATH_REGEX.match(path)
    assert m, path
    gui = AGENT_LOCALMEDIA + "://tvaf%s" % path
    u = urlparse.urlparse(guid)
    qd = urlparse.parse_qs(u.query)
    if "lang" not in qd:
        qd["lang"] = "en"
    qs = urlparse.urlencode(qd)
    return urlparse.urlunparse(
        (u.scheme, u.netloc, u.path, u.params, qs, u.fragment))


def guid_path(guid):
    u = urlparse.urlparse(guid)
    if u.scheme == AGENT_TVDB:
        path = "/tvdb_series=%d" % int(u.netloc)
        p = [p for p in u.path.split("/") if p]
        if len(p) >= 1:
            path += "/tvdb_season=%d" % int(p[0])
        if len(p) >= 2:
            path += "/tvdb_episode=%d" % int(p[1])
        return path
    if u.scheme == AGENT_LOCALMEDIA:
        if u.netloc == "tvaf":
            return u.path


def path_parent(path):
    m = PATH_REGEX.match(path)
    assert m, path
    s = path.split("/")
    if m.group("btn_group_key") == "btn_episode_group":
        if m.group("season_key"):
            return "/".join(s[:-1])
        if m.group("series_key") == "tvdb_series":
            season = "tvdb_season=-1"
        if m.group("series_key") == "btn_series":
            season = "btn_season=-1"
        return "/".join(s[:-1] + [season])
    if (m.group("btn_group_key") == "btn_season_group" and
            m.group("season_key") and not m.group("file_key")):
        return "/".join(s[:-2])
    if (m.group("btn_group_key") == "btn_season_group" or
            m.group("season_key")):
        return "/".join(s[:-1])


def guid_parent(guid):
    u = urlparse.urlparse(guid)
    if u.scheme == AGENT_TVDB:
        if not u.path or u.path == "/":
            return None
        p = u.path.split("/")
        p = p[:len(p) - 1]
        path = "/".join(p)
    if u.scheme == AGENT_LOCALMEDIA:
        path = path_parent(u.path)
        if path is None:
            return None
    return urlparse.urlunparse(
        (u.scheme, u.netloc, path, u.params, u.query, u.fragment))


class PlexAPI(object):

    def __init__(self, path):
        self.path = path
        self._local = threading.local()

    @property
    def db(self):
        if hasattr(self._local, "db"):
            return self._local.db
        path = os.path.join(self.path, LIBRARY_PATH)
        # No SQLITE_OPEN_CREATE
        db = apsw.Connection(path, flags=apsw.SQLITE_OPEN_READWRITE)
        db.setbusytimeout(5000)
        self._local.db = db
        return db

    def get_token():
        if "X_PLEX_TOKEN" in os.environ:
            return os.environ["X_PLEX_TOKEN"]
        path = os.path.join(self.path, PREFERENCES_PATH)
        return ElementTree.parse(path).getroot().get("PlexOnlineToken")

    def get_global(self, name):
        row = self.db.cursor().execute(
            "select value from preferences where name = ?", (name,)).fetchone()
        if row:
            return row[0]

    def get_global_int(self, name):
        try:
            return int(self.get_global(name))
        except TypeError:
            return None

    def set_global(self, name, value):
        with self.db:
            self.db.cursor().execute(
                "insert or replace into preferences (name, value) "
                "values (?, ?)", (name, value))


class Config(object):

    def __init__(self, path):
        self.path = path

        with open(self.path) as f:
            self.config = yaml.load(f)

        self.tvafdb = self.config["tvafdb"]
        self.inodb = self.config["inodb"]
        self.tvdb = self.config["tvdb"]
        self.btnapi = self.config["btnapi"]
        self.plex = self.config["plex"]

        self._local = threading.local()
        self._shows_section_id = None

    @property
    def mountpoint(self):
        return self.config["mountpoint"]

    @property
    def plex_home(self):
        if "plex_home" in self.config:
            return self.config["plex_home"]
        return get_default_plex_home()

    @property
    def shows_section_id(self):
        if self._shows_section_id is not None:
            return self._shows_section_id
        with self.plex.db:
            for row in self.plex.db.cursor().execute(
                    "select section_locations.root_path, library_sections.id "
                    "from library_sections, section_locations "
                    "where section_locations.library_section_id = "
                    "library_sections.id and "
                    "library_sections.section_type = ?",
                    (METADATA_TYPE_SHOW,)):
                path, id = row
                path = os.path.join(path, CONFIG_NAME)
                if path == self.path:
                    self._shows_section_id = id
                    return id
            self.plex.db.cursor().execute(
                "insert into library_sections "
                "(name, section_type, language, agent, scanner) values "
                "(?, ?, ?, ?, ?)",
                ("TVAF Shows", METADATA_TYPE_SHOW, "en",
                 AGENT_TVDB, "Plex Series Scanner"))
            id = self.plex.db.last_insert_rowid()
            dirname = os.path.dirname(self.path)
            self.plex.db.cursor().execute(
                "insert into section_locations "
                "(library_section_id, root_path, available) values "
                "(?, ?, 1)",
                (id, dirname))
            self._shows_section_id = id
            return id


class MediaItem(object):

    def __init__(self, tvaf_config, media_item_id, library_section_id=None,
                 path=None):
        self.tvaf = tvaf_config
        self.media_item_id = media_item_id

        self._library_section_id = library_section_id
        self._path = path
        self._tvaf = None

    @classmethod
    def find_by_id(cls, tvaf_config, media_item_id):
        db = tvaf_config.plex.db
        r = db.cursor().execute(
            "select id from media_items where id = ? ",
            (media_item_id,)).fetchone()
        if r:
            return cls(tvaf_config, media_item_id)

    @classmethod
    def find_or_create(
            cls, tvaf_config, library_section_id, path, create=True):
        db = tvaf_config.plex.db
        with db:
            r = db.cursor().execute(
                "select id from media_items where library_section_id = ? "
                "and source = ?",
                (library_section_id, path)).fetchone()
            if r:
                id = r[0]
                log().debug("%s: existing media item %s", path, id)
                return cls(
                    db, id, library_section_id=library_section_id,
                    path=path)
            if not create:
                return None

            db.cursor().execute(
                "insert into media_items "
                "(library_section_id, source) values (?, ?)",
                (library_section_id, path))
            id = db.last_insert_rowid()
            log().debug("%s: added media item %s", path, id)
            mi = cls(tvaf_config, id, library_section_id=library_section_id, path=path)
            mi.update_id(new=True)
            mi.set_paths([STUB_FILE])
            return mi

    @property
    def db(self):
        return self.tvaf.plex.db

    @property
    def tvafdb(self):
        return self.tvaf.tvafdb

    @property
    def path(self):
        if self._path is None:
            row = self.db.cursor().execute(
                "select source from media_items where id = ?",
                (self.media_item_id,)).fetchone()
            if row:
                self._path = row[0]
        return self._path

    @property
    def library_section_id(self):
        if self._library_section_id is None:
            row = self.db.cursor().execute(
                "select library_section_id from media_items where id = ?",
                (self.media_item_id,)).fetchone()
            if row:
                self._library_section_id = row[0]
        return self._library_section_id

    @property
    def metadata_item_guid(self):
        s = self.path.rsplit("/", 1)
        assert len(s) == 2, self.path
        assert s[1].split("=", 1)[0] == "version_id", self.path
        return path_to_default_guid(s[0])

    @property
    def metadata_item(self):
        return MetadataItem.find_or_create(
            self.tvaf, self.library_section_id, self.metadata_item_guid)

    def delete_part(self, media_part_id):
        with self.db:
            c = self.db.cursor()
            c.execute(
                "delete from media_parts where id = ?",
                (media_part_id,))
            c.execute(
                "delete from media_part_settings where media_part_id = ?",
                (media_part_id,))
            c.execute(
                "delete from media_streams where media_part_id = ?",
                (media_part_id,))

    def delete(self):
        with self.db:
            r = self.db.cursor().execute(
                "select id from media_items where library_section_id = ? and "
                "source = ?", (self.library_section_id, self.path)).fetchone()
            if not r:
                return
            media_item_id = r[0]

            c = self.db.cursor().execute(
                "select id from media_parts "
                "where media_item_id = ? order by media_parts.'index'",
                (media_item_id,))
            media_part_ids = set(id for id, in c)

            for media_part_id in media_part_ids:
                self.delete_part(media_part_id)

            c.execute(
                "delete from media_items where id = ?", (media_item_id,))
            c.execute(
                "delete from media_item_settings where media_item_id = ?",
                (media_item_id,))

    def get_parts(self):
        return self.db.cursor().execute(
            "select media_parts.'index', file, id from media_parts "
            "where media_item_id = ? order by media_parts.'index'",
            (self.media_item_id,))

    def update_id(self, new=False):
        with self.db:
            parent_id = self.metadata_item.metadata_item_id
            c = self.db.cursor()
            if self.is_fg() or new:
                c.execute(
                    "update media_items set metadata_item_id = ? where id = ?",
                    (parent_id, self.media_item_id))
            else:
                c.execute(
                    "update metadata_items set parent_id = ? where id = ?",
                    (parent_id, self.get_real_metadata_item_id()))

    def set_paths(self, paths):
        with self.db:
            path_to_index = { path: i for i, path in enumerate(paths) }
            have_paths = set()

            c = self.db.cursor()

            for part_index, part_path, media_part_id in self.get_parts():
                have_paths.add(part_path)
                path_index = path_to_index.get(part_path)
                if path_index == part_index:
                    log().debug("%s: %s: reusing part %d",
                        self.path, part_path, media_part_id)
                elif path_index is None:
                    log().debug("%s: %s: deleting part %d",
                        self.path, part_path, media_part_id)
                    self.delete_part(media_part_id)
                elif path_index != part_index:
                    log().debug("%s: %s: renumbering part %d to %d",
                        self.path, part_path, media_part_id, path_index)
                    c.execute(
                        "update media_parts set 'index' = ? where id = ?",
                        (path_index, media_part_id))

            for index, file_path in enumerate(paths):
                if file_path in have_paths:
                    continue
                c.execute(
                    "insert into media_parts "
                    "(media_item_id, 'index', file) "
                    "values (?, ?, ?)",
                    (self.media_item_id, index, file_path))
                id = self.db.last_insert_rowid()
                log().debug("%s: %s: added as %d", self.path, file_path, id)

    def update(self):
        paths = self.tvafdb.get(self.path, "version_uris_file")
        paths = [urlparse.urlparse(p).path for p in paths]
        if not paths:
            paths = [STUB_FILE]
        self.set_paths(paths)

    def is_analyzed(self):
        r = self.db.cursor().execute(
            "select media_analysis_version from media_items where id = ?",
            (self.media_item_id,)).fetchone()
        if r:
            return r[0] == MEDIA_ANALYSIS_VERSION

    def should_resolve(self):
        return any(path == "/dev/null" for _, path, _ in self.get_parts())

    def resolve(self):
        bt_path = self.tvaf.tvafdb.get(path, "version_bittorrent_path")
        m = tvaf.btn.BTN_TORRENT_ENTRY_PATH_REGEX.match(bt_path)
        if not m:
            return
        id = int(m.group("torrent_entry_id"))
        te = self.tvaf.btnapi.getTorrentByIdCached(id)
        assert te
        _ = te.raw_torrent
        with self.tvaf.btnapi.db:
            te.serialize()
        sc = tvaf.sync.SyncConfig(sync_btn=True, sync_btn_fs=True)
        syncer = tvaf.sync.Syncer(sc, self.tvaf)
        syncer.sync()

    def get_settings(self):
        r = self.db.cursor().execute(
            "select settings from media_items where id = ?",
            (self.media_item_id,)).fetchone()
        if not r:
            return {}
        return dict(sorted(urlparse.parse_qsl(r[0])))

    def set_settings(self, **kwargs):
        with self.db:
            qd = self.get_settings()
            for k, v in kwargs.iteritems():
                if v is None:
                    if k in qd:
                        del qd[k]
                else:
                    qd[k] = v
            qs = urlparse.urlencode(qd)
            self.db.cursor().execute(
                "update media_items set settings = ? where id = ?",
                (qs, self.media_item_id))

    def analyze_rank(self):
        with self.tvafdb.db:
            bt_path = self.tvafdb.get(self.path, "version_bittorrent_path")
            if bt_path is not None:
                return -(self.tvafdb.get(bt_path, "bittorrent_seeders") or 0)
            else:
                return 0

    def get_real_metadata_item_id(self):
        r = self.db.cursor().execute(
            "select metadata_item_id from media_items where id = ?",
            (self.media_item_id,)).fetchone()
        if r:
            return r[0]

    def is_fg(self):
        with self.db:
            r = self.db.cursor().execute(
                "select metadata_type from metadata_items where id = ?",
                (self.get_real_metadata_item_id(),)).fetchone()
            if r:
                return r[0] != METADATA_TYPE_BG

    def move_to_bg(self):
        with self.db:
            if not self.is_fg():
                log().debug("%s: already in bg", self.path)
                return

            c = self.db.cursor()
            c.execute(
                "insert into metadata_items (library_section_id, "
                "metadata_type, parent_id) values (?, ?, ?)",
                (self.library_section_id,
                 METADATA_TYPE_BG, self.metadata_item.metadata_item_id))
            bg_id = self.db.last_insert_rowid()
            c.execute(
                "update media_items set metadata_item_id = ? "
                "where id = ?", (bg_id, self.media_item_id))
            log().debug(
                "%s: moved to background metadata item %s",
                self.path, bg_id)

    def move_to_fg(self):
        with self.db:
            if self.is_fg():
                log().debug("%s: already in fg", self.path)
                return
            log().debug("%s: moving to foreground", self.path)
            bg_id = self.get_real_metadata_item_id()
            c = self.db.cursor()
            c.execute(
                "update media_items set metadata_item_id = ? where id = ?",
                (self.metadata_item.metadata_item_id, self.media_item_id))
            c.execute(
                "delete from metadata_items where id = ?", (bg_id,))


class MetadataItem(object):

    def __init__(self, tvaf_config, metadata_item_id, library_section_id=None,
                 guid=None):
        self.tvaf = tvaf_config
        self.metadata_item_id = metadata_item_id

        self._library_section_id = library_section_id
        self._guid = guid
        self._tvaf = None

    @classmethod
    def find_by_id(cls, tvaf_config, metadata_item_id):
        db = tvaf_config.plex.db
        r = db.cursor().execute(
            "select id from metadata_items where id = ? ",
            (metadata_item_id,)).fetchone()
        if r:
            return cls(tvaf_config, metadata_item_id)

    @classmethod
    def find_or_create(
            cls, tvaf_config, library_section_id, guid, create=True):
        db = tvaf_config.plex.db
        with db:
            r = db.cursor().execute(
                "select id from metadata_items where library_section_id = ? "
                "and guid = ?",
                (library_section_id, guid)).fetchone()
            if r:
                id = r[0]
                log().debug(
                    "%s: existing metadata item %s", guid_path(guid), id)
                return cls(
                    tvaf_config, id, library_section_id=library_section_id,
                    guid=guid)
            if not create:
                return None

            db.cursor().execute(
                "insert into metadata_items "
                "(library_section_id, guid) values (?, ?)",
                (library_section_id, guid))
            id = db.last_insert_rowid()
            log().debug("%s: added metadata item %s", guid_path(guid), id)
            mi = cls(
                tvaf_config, id, library_section_id=library_section_id, guid=guid)
            mi.update_id()
            return mi

    @property
    def db(self):
        return self.tvaf.plex.db

    @property
    def tvafdb(self):
        return self.tvaf.tvafdb

    def mark_dirty(self):
        with self.db:
            self.db.cursor().execute(
                "update metadata_items set refreshed_at = null where id = ?",
                (self.metadata_item_id,))

    def refresh(self):
        requests.put(
            "http://localhost:32400/library/metadata/%d/refresh" %
            self.metadata_item_id,
            headers={"X-Plex-Token": self.tvaf.plex.get_token()})

    @property
    def library_section_id(self):
        if self._library_section_id is None:
            row = self.db.cursor().execute(
                "select library_section_id from metadata_items where id = ?",
                (self.metadata_item_id,)).fetchone()
            if row:
                self._library_section_id = row[0]
        return self._library_section_id

    @property
    def guid(self):
        if self._guid is None:
            row = self.db.cursor().execute(
                "select guid from metadata_items where id = ?",
                (self.metadata_item_id,)).fetchone()
            if row:
                self._guid = row[0]
        return self._guid

    @property
    def path(self):
        return guid_path(self.guid)

    def update_id(self):
        with self.db:
            self.db.cursor().execute(
                "update metadata_items "
                "set metadata_type = ?, 'index' = ?, parent_id = ? "
                "where id = ?",
                (self.plex_type, self.index,
                 self.parent.metadata_item_id if self.parent else None,
                 self.metadata_item_id))

    def update(self):
        with self.db:
            self.db.cursor().execute(
                "update metadata_items "
                "set title = ?, originally_available_at = ? where id = ?",
                (self.title, self.originally_available_at, self.index,
                 self.metadata_item_id))

    @property
    def parent_guid(self):
        return guid_parent(self.guid)

    @property
    def parent(self):
        if self.parent_guid:
            return MetadataItem.find_or_create(
                self.tvaf, self.library_section_id, self.parent_guid)

    @property
    def base(self):
        guid = None
        next_guid = self.guid
        while next_guid:
            guid = next_guid
            next_guid = guid_parent(next_guid)
        return MetadataItem.find_or_create(
            self.tvaf, self.library_section_id, guid)

    @property
    def plex_type(self):
        m = PATH_REGEX.match(self.path)
        assert m, self.path
        if m.group("btn_group_key") == "btn_episode_group":
            return METADATA_TYPE_EPISODE
        if m.group("file_key"):
            return METADATA_TYPE_EPISODE
        if (m.group("season_key") or
                m.group("btn_group_key") == "btn_season_group"):
            return METADATA_TYPE_SEASON
        if m.group("series_key"):
            return METADATA_TYPE_SHOW

    @property
    def index(self):
        m = PATH_REGEX.match(self.path)
        assert m, self.path
        if m.group("file_key") in ("tvdb_episode", "btn_episode", "episode"):
            return int(m.group("file_value"))
        if m.group("season_key"):
            return int(m.group("season"))

    @property
    def title(self):
        m = PATH_REGEX.match(self.path)
        assert m, self.path
        if m.group("file_key") in ("filename", "date"):
            return m.group("file_value")
        if m.group("file_key"):
            return None
        if m.group("btn_group_key"):
            return self.tvafdb.get(self.path, "btn_group_name")
        if m.group("season_key") and int(m.group("season")) == -1:
            return "Other"
        if m.group("season_key"):
            return None
        if m.group("series_key") == "btn_series":
            return self.tvafdb.get(self.path, "btn_series_name")
        if m.group("series_key") == "tvdb_series":
            name = self.tvafdb.get(self.path, "tvdb_series_seriesName_en")
            if urlparse.urlparse(self.guid).scheme == AGENT_LOCALMEDIA:
                name += " (Extras)"
            return name

    @property
    def originally_available_at(self):
        m = PATH_REGEX.match(self.path)
        assert m, self.path
        if m.group("file_key") == "date":
            return m.group("file_value")

    def get_media_items(self):
        c = self.db.cursor().execute(
            "select id from media_items where metadata_item_id = ?",
            (self.metadata_item_id,))
        for id, in c:
            yield MediaItem(self.tvaf, id)
        c = self.db.cursor().execute(
            "select media_items.id "
            "from metadata_items indexed by index_metadata_items_on_parent_id "
            "inner join media_items "
            "where media_items.metadata_item_id = metadata_items.id and "
            "metadata_items.parent_id = ? and "
            "metadata_items.metadata_type = ?",
            (self.metadata_item_id, METADATA_TYPE_BG))
        for id, in c:
            yield MediaItem(self.tvaf, id)
