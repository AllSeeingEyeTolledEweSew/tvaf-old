import errno
import os
import re
import yaml

import btn
from tvaf import model
from yatfs import inodb as inodb_lib


GROUP_EPISODE_REGEX = re.compile(
    r"S(?P<season>[0-9]{2})E(?P<episode>[0-9]{2})$")
GROUP_DATE_REGEX = re.compile(
    r"(?P<y>[0-9]{4})\.(?P<m>[0-9]{2})\.(?P<d>[0-9]{2})$")
GROUP_SEASON_REGEX = re.compile(
    r"Season (?P<season>[0-9]+)(\.(?P<part>[0-9]+))?")


class Config(object):

    NAME = "tvaf_config.yaml"

    def __init__(self, path=None):
        self.path = path
        if path:
            with open(path) as f:
                config = yaml.load(f)
            self.mountpoint = config.get("mountpoint")
            self.inodb_path = config.get("inodb_path")
        else:
            self.mountpoint = None
            self.inodb_path = None

        self._inodb = None

    @property
    def inodb(self):
        if self._inodb is None:
            self._inodb = inodb_lib.InoDb(self.inodb_path)
        return self._inodb

    def get_factory(self, tvaf_id):
        assert tvaf_id.SCHEME == model.TvdbId.SCHEME
        return BtnEntryFactory(tvaf_id)

    def update(self, tvaf_id, factory=None):
        if factory is None:
            factory = self.get_factory(tvaf_id)

        files = []
        for entry in factory.entries():
            t = entry.time
            hash = entry.hash
            for idx, f in enumerate(entry.files):
                path = os.path.join(
                    "/",
                    entry.base_path,
                    os.fsdecode(os.path.join(*f[b"path"])))
                dirname, filename = os.path.split(path)
                files.append((dirname, filename, f[b"length"], hash, idx, t))

        with self.inodb:
            for dirname, filename, size, hash, idx, t in files:
                path = os.path.join(dirname, filename)
                self.inodb.mkdir_p(dirname, 0o755, 0, 0)
                try:
                    ino = self.inodb.mkfile(path, 0o444, hash, idx, size, 0, 0)
                except OSError as e:
                    if e.errno == errno.EEXIST:
                        continue
                    raise
                self.inodb.setattr_ino(ino, st_ctime=t, st_mtime=t)


def _get_parts(files):
    largest = max(range(len(files)), key=lambda i: files[i][b"length"])
    return [largest]


class BtnEntryFactory(object):

    def __init__(self, tvaf_id):
        assert tvaf_id.SCHEME == model.TvdbId.SCHEME
        self.tvaf_id = tvaf_id
        self.api = btn.API()

    def entries(self):
        for te in self.api.getTorrentsCached(tvdb=self.tvaf_id.series):
            yield model.BtnEntry(self.tvaf_id, te)


class BtnEpisodeEntryFactory(object):

    def __init__(self, tvaf_id):
        self.tvaf_id = tvaf_id
        self.entry_factory = BtnEntryFactory(tvaf_id)

    def entries(self):
        for entry in self.entry_factory.entries():
            te = entry.torrent_entry
            if te.group.category != "Episode":
                continue
            group_name = te.group.name
            m = GROUP_EPISODE_REGEX.match(group_name)
            if m:
                tvaf_id = model.TvdbId(
                    self.tvaf_id.series, season=int(m.group("season")),
                    episode=int(m.group("episode")))
            else:
                continue
            parts = _get_parts(entry.files)
            yield model.BtnMultipartFileEntry(tvaf_id, te, parts)


def get_raw_torrent(hash):
    api = btn.API()
    tes = api.getTorrentsCached(hash=hash.upper())
    if not tes:
        return b""
    return tes[0].raw_torrent
