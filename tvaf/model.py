import re
import os

from yatfs import util as yatfs_util


def str_to_id(s):
    p = s.split("-")
    assert len(p) == 2, s
    for cls in ImdbId, TvdbId, BtnId:
        if p[0] == cls.SCHEME:
            break
    else:
        assert False, s
    m = cls.REGEX.match(p[1])
    assert m, s
    return cls(**m.groupdict())


class TvafId(object):

    SCHEME = None
    REGEX = None


class ImdbId(TvafId):

    REGEX = re.compile(r"(?P<imdb_id>tt[0-9]{7})")
    SCHEME = "imdb"

    def __init__(self, imdb_id):
        self.imdb_id = imdb_id

    def __str__(self):
        return "imdb-%s" % self.imdb_id


class TvdbId(TvafId):

    REGEX = re.compile(r"(?P<series>\d+)")
    SCHEME = "tvdb"

    def __init__(self, series):
        self.series = int(series)

    def __str__(self):
        return "tvdb-%d" % self.series


class BtnId(TvafId):

    REGEX = re.compile(r"(?P<btn_id>\d+)")
    SCHEME = "btn"

    def __init__(self, btn_id):
        self.btn_id = int(btn_id)

    def __str__(self):
        return "btn-%d" % self.btn_id


class Entry(object):

    def __init__(self, tvaf_id, torrent_entry):
        self.tvaf_id = tvaf_id
        self.torrent_entry = torrent_entry

    @property
    def raw_torrent(self):
        return self.torrent_entry.raw_torrent

    @property
    def torrent_object(self):
        return self.torrent_entry.torrent_object

    @property
    def hash(self):
        return self.torrent_entry.info_hash

    @property
    def info(self):
        return self.torrent_object[b"info"]

    @property
    def files(self):
        return yatfs_util.info_files(self.info)

    @property
    def time(self):
        return self.torrent_entry.time

    @property
    def base_path(self):
        parts = [str(self.tvaf_id)]
        if self.group:
            parts.append(self.group)
        if self.edition:
            parts.append(self.edition)
        return os.path.join(*parts)

    @property
    def group(self):
        return None

    @property
    def edition(self):
        return None


class BtnEntry(Entry):

    @property
    def group(self):
        return self.torrent_entry.group.name


class MultipartFileEntry(Entry):

    def __init__(self, tvaf_id, torrent_entry, file_indicies):
        super(FileEntry, self).__init__(tvaf_id, torrent_entry)
        self.file_indicies = file_indicies

    @property
    def paths(self):
        paths = []
        for i in self.file_indices:
            path = os.fsdecode(os.path.join(*self.files[i][b"path"]))
            paths.append(os.path.join(self.base_path, path))
        return paths


class BtnMultipartFileEntry(BtnEntry, MultipartFileEntry):

    pass
