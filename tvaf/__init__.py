import re

import btn

from tvaf import model


GROUP_EPISODE_REGEX = re.compile(
    r"S(?P<season>[0-9]{2})E(?P<episode>[0-9]{2})$")
GROUP_DATE_REGEX = re.compile(
    r"(?P<y>[0-9]{4})\.(?P<m>[0-9]{2})\.(?P<d>[0-9]{2})$")
GROUP_SEASON_REGEX = re.compile(
    r"Season (?P<season>[0-9]+)(\.(?P<part>[0-9]+))?")


def _get_parts(files):
    largest = max(range(len(files)), key=lambda i: files[i][b"length"])
    return [largest]


class BtnEntryFactory(object):

    def __init__(self, tvaf_id):
        assert tvaf_id.SCHEME == model.TvdbId.SCHEME
        self.tvaf_id = tvaf_id
        self.api = btn.API()

    def entries(self):
        for te in self.api.getTorrentsPaged(tvdb=self.tvaf_id.series):
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
