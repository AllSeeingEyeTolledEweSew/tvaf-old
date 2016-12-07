import re


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
