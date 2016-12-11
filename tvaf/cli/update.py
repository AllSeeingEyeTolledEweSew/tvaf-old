import argparse
import logging
import sys
import os

import btn
from yatfs import inodb as inodb_lib
from yatfs import util as yatfs_util

from tvaf import model


def log():
    return logging.getLogger(__name__)


def add_pseudoextension(name, x):
    s = name.rsplit(".", 1)
    if len(s) == 1:
        return "%s.%s" % (name, x)
    else:
        return "%s.%s.%s" % (s[0], x, s[1])

def main():
    logging.basicConfig(
        stream=sys.stdout, level=logging.DEBUG,
        format="%(asctime)s %(levelname)s %(threadName)s "
        "%(filename)s:%(lineno)d %(message)s")

    parser = argparse.ArgumentParser(
        description="TVAF Update")
    parser.add_argument("-d", "--inodb")
    parser.add_argument("--btn")
    parser.add_argument("tvaf_id", type=model.str_to_id)

    args = parser.parse_args()

    tvaf_id = args.tvaf_id
    assert tvaf_id.SCHEME == model.TvdbId.SCHEME
    if args.inodb:
        db_path = args.inodb
    else:
        db_path = os.path.expanduser("~/.tvaf/ino.db")
    inodb = inodb_lib.InoDb(db_path)

    btn_api = btn.API(cache_path=args.btn)
    tes = list(btn_api.getTorrentsPaged(tvdb=tvaf_id.series))

    uid = os.geteuid()
    gid = os.getegid()

    with inodb:
        for te in tes:
            info = te.torrent_object[b"info"]
            hash = te.info_hash.lower()
            t = te.time
            for idx, f in enumerate(yatfs_util.info_files(info)):
                path = os.fsdecode(os.path.join(*f[b"path"]))
                path = os.path.join("/", str(tvaf_id), te.group.name, path)
                dirname, filename = os.path.split(path)
                filename = add_pseudoextension(filename, "%s.%d" % (hash, idx))
                path = os.path.join(dirname, filename)
                inodb.mkdir_p(dirname, 0o755, uid, gid)
                try:
                    ino = inodb.mkfile(
                        path, 0o444, hash, idx, f[b"length"], uid, gid)
                except OSError as e:
                    if e.errno == errno.ENOENT:
                        continue
                    raise
                inodb.setattr_ino(ino, st_ctime=t, st_mtime=t)
