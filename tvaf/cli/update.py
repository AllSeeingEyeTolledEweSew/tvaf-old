import argparse
import errno
import logging
import sys
import os

from yatfs import inodb as inodb_lib

import tvaf
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
    parser.add_argument("--unique", action="store_true")
    parser.add_argument("tvaf_id", type=model.str_to_id)

    args = parser.parse_args()

    tvaf_id = args.tvaf_id
    if args.inodb:
        db_path = args.inodb
    else:
        db_path = os.path.expanduser("~/.tvaf/ino.db")
    inodb = inodb_lib.InoDb(db_path)

    factory = tvaf.BtnEntryFactory(tvaf_id)

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
            if args.unique:
                filename = add_pseudoextension(
                    filename, "%s.%d" % (hash, idx))
            files.append((dirname, filename, f[b"length"], hash, idx, t))

    uid = os.geteuid()
    gid = os.getegid()

    with inodb:
        for dirname, filename, size, hash, idx, t in files:
            path = os.path.join(dirname, filename)
            inodb.mkdir_p(dirname, 0o755, uid, gid)
            try:
                ino = inodb.mkfile(path, 0o444, hash, idx, size, uid, gid)
            except OSError as e:
                if e.errno == errno.EEXIST:
                    continue
                raise
            inodb.setattr_ino(ino, st_ctime=t, st_mtime=t)
