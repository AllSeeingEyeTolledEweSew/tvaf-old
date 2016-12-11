import argparse
import sys

import btn


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument("hash")

    args = parser.parse_args()

    api = btn.API()

    sr = api.getTorrents(hash=args.hash.upper(), cache=api.CACHE_ONLY)
    if not sr.torrents:
        return 1
    te = sr.torrents[0]
    sys.stdout.buffer.write(te.raw_torrent)
    return 0
