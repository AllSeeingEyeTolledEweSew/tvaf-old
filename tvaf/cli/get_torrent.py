import argparse
import sys

import btn


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument("hash")

    args = parser.parse_args()

    api = btn.API()

    tes = api.getTorrentsCached(hash=args.hash.upper())
    if not tes:
        return 1
    te = tes[0]
    sys.stdout.buffer.write(te.raw_torrent)
    return 0
