import argparse
import logging
import os
import sys
import yaml

import tvaf
import tvaf.sync


def main():
    parser = argparse.ArgumentParser(description="Synchronize TVAF.")
    parser.add_argument("--verbose", "-v", action="count")
    parser.add_argument("--tvaf_config", "-c")

    parser.add_argument("--scrape_btn", action="store_true")
    parser.add_argument("--sync_btn", action="store_true")
    parser.add_argument("--sync_btn_fs", action="store_true")
    parser.add_argument("--sync_tvdb", action="store_true")
    parser.add_argument("--sync_plex", action="store_true")

    args = parser.parse_args()

    if not args.tvaf_config:
        args.tvaf_config = open(os.path.expanduser("~/.tvaf/tvaf_config.yaml"))

    tvaf_config = tvaf.Config(args.tvaf_config)

    if args.verbose:
        level = logging.DEBUG
    else:
        level = logging.INFO

    sync_config = tvaf.sync.SyncConfig(
        scrape_btn=args.scrape_btn, sync_btn=args.sync_btn,
        sync_btn_fs=args.sync_btn_fs, sync_tvdb=args.sync_tvdb,
        sync_plex=args.sync_plex)

    logging.basicConfig(
        stream=sys.stdout, level=level,
        format="%(asctime)s %(levelname)s %(threadName)s "
        "%(filename)s:%(lineno)d %(message)s")

    syncer = tvaf.sync.Syncer(sync_config, tvaf_config)
    syncer.sync()
