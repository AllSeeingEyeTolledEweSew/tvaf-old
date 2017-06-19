import argparse
import logging
import logging.handlers
import os
import re
import subprocess
import sys
import time
import traceback
import urlparse

import apsw
apsw.fork_checker()
import tvaf


ANALYZE_TIMEOUT = 60


def log():
    return logging.getLogger(__name__)


def log_to_syslog(name):
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    handler = logging.StreamHandler(stream=open("/tmp/scanner.log", mode="a"))
    handler.setFormatter(logging.Formatter(
        name + "[%(process)d]: %(levelname)s %(threadName)s "
        "%(filename)s:%(lineno)d %(message)s"))
    logger.addHandler(handler)
    handler = logging.StreamHandler(stream=sys.stdout)
    handler.setFormatter(logging.Formatter(
        name + "[%(process)d]: %(asctime)s %(levelname)s %(threadName)s "
        "%(filename)s:%(lineno)d %(message)s"))
    logger.addHandler(handler)


class Analyzer(object):

    def __init__(self, item):
        self.item = item
        self.proc = None

    def start_analyze_process(self):
        with self.item.db:
            env = os.environ.copy()
            for k, v in env.copy().items():
                m = re.match(r"TVAF_PLEX_PASSTHRU_(?P<name>.*)", k)
                if m:
                    del env[k]
                    name = m.group("name")
                    env[name] = v
            args = [tvaf.REAL_SCANNER_PATH, "--analyze", "--item",
                    str(self.item.get_real_metadata_item_id())]
            log().debug("%s", args)
            self.proc = subprocess.Popen(args, env=env)
            log().debug("real scanner has pid %s", self.proc.pid)
            self.item.db.cursor().execute(
                "update metadata_items set studio = ? where id = ?",
                (self.proc.pid, self.item.get_real_metadata_item_id()))

    def mark_item_failed(self, msg):
        with self.item.db:
            self.item.db.cursor().execute(
                "update metadata_items set summary = ? where id = ?",
                (msg, self.item.get_real_metadata_item_id()))

    def mark_result(self, result):
        self.item.set_settings(analyze_status=result)

    def run(self):
        log().debug("analyzing media item %s", self.media_item_id)

        try:
            with tvaf.begin(self.item.db, "immediate"):
                self.item.move_to_bg()
                self.start_analyze_process()
            r = self.proc.wait()
            assert r == 0, r
            with tvaf.begin(self.item.db, "immediate"):
                self.item.move_to_fg()
                self.mark_result_unlocked(0)
            return 0
        except:
            log().exception(
                "while analyzing media item %s", self.item.media_item_id)
            with tvaf.begin(self.item.db, "immediate"):
                if not self.item.is_fg():
                    self.mark_item_failed(traceback.format_exc())
                self.mark_result_unlocked(1)
            return 1


class MediaItem(tvaf.MediaItem):

    def start_analyze_process(self):
        with self.db:
            assert not self.is_fg()
            env = os.environ.copy()
            for k, v in env.copy().items():
                m = re.match(r"TVAF_PLEX_PASSTHRU_(?P<name>.*)", k)
                if m:
                    del env[k]
                    name = m.group("name")
                    env[name] = v
            args = [tvaf.REAL_SCANNER_PATH, "--analyze", "--item",
                    str(self.get_real_metadata_item_id())]
            log().debug("%s [%s]: %s", self.path, self.media_item_id, args)
            self.proc = subprocess.Popen(args, env=env)
            log().debug(
                "%s [%s]: real scanner has pid %s", self.path,
                self.media_item_id, self.proc.pid)

    def analyze(self):
        log().debug("%s [%s]: analyzing", self.path, self.media_item_id)

        try:
            with tvaf.begin(self.db, "immediate"):
                self.move_to_bg()
                self.start_analyze_process()
                self.set_settings(
                    analyze_real_pid=self.proc.pid, analyze_status=None,
                    analyze_message=None)
            r = self.proc.wait()
            assert r == 0, r
            with tvaf.begin(self.db, "immediate"):
                self.move_to_fg()
                self.set_settings(
                    analyze_status=0, analyze_message=None,
                    analyze_real_pid=None)
            return 0
        except:
            log().exception(
                "%s [%s]: while analyzing media item", self.path,
                self.media_item_id)
            with tvaf.begin(self.db, "immediate"):
                self.set_settings(
                    analyze_real_pid=None, analyze_status=1,
                    analyze_message=traceback.format_exc())
            return 1

    def analyze_in_subprocess(self):
        with tvaf.begin(self.db, "immediate"):
            if self.is_analyzing():
                pid, _ = self.get_analyze_status()
                log().debug(
                    "%s [%s]: reusing analyze babysitter pid %s",
                    self.path, self.media_item_id, pid)
                return
            proc = subprocess.Popen([
                tvaf.SCANNER_PATH, "--analyze", "--media_item",
                str(self.media_item_id)])
            log().debug(
                "%s [%s]: started new analyze babysitter pid %s", self.path,
                self.media_item_id, proc.pid)
            self.set_settings(analyze_pid=proc.pid, analyze_status=None)

    def is_analyzing(self):
        pid, status = self.get_analyze_status()
        if pid is not None and status is None:
            return pid

    def get_analyze_status(self):
        settings = self.get_settings()
        pid = settings.get("analyze_pid")
        if pid is None:
            return (None, None)
        try:
            pid = int(pid)
        except ValueError:
            return (None, None)
        status = settings.get("analyze_status")
        if status is None:
            try:
                os.kill(pid, 0)
            except ProcessLookupError:
                return (None, None)
            return (pid, None)
        try:
            status = int(status)
        except ValueError:
            return (pid, 1)
        return (pid, status)


class MetadataItem(tvaf.MetadataItem):

    def get_media_items(self):
        for mi in super(MetadataItem, self).get_media_items():
            yield MediaItem(db=mi.db, media_item_id=mi.media_item_id)

    def ensure_all_analyzing(self):
        attempts = 0
        most_mis_seen = 0
        while True:
            mis = []
            mis_seen = 0
            with self.db:
                for mi in self.get_media_items():
                    mis_seen += 1
                    if mi.is_analyzed() or mi.is_analyzing():
                        continue
                    mis.append(mi)
                if not mis:
                    break
                mis = sorted(mis, key=lambda mi: mi.analyze_rank())
            if not mis:
                break
            most_mis_seen = max(mis_seen, most_mis_seen)
            if attempts > 3 * most_mis_seen:
                log().debug(
                    "%s [%s]: done %d resolve/analyze attempts. seen at most "
                    "%d un-analyzed media items. breaking to avoid a possible "
                    "infinite loop.", self.path, self.metadata_item_id,
                    attempts, most_mis_seen)
                break
            attempts += 1
            mi = mis[0]
            if mi.should_resolve():
                mi.resolve()
            else:
                mi.analyze_in_subprocess()

    def analyze(self):
        start = time.time()
        while True:
            self.ensure_all_analyzing()
            now = time.time()
            with self.db:
                analyzed = [mi.is_analyzed() for mi in self.get_media_items()]
            if all(analyzed):
                log().debug(
                    "%s [%s]: all analyzed", self.path, self.metadata_item_id)
                break
            if any(analyzed) and (now - start) > ANALYZE_TIMEOUT:
                log().debug(
                    "%s [%s]: %d analyzed, %d timed out for now", self.path,
                    self.metadata_item_id,
                    len([a for a in analyzed if analyzed]),
                    len([a for a in analyzed if not analyzed]))
                break
            time.sleep(0.1)


class ScanWrapper(object):

    def __init__(self):
        self._db = None

    @property
    def db(self):
        if not self._db:
            self._db = tvaf.get_default_db()
        return self._db

    def parse_args(self):
        parser = argparse.ArgumentParser(description="Plex Media Scanner Shim")
        parser.add_argument("--log-file-suffix")

        parser.add_argument("-r", "--refresh", action="store_true")
        parser.add_argument("-a", "--analyze", action="store_true")
        parser.add_argument("--analyze-deeply", action="store_true")
        parser.add_argument("-b", "--index", action="store_true")
        parser.add_argument("-s", "--scan", action="store_true")
        parser.add_argument("-i", "--info", action="store_true")
        parser.add_argument("-l", "--list", action="store_true")
        parser.add_argument("-g", "--generate", action="store_true")
        parser.add_argument("-t", "--tree", action="store_true")
        parser.add_argument("-w", "--reset", action="store_true")
        parser.add_argument("-n", "--add-section")
        parser.add_argument("--type", type=int)
        parser.add_argument("--agent")
        parser.add_argument("--location")
        parser.add_argument("--lang")
        parser.add_argument("-D", "--del-section", type=int)

        parser.add_argument("-c", "--section", type=int)
        parser.add_argument("-o", "--item", type=int)
        parser.add_argument("-d", "--directory")
        parser.add_argument("-f", "--file")

        parser.add_argument("-x", "--force", action="store_true")
        parser.add_argument("--no-thumbs", action="store_true")
        parser.add_argument("--chapter-thumbs-only", action="store_true")
        parser.add_argument("--thumbOffset")
        parser.add_argument("--artOffset")

        parser.add_argument("--media_item", type=int)

        self.args = parser.parse_args()

    def passthrough(self):
        log().debug("passing to real scanner")
        path = os.path.join(
            tvaf.get_default_plex_home(), tvaf.REAL_SCANNER_PATH)
        os.execv(path, sys.argv)

    def analyze_metadata_item(self):
        log().debug("analyzing metadata item: %s", self.args.item)
        tvaf_config = tvaf.get_metadata_item_tvaf(
            tvaf.get_default_db(), self.args.item)
        mdi = MetadataItem.find_by_id(tvaf_config, self.args.item)
        mdi.analyze()

    def analyze_media_item(self):
        log().debug("analyzing media item: %s", self.args.media_item)
        tvaf_config = tvaf.get_media_item_tvaf(
            tvaf.get_default_db(), self.args.media_item)
        mi = MediaItem(tvaf_config, self.args.media_item)
        mi.analyze()

    def run(self):
        self.parse_args()
        if self.args.section:
            if tvaf.get_section_tvaf(self.args.section):
                log().debug("ignoring scan of tvaf section")
                return

        if self.args.item:
            self.tvaf = tvaf.get_metadata_item_tvaf(
                tvaf.get_default_db(), self.args.item)
            if not self.tvaf:
                self.passthrough()

        if self.args.analyze and self.args.media_item:
            sys.exit(self.analyze_media_item())
        elif self.args.analyze and self.args.item:
            sys.exit(self.analyze_metadata_item())
        else:
            self.passthrough()


def main():
    log_to_syslog("pms-scan-shim")

    log().debug("%s", sys.argv)
    log().debug("%s", os.environ)

    try:
        return ScanWrapper().run()
    except Exception:
        log().exception("during %s", sys.argv)
        raise
