import threading

from btn import scrape as btn_scrape
from tvaf import btn as tvaf_btn
from tvaf import tvdb as tvaf_tvdb


class SyncConfig(object):

    def __init__(self, sync_tvdb=False, scrape_btn=False, sync_btn=False,
                 sync_btn_fs=False, sync_plex=False):
        self.sync_tvdb = sync_tvdb
        self.scrape_btn = scrape_btn
        self.sync_btn = sync_btn
        self.sync_btn_fs = sync_btn_fs
        self.sync_plex = sync_plex


class Syncer(object):

    OPS = ("sync_tvdb", "scrape_btn", "sync_btn", "sync_btn_fs", "sync_plex")

    def __init__(self, sync_config, tvaf_config):
        self.sync_config = sync_config
        self.tvaf = tvaf_config
        if tvaf_config:
            self.btnapi = tvaf_config.btnapi
            self.inodb = tvaf_config.inodb
            self.mountpoint = tvaf_config.mountpoint
            self.tvafdb = tvaf_config.tvafdb
            self.tvdb = tvaf_config.tvdb
            self.plex = tvaf_config.plex

        self._lock = threading.Lock()
        self._threads = {}

    @property
    def btnapi(self):
        return self.tvaf.btnapi

    @property
    def inodb(self):
        return self.tvaf.inodb

    @property
    def mountpoint(self):
        return self.tvaf.mountpoint

    @property
    def tvafdb(self):
        return self.tvaf.tvafdb

    @property
    def tvdb(self):
        return self.tvaf.tvdb

    @property
    def plex(self):
        return self.tvaf.plex

    def maybe_start(self, op):
        with self._lock:
            if not getattr(self.sync_config, op):
                return
            thread = self._threads.get(op)
            if thread is not None:
                return thread
            target = getattr(self, op)
            thread = threading.Thread(target=target, name=op)
            thread.start()
            self._threads[op] = thread
            return thread

    def maybe_join(self, op):
        thread = self.maybe_start(op)
        if thread:
            thread.join()

    def scrape_btn(self):
        scraper = btn_scrape.Scraper(self.btnapi)
        scraper.scrape()

    def sync_tvdb(self):
        syncer = tvaf_tvdb.Syncer(self.tvafdb, self.tvdb)
        syncer.sync()

    def sync_btn(self):
        self.maybe_join("scrape_btn")
        self.maybe_join("sync_tvdb")
        syncer = tvaf_btn.Syncer(self.tvafdb, self.btnapi)
        syncer.sync()

    def sync_btn_fs(self):
        self.maybe_join("sync_btn")
        syncer = tvaf_btn.FsSyncer(
            self.tvafdb, self.inodb, self.btnapi, self.mountpoint)
        syncer.sync()

    def sync_plex_unlocked(self):
        shows_key = "tvaf_ts_" + self.shows_tvaf_section_id
        start_ts = self.get_plex_global_int(shows_key)

        for crud in self.tvafdb.feed(
                timestamp=start_ts, keys=("version_id", "version_uris_file")):
            with tvaf.begin(self.plex.db):
                mi = tvaf.MediaItem.find_or_create(
                    self.tvaf, self.shows_tvaf_section_id, version_id,
                    create=(crud.action == crud.ACTION_UPDATE))
                if crud.action == crud.ACTION_UPDATE:
                    mi.update()
                elif crud.action == crud.ACTION_DELETE:
                    mi.delete()

        end_ts = self.tvafdb.get_timestamp()

        with tvaf.begin(self.plex.db):
            self.plex.set_global(shows_key, end_ts)

    def sync_plex(self):
        self.maybe_join("sync_btn_fs")
        with self.tvafdb.db:
            self.sync_plex_unlocked()

    def sync(self):
        for op in self.OPS:
            self.maybe_start(op)
        for op in self.OPS:
            self.maybe_join(op)
