import sqlite3
import threading

import btn


class SyncActionFeed(object):

    STATUS_OK = 0
    STATUS_ERROR = 1
    STATUS_PENDING = 2

    def __init__(self, key, db_path=None):
        if db_path is None:
            db_path = os.expanduser("~/.tvaf/feed.db")

        self.key = key
        self.db_path = db_path

        self._local = threading.local()

    @property
    def db(self):
        db = getattr(self._local, "db", None)
        if db is not None:
            return db
        if self.path is None:
            return None
        if not os.path.exists(os.path.dirname(self.db_path)):
            os.makedirs(os.path.dirname(self.db_path))
        db = sqlite3.connect(self.db_path)
        self._local.db = db
        db.row_factory = sqlite3.Row
        with db:
            db.execute(
                "create table if not exists feed_state ("
                "key text not null, "
                "item_id integer not null, "
                "status integer not null)")
            db.execute(
                "create unique index if not exists feed_state_key_item "
                "on feed_state (key, item_id)")
        return db

    def get_items(self):
        raise NotImplementedError()

    def add(self, item_id):
        raise NotImplementedError()

    def remove(self, item_id):
        raise NotImplementedError()

    def sync(self):
        with self.db:
            self.db.execute(
                "create temp table if not exists items ("
                "item_id integer not null primary key)")
            self.db.executemany(
                "insert into temp.items (item_id) values (?)",
                ((i,) for i in self.get_items()))
            c = self.db.execute(
                "select temp.items.item_id "
                "from temp.items "
                "left outer join feed_state "
                "on temp.items.item_id = feed_state.item_id "
                "and feed_state.key = ? "
                "where "
                "feed_state.item_id is null or "
                "feed_state.status = ?",
                (self.key, self.STATUS_ERROR))
            to_add = [r[0] for r in c]
            c = self.db.execute(
                "select feed_state.item_id "
                "from feed_state "
                "left outer join temp.items "
                "on temp.items.item_id = feed_state.item_id "
                "and feed_state.key = ? "
                "where "
                "temp.items.item_id is null and feed_state.status = ?",
                (self.key, self.STATUS_OK))
            to_remove = [r[0] for r in c]
            c = self.db.executemany(
                "insert or replace into feed_state (key, item_id, status) "
                "values (?, ?, ?)",
                ((self.key, i, self.STATUS_PENDING)
                 for i in itertools.chain(to_remove, to_add)))
            self.db.execute("drop table temp.items")

        for items, fn in ((to_remove, self.remove), (to_add, self.add)):
            for i in items:
                try:
                    fn(i)
                    r = self.STATUS_OK
                except:
                    log().exception("While processing item %s", i)
                    r = self.STATUS_ERROR
                with self.db:
                    self.db.execute(
                        "insert or replace into feed_state "
                        "(key, item_id, status)",
                        (self.key, i, r))


class BtnSyncActionFeed(SyncActionFeed):

    def __init__(self, key, db_path=None):
        super(BtnSyncActionFeed, self).__init__(key, db_path=db_path)
        self.api = btn.BTN()

    def get_items(self):
        for row in self.api.db.execute("select id from torrent_entry"):
            yield row["id"]
