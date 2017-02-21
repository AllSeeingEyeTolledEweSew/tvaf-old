import json
import sqlite3
import threading


class CrudResult(object):

    ACTION_UPDATE = "update"
    ACTION_DELETE = "delete"

    def __init__(self, action, path, updated, keys):
        self.action = action
        self.path = path
        self.updated = updated
        self.keys = keys

    def __repr__(self):
        return "<action=%s, path=%s, updated=%d, keys=%s>" % (
            self.action, self.path, self.updated, self.keys)


def decode(value):
    if value and isinstance(value, basestring) and value[0] in ("{", "["):
        try:
            return json.loads(value)
        except ValueError:
            pass
    return value


def encode(value):
    if isinstance(value, dict) or isinstance(value, list):
        return json.dumps(value, sort_keys=True)
    return value


class TvafDb(object):

    def __init__(self, db_path):
        self.db_path = db_path
        self._db = None
        self._lock = threading.RLock()

    @property
    def db(self):
        with self._lock:
            if self._db:
                return self._db
            self._db = sqlite3.connect(self.db_path)
            self._db.execute(
                "create table if not exists item ("
                "path text not null, "
                "key text not null, "
                "value numeric, "
                "updated_at integer not null, "
                "deleted tinyint not null default 0, "
                "primary key (path, key))")
            self._db.execute(
                "create table if not exists global ("
                "name text not null primary key, "
                "value numeric)")
            self._db.execute(
                "create index if not exists "
                "item_on_updated_at on item (updated_at)")
            self._db.execute(
                "create index if not exists "
                "item_on_key_and_updated_at on item (key, updated_at)")
            self._db.execute(
                "create index if not exists "
                "item_on_key_and_value on item (key, value)")
            self._db.execute("pragma journal_mode=wal")
            return self._db

    def browse(self, path):
        prev = None
        if path == "/":
            path = ""
        c = self.db.execute(
            "select path from item where path > ? and path < ? and "
            "not deleted group by path",
            (path + "/", path + "0"))
        for child, in c:
            child = child[len(path)+1:].split("/", 1)[0]
            if child != prev:
                prev = child
                yield child

    def get(self, path, key):
        row = self.db.execute(
            "select value from item where path = ? and key = ? and "
            "not deleted",
            (path, key)).fetchone()
        if row:
            return decode(row[0])

    def get(self, path, keys=None):
        args = {"path": path}
        if isinstance(keys, list) or isinstance(keys, tuple):
            pred = "key in (%s)" % ",".join(
                ":key%d" % i for i in range(len(keys)))
            for i, key in enumerate(keys):
                args["key%d" % i] = key
        elif isinstance(keys, basestring):
            pred = "key = :key"
            args["key"] = keys
        else:
            pred = "1"
        c = self.db.execute(
            "select key, value from item where path = :path and "
            "%s and not deleted" % pred, args)
        if isinstance(keys, basestring):
            row = c.fetchone()
            return decode(row[1]) if row else None
        else:
            return { r[0]: decode(r[1]) for r in c }

    def search(self, **kwargs):
        if not kwargs:
            return
        joins = []
        values = []
        args = {}
        query = "select i0.path from"
        where_clauses = ["not deleted"]
        for i, (k, v) in enumerate(kwargs.iteritems()):
            if i == 0:
                query += " item i0"
            else:
                query += (
                    " inner join item i%(i)d on i0.path = i%(i)d.path" %
                    {"i": i})
            where_clauses.append("i%(i)d.key = :k%(i)d" % {"i": i})
            args["k%d" % i] = k
            if v is not None:
                where_clauses.append("i%(i)d.value = :v%(i)d" % {"i": i})
                args["v%d" % i] = encode(v)
            else:
                where_clauses.append("i%(i)d.value is None")
        query += " where " + " and ".join(where_clauses)
        c = self.db.execute(query, args)
        for path, in c:
            yield path

    def feed(self, timestamp=None, keys=None):
        if timestamp is None:
            timestamp = 0

        args = {
            "ts": timestamp}

        if keys:
            key_predicate = "(key in (%s))" % ",".join(
                ":key%d" % i for i in range(len(keys)))
            for i, key in enumerate(keys):
                args["key%d" % i] = key
            index = "item_on_key_and_updated_at"
        else:
            key_predicate = "1"
            index = "item_on_updated_at"

        c = self.db.execute(
            "select "
            "path, key, updated_at, deleted "
            "from item indexed by %(index)s where %(key_predicate)s and "
            "updated_at > :ts order by path" %
            {"key_predicate": key_predicate, "index": index}, args)

        cur = None
        for path, key, updated_at, deleted in c:
            if path != cur:
                if cur is not None:
                    yield CrudResult(
                        CrudResult.ACTION_DELETE if all_deleted
                        else CrudResult.ACTION_UPDATE,
                        cur, max_updated_at, keys)
                cur = path
                max_updated_at = updated_at
                all_deleted = bool(deleted)
                keys = set((key,))
            else:
                max_updated_at = max(max_updated_at, updated_at)
                all_deleted = all_deleted and deleted
                keys.add(key)
        if cur is not None:
            yield CrudResult(
                CrudResult.ACTION_DELETE if all_deleted
                else CrudResult.ACTION_UPDATE,
                cur, max_updated_at, keys)

    def update(self, path, data, timestamp=None):
        self.updatemany([(path, data)], timestamp=timestamp)

    def updatemany(self, pairs, timestamp=None):
        if timestamp is None:
            timestamp = self.tick()
        arglist = []
        for path, data in pairs:
            for k, v in data.iteritems():
                arglist.append(
                    {"path": path, "timestamp": timestamp, "key": k,
                     "value": encode(v)})
        for args in arglist:
            self.db.execute(
                "insert or ignore into item "
                "(path, key, value, updated_at) values "
                "(:path, :key, :value, :timestamp)", args)
            if args["value"] is None:
                condition = "value is null"
            else:
                condition = "value = :value"
            self.db.execute(
                "update item set value = :value, deleted = 0, "
                "updated_at = "
                "case when (not deleted) and %(condition)s then updated_at "
                "else :timestamp end "
                "where path = :path and key = :key and changes() = 0" %
                {"condition": condition}, args)
        # self.db.executemany(
        #     "insert or replace into item "
        #     "(path, key, value, updated_at, deleted) "
        #     "select "
        #     "i.path, i.key, i.value, "
        #     "case when item.value = i.value or "
        #     "(item.value is null and i.value is null) then "
        #     "coalesce(item.updated_at, i.timestamp) else i.timestamp end, "
        #     "0 "
        #     "from (select "
        #     ":path path, :key key, :value value, :timestamp timestamp) i "
        #     "left outer join item on item.path = i.path and "
        #     "item.key = i.key and not item.deleted", args)

    def delete(self, path, keys=None, timestamp=None):
        if timestamp is None:
            timestamp = self.tick()
        if keys is None:
            self.db.execute(
                "update item set deleted = 1, updated_at = :timestamp "
                "where path = :path and not deleted",
                {"path": path, "timestamp": timestamp})
        else:
            self.db.executemany(
                "update item set deleted = 1, updated_at = :timestamp "
                "where path = :path and key = :key and not deleted",
                [{"path": path, "timestamp": timestamp, "key": k}
                 for k in keys])

    def get_global(self, name):
        row = self.db.execute(
            "select value from global where name = ?", (name,)).fetchone()
        if row:
            return row[0]

    def set_global(self, name, value):
        self.db.execute(
            "insert or replace into global (name, value) values (?, ?)",
            (name, value))

    def tick(self):
        timestamp = self.get_timestamp() + 1
        self.set_global("timestamp", timestamp)
        return timestamp

    def get_timestamp(self):
        return self.get_global("timestamp") or 0
