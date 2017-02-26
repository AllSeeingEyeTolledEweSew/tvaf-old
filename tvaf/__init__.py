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

    MAX_PARAMS = 999

    def __init__(self, db_path, auto_ensure_indexes=True):
        self.db_path = db_path
        self._local = threading.local()
        self.auto_ensure_indexes = auto_ensure_indexes

    def ensure_indexes(self):
        self.db.execute(
            "create index if not exists "
            "item_on_updated_at on item (updated_at)")
        self.db.execute(
            "create index if not exists "
            "item_on_key_and_updated_at on item (key_id, updated_at)")
        self.db.execute(
            "create index if not exists "
            "item_on_key_and_value on item (key_id, value)")

    @property
    def db(self):
        if hasattr(self._local, "db"):
            return self._local.db
        db = sqlite3.connect(self.db_path)
        db.execute(
            "create table if not exists item ("
            "path_id integer not null, "
            "key_id integer not null, "
            "value blob, "
            "updated_at integer not null, "
            "deleted tinyint not null default 0, "
            "primary key (path_id, key_id))")
        db.execute(
            "create table if not exists global ("
            "name text not null primary key, "
            "value blob)")
        db.execute(
            "create table if not exists "
            "key (id integer primary key, name text not null)")
        db.execute(
            "create unique index if not exists key_on_name on key (name)")
        db.execute(
            "create table if not exists "
            "path (id integer primary key, name text not null)")
        db.execute(
            "create unique index if not exists path_on_name on key (path)")
        db.execute("pragma journal_mode=wal")
        self._local.db = db
        if self.auto_ensure_indexes:
            self.ensure_indexes()
        return db

    def browse(self, path):
        prev = None
        if path == "/":
            path = ""
        c = self.db.execute(
            "select path.name from path "
            "inner join item on path.id = item.path_id "
            "where path.name > ? and path.name < ? and not item.deleted "
            "group by path.name",
            (path + "/", path + "0"))
        for child, in c:
            child = child[len(path)+1:].split("/", 1)[0]
            if child != prev:
                prev = child
                yield child

    def get(self, path, keys=None):
        args = {"path": path}
        if isinstance(keys, list) or isinstance(keys, tuple):
            pred = "key.name in (%s)" % ",".join(
                ":key%d" % i for i in range(len(keys)))
            for i, key in enumerate(keys):
                args["key%d" % i] = key
        elif isinstance(keys, basestring):
            pred = "key.name = :key"
            args["key"] = keys
        else:
            pred = "1"
        c = self.db.execute(
            "select key.name, item.value from path "
            "inner join item on path.id = item.path_id "
            "inner join key on key.id = item.key_id "
            "where path.name = :path and "
            "%s and not item.deleted" % pred, args)
        if isinstance(keys, basestring):
            row = c.fetchone()
            return decode(row[1]) if row else None
        else:
            return { r[0]: decode(r[1]) for r in c }

    def search(self, *args, **kwargs):
        terms = args[0] if args else []
        if not kwargs and not terms:
            return
        joins = []
        values = []
        args = {}
        query = "select path.name from"
        where_clauses = []
        for i, (k, v) in enumerate(kwargs.iteritems() if kwargs else terms):
            if i == 0:
                query += " item i0"
            else:
                query += (
                    " inner join item i%(i)d on i0.path_id = i%(i)d.path_id" %
                    {"i": i})
            query += (
                " inner join key k%(i)d on i%(i)d.key_id = k%(i)d.id" %
                {"i": i})
            where_clauses.append("not i%(i)d.deleted" % {"i": i})
            where_clauses.append("k%(i)d.name = :k%(i)d" % {"i": i})
            where_clauses.append("i%(i)d.value is :v%(i)d" % {"i": i})
            args["k%d" % i] = k
            args["v%d" % i] = encode(v)
        query += " inner join path on i0.path_id = path.id"
        if where_clauses:
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
            key_predicate = "(key.name in (%s))" % ",".join(
                ":key%d" % i for i in range(len(keys)))
            for i, key in enumerate(keys):
                args["key%d" % i] = key
        else:
            key_predicate = "1"

        c = self.db.execute(
            "select "
            "path.name, key.name, item.updated_at, item.deleted "
            "from item "
            "inner join key on item.key_id = key.id "
            "inner join path on item.path_id = path.id "
            "where %(key_predicate)s and "
            "item.updated_at > :ts order by path.name" %
            {"key_predicate": key_predicate}, args)

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
        paths = list(set(path for path, data in pairs))
        keys = list(set(
            k for path, data in pairs for k, v in data.iteritems()))

        path_to_id = {}
        key_to_id = {}

        if paths:
            self.db.executemany(
                "insert or ignore into path (name) values (?)",
                [(path,) for path in paths])
        for i in range(0, len(paths), self.MAX_PARAMS):
            part = paths[i:i + self.MAX_PARAMS]
            path_to_id.update({
                p: i for i, p in self.db.execute(
                    "select id, name from path where name in (%s)" %
                    ",".join("?" for _ in range(len(part))), part)})
        assert len(paths) == len(path_to_id)

        if keys:
            self.db.executemany(
                "insert or ignore into key (name) values (?)",
                [(key,) for key in keys])
        for i in range(0, len(keys), self.MAX_PARAMS):
            part = keys[i:i + self.MAX_PARAMS]
            key_to_id.update({
                k: i for i, k in self.db.execute(
                    "select id, name from key where name in (%s)" %
                    ",".join("?" for _ in range(len(part))), part)})
        assert len(keys) == len(key_to_id)

        arglist = []
        for path, data in pairs:
            for k, v in data.iteritems():
                arglist.append(
                    {"path_id": path_to_id[path], "timestamp": timestamp,
                     "key_id": key_to_id[k], "value": encode(v)})

        self.db.executemany(
            "insert or ignore into item "
            "(path_id, key_id, value, updated_at) values "
            "(:path_id, :key_id, :value, :timestamp)", arglist)
        self.db.executemany(
            "update item set deleted = 0, value = :value, "
            "updated_at = :timestamp "
            "where path_id = :path_id and key_id = :key_id "
            "and (deleted or value is not :value)", arglist)

    def delete(self, path, keys=None, timestamp=None):
        if timestamp is None:
            timestamp = self.tick()
        args = {"path": path, "timestamp": timestamp}
        if keys is None:
            condition = "1"
        else:
            condition = (
                "key_id in (select id from key where name in (%s))" %
                ",".join(":k%d" % i for i in range(len(keys))))
            for i, k in enumerate(keys):
                args["k%d" % i] = k
        self.db.execute(
            "update item set deleted = 1, updated_at = :timestamp "
            "where path_id = (select id from path where name = :path) "
            "and not deleted and %(condition)s" % {"condition": condition},
            args)

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
