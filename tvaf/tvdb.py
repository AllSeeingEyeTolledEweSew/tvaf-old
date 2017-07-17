import collections
import contextlib
import json
import logging
import requests
import threading
import time
import urlparse

import concurrent.futures
from requests_toolbelt.adapters import source as source_adapters

import tvafdb as tvafdb_lib


def log():
    return logging.getLogger(__name__)


class API(object):

    API_KEY = "0629B785CE550C8D"
    # TVAF_API_KEY = "9C20C04C21DF2DFD"
    HOST = "api.thetvdb.com"
    UPDATE_WINDOW = 7 * 24 * 60 * 60

    def __init__(self, apikey=None, username=None, userkey=None,
                 max_connections=10, max_retries=10, bind_ip=None):
        self.apikey = apikey or self.API_KEY
        self.username = username
        self.userkey = userkey

        self._lock = threading.RLock()
        self._token = None
        self._languages = None
        self.sessions = []
        self.sessions_semaphore = threading.Semaphore(max_connections)
        self.bind_ip = bind_ip
        self.max_retries = max_retries

    @property
    def token(self):
        with self._lock:
            if self._token is not None:
                return self._token
            payload = {"apikey": self.apikey}
            if self.username is not None:
                payload["username"] = self.username
            if self.userkey is not None:
                payload["userkey"] = self.userkey
            r = self.post_noauth("/login", payload)
            assert r.status_code == 200, (r.status_code, r.headers, r.text)
            self._token = r.json()["token"]
            return self._token

    def _create_session(self):
        session = requests.Session()
        if self.bind_ip:
            adapter = source_adapters.SourceAddressAdapter(self.bind_ip)
            session.mount("https://", adapter)
        return session

    @contextlib.contextmanager
    def session(self):
        with self.sessions_semaphore:
            with self._lock:
                if self.sessions:
                    session = self.sessions.pop(0)
                else:
                    session = self._create_session()
            try:
                yield session
            finally:
                with self._lock:
                    self.sessions.append(session)

    def call_noauth(self, method, path, data=None, headers=None, qd=None):
        headers = headers or {}
        headers["Accept"] = "application/json"
        qs = urlparse.urlencode(qd or {})
        url = urlparse.urlunparse(("https", self.HOST, path, None, qs, None))
        with self.session() as session:
            for _ in range(self.max_retries):
                try:
                    r = getattr(session, method)(
                        url, data=data, headers=headers, timeout=5)
                except (requests.exceptions.ConnectionError,
                        requests.exceptions.Timeout):
                    log().error("Got a connection error, retrying.")
                    continue
                if r.status_code != 502:
                    break
                log().error("Got a 502, retrying.")
            else:
                assert False, "Retries exceeded"
            return r

    def call(self, method, path, data=None, headers=None, qd=None):
        headers = headers or {}
        headers["Authorization"] = "Bearer " + self.token
        return self.call_noauth(
            method, path, data=data, headers=headers, qd=qd)

    def post(self, path, data, headers=None, qd=None):
        headers = headers or {}
        headers["Content-Type"] = "application/json"
        return self.call(
            "post", data=json.dumps(data or {}), headers=headers, qd=qd)

    def post_noauth(self, path, data, headers=None, qd=None):
        headers = headers or {}
        headers["Content-Type"] = "application/json"
        return self.call_noauth(
            "post", path, data=json.dumps(data or {}), headers=headers, qd=qd)

    def get(self, path, headers=None, qd=None):
        return self.call("get", path, headers=headers, qd=qd)

    @property
    def languages(self):
        with self._lock:
            if self._languages is not None:
                return self._languages
            r = self.get("/languages")
            assert r.status_code == 200, (r.status_code, r.headers, r.text)
            self._languages = r.json()["data"]
            return self._languages


class SeriesSyncer(object):

    def __init__(self, tvafdb, tvdb, series_id):
        self.tvafdb = tvafdb
        self.tvdb = tvdb
        self.series_id = series_id

    def get_data(self):
        pool = concurrent.futures.ThreadPoolExecutor(len(self.tvdb.languages))
        data = {}
        l_to_f = {}
        for language in self.tvdb.languages:
            l = language["abbreviation"]
            f = pool.submit(self.tvdb.get,
                "/series/%d" % self.series_id,
                headers={"Accept-Language": l})
            l_to_f[l] = f
        for l, f in l_to_f.iteritems():
            r = f.result()
            assert r.status_code == 200, (r.status_code, r.headers, r.text)
            d = r.json()
            if "invalidLanguage" in d.get("errors", {}):
                continue
            for k, v in d["data"].iteritems():
                if k in ("seriesName", "overview"):
                    data[k + "_" + l] = v
                else:
                    data[k] = v
        return data

    def get_episodes_page(self, page):
        episodes = {}
        links = None
        l_to_f = {}
        l0 = self.tvdb.languages[0]["abbreviation"]
        r = self.tvdb.get(
            "/series/%d/episodes" % self.series_id, qd={"page": page},
            headers={"Accept-Language": l0})
        if r.status_code == 404:
            return {"data": [], "links": {"first": 1, "last": 1}}
        pool = concurrent.futures.ThreadPoolExecutor(
            len(self.tvdb.languages) - 1)
        for language in self.tvdb.languages[1:]:
            l = language["abbreviation"]
            f = pool.submit(self.tvdb.get,
                "/series/%d/episodes" % self.series_id, qd={"page": page},
                headers={"Accept-Language": l})
            l_to_f[l] = f
        f = concurrent.futures.Future()
        f.set_result(r)
        l_to_f[l0] = f
        for l, f in l_to_f.iteritems():
            r = f.result()
            assert r.status_code == 200, (r.status_code, r.headers, r.text)
            d = r.json()
            links = d["links"]
            for episode in d["data"]:
                language = episode.pop("language")
                id = episode["id"]
                if id not in episodes:
                    episodes[id] = {}
                for k, v in episode.iteritems():
                    if k in language:
                        if not language[k]:
                            continue
                        episodes[id][k + "_" + language[k]] = v
                    else:
                        episodes[id][k] = v
        episodes = sorted(episodes.itervalues(), key=lambda e: e["id"])
        return {"data": episodes, "links": links}

    def get_episodes(self):
        page1 = self.get_episodes_page(1)
        l = page1["links"]

        pagenums = set(range(l["first"], l["last"] + 1)) - set((1, ))

        if pagenums:
            pool = concurrent.futures.ThreadPoolExecutor(len(pagenums))
            page_fs = [
                pool.submit(self.get_episodes_page, p) for p in pagenums]
            pages = [f.result() for f in page_fs]
        else:
            pages = []
        episodes = {}
        for page in [page1] + pages:
            for e in page["data"]:
                episodes[e["id"]] = e

        if not episodes:
            return []

        pool = concurrent.futures.ThreadPoolExecutor(len(episodes))
        fs = [
            pool.submit(self.tvdb.get, "/episodes/%d" % i)
            for i in episodes.iterkeys()]
        for f in concurrent.futures.as_completed(fs):
            r = f.result()
            assert r.status_code == 200, (r.status_code, r.headers, r.text)
            e = r.json()["data"]
            language = e.pop("language")
            for k, v in e.iteritems():
                if k in language:
                    continue
                episodes[e["id"]][k] = v
        return list(episodes.itervalues())

    def get_images_type(self, type):
        r = self.tvdb.get(
            "/series/%d/images/query" % self.series_id, qd={"keyType": type})
        assert r.status_code == 200, (r.status_code, r.headers, r.text)
        return r.json()["data"]

    def get_images(self):
        pool = concurrent.futures.ThreadPoolExecutor(10)

        series_images = []
        season_images = collections.defaultdict(list)

        r = self.tvdb.get("/series/%d/images" % self.series_id)
        if r.status_code == 404:
            return (series_images, season_images)
        assert r.status_code == 200, (r.status_code, r.headers, r.text)
        summary = r.json()["data"]
        types = [k for k, v in summary.iteritems() if v]
        type_to_f = {t: pool.submit(self.get_images_type, t) for t in types}

        for type, f in type_to_f.iteritems():
            images = f.result()
            for image in images:
                if type in ("season", "seasonwide"):
                    image["subKey"] = int(image["subKey"])
                    season = image["subKey"]
                    season_images[season].append(image)
                else:
                    series_images.append(image)

        return (series_images, season_images)

    def get(self):
        pool = concurrent.futures.ThreadPoolExecutor(3)

        series_data_f = pool.submit(self.get_data)
        episode_data_f = pool.submit(self.get_episodes)
        images_f = pool.submit(self.get_images)

        self.series_data = series_data_f.result()
        self.episode_data = episode_data_f.result()
        self.series_images, self.season_images = images_f.result()

        self.series_images = sorted(self.series_images, key=lambda i: i["id"])
        self.season_images = {
            s: sorted(images, key=lambda i: i["id"])
            for s, images in self.season_images.iteritems()}

    def get_existing_episodes_and_images(self):
        self.existing_season_paths = set()
        self.existing_episode_paths = set()
        self.existing_image_paths = set()
        base_path = "/tvdb_series=%d" % self.series_id
        for child in self.tvafdb.browse(base_path):
            k, v = child.split("=", 1)
            if k == "tvdb_season":
                season_path = base_path + "/" + child
                self.existing_season_paths.add(season_path)
                for child in self.tvafdb.browse(season_path):
                    path = season_path + "/" + child
                    k, v = child.split("=", 1)
                    if k == "tvdb_episode":
                        episode_path = season_path + "/" + child
                        for child in self.tvafdb.browse(episode_path):
                            k, v = child.split("=", 1)
                            if k == "tvdb_episode_id":
                                self.existing_episode_paths.add(
                                    episode_path + "/" + child)
                    elif k == "tvdb_image":
                        self.existing_image_paths.add(
                            season_path + "/" + child)
            elif k == "tvdb_image":
                self.existing_image_paths.add(base_path + "/" + child)

    def apply(self, ts):
        base_path = "/tvdb_series=%d" % self.series_id
        series_data = {
            "tvdb_series_%s" % k: v for k, v in self.series_data.iteritems()}
        self.tvafdb.update(
            "/tvdb_series=%d" % self.series_id, series_data, timestamp=ts)

        episode_paths = set()
        season_paths = set()
        pairs = []
        for episode_data in self.episode_data:
            season_path = (
                base_path + "/tvdb_season=%d" %
                (episode_data["airedSeason"] or 0))
            season_paths.add(season_path)
            path = season_path + "/tvdb_episode=%d/tvdb_episode_id=%d" % (
                episode_data["airedEpisodeNumber"] or 0, episode_data["id"])
            episode_paths.add(path)
            episode_data = {
                "tvdb_episode_%s" % k: v for k, v in episode_data.iteritems()}
            pairs.append((path, episode_data))
        self.tvafdb.updatemany(pairs, timestamp=ts)

        image_paths = set()
        pairs = []
        for image in self.series_images:
            path = base_path + "/tvdb_image=%d" % image["id"]
            image_paths.add(path)
            pairs.append((
                path,
                {"tvdb_image_" + k: v for k, v in image.iteritems()}))
        for s, images in self.season_images.iteritems():
            for image in images:
                path = (
                    base_path + "/tvdb_season=%d/tvdb_image=%d" %
                    (s, image["id"]))
                image_paths.add(path)
                pairs.append((
                    path,
                    {"tvdb_image_" + k: v for k, v in image.iteritems()}))
        self.tvafdb.updatemany(pairs, timestamp=ts)

        self.get_existing_episodes_and_images()

        for path in sorted(self.existing_season_paths - season_paths):
            series_data = self.tvafdb.get(path)
            delete_keys = set(
                k for k in series_data if k.startswith("tvdb_season_"))
            if delete_keys:
                log().info("Deleting %s.", path)
                self.tvafdb.delete(path, keys=delete_keys, timestamp=ts)
        for path in sorted(self.existing_episode_paths - episode_paths):
            episode_data = self.tvafdb.get(path)
            delete_keys = set(
                k for k in episode_data if k.startswith("tvdb_episode_"))
            if delete_keys:
                log().info("Deleting %s.", path)
                self.tvafdb.delete(path, keys=delete_keys, timestamp=ts)
        for path in sorted(self.existing_image_paths - image_paths):
            image_data = self.tvafdb.get(path)
            delete_keys = set(
                k for k in image_data if k.startswith("tvdb_image_"))
            if delete_keys:
                log().info("Deleting %s.", path)
                self.tvafdb.delete(path, keys=delete_keys, timestamp=ts)


class Syncer(object):

    OLDEST_TS = 1163101692
    GRACE = 60 * 60
    CACHE_TIME = 2 * 600
    DESIRED_BATCH_SIZE = 32

    def __init__(self, tvafdb, tvdb):
        self.tvafdb = tvafdb_lib.TvafDb(tvafdb.path, auto_ensure_indexes=False)
        self.tvdb = tvdb

    def get_series_id_to_last_updated(self, ts):
        r = self.tvdb.get("/updated/query", qd={"fromTime": ts})
        assert r.status_code == 200, (r.status_code, r.headers, r.text)
        entries = r.json()["data"] or []
        series_id_to_last_updated = {}
        for entry in entries:
            series_id = entry["id"]
            last_updated = entry["lastUpdated"]
            series_id_to_last_updated[series_id] = last_updated
        return series_id_to_last_updated

    def sync_step(self):
        ts = self.tvafdb.get_global("tvdb_sync_ts") or 0
        if not ts:
            ts = self.OLDEST_TS
            log().info("Getting full history. Dropping indexes.")
            self.tvafdb.drop_indexes()

        updated_prev = {}
        syncers = {}
        while True:
            all_updated = self.get_series_id_to_last_updated(ts)
            if (all_updated and
                    ts + self.tvdb.UPDATE_WINDOW > time.time() + self.GRACE):
                now = max(all_updated.itervalues())
            else:
                now = time.time()
            cache_limit = now - self.CACHE_TIME
            updated = {}

            prev_u = None
            sub_batch = {}
            for u, id in sorted((u, id) for id, u in all_updated.iteritems()):
                if u < cache_limit and u != prev_u:
                    if len(updated) + len(sub_batch) > self.DESIRED_BATCH_SIZE:
                        log().debug("reached batch size at %d.", u)
                        break
                    log().debug("adding sub-batch: %d for %s", len(sub_batch),
                            prev_u)
                    updated.update(sub_batch)
                    sub_batch = {}
                sub_batch[id] = u
                prev_u = u
            if not updated:
                log().debug("sub-batch too large, but must use anyway.")
                updated = sub_batch
            log().debug("got batch size: %d.", len(updated))

            if updated == updated_prev:
                break

            if updated_prev:
                log().info("Something changed while we were getting data.")

            should_get = [
                series_id
                for series_id, u in updated.iteritems()
                if u != updated_prev.get(series_id)]

            pool = concurrent.futures.ThreadPoolExecutor(
                min(len(should_get), self.DESIRED_BATCH_SIZE))

            fs = []
            for series_id in should_get:
                if series_id not in syncers:
                    syncers[series_id] = SeriesSyncer(
                        self.tvafdb, self.tvdb, series_id)
                fs.append(pool.submit(syncers[series_id].get))

            for i, f in enumerate(concurrent.futures.as_completed(fs)):
                f.result()
                log().info("Got %d/%d.", i + 1, len(fs))

            updated_prev = updated

        done = False
        if not updated:
            next_ts = ts + self.tvdb.UPDATE_WINDOW
        else:
            next_ts = max(updated.itervalues()) + 1

        if next_ts > cache_limit:
            next_ts = cache_limit
            done = True

        prev_ts = ts
        for i, (series_id, u) in enumerate(
                sorted(updated.iteritems(), key=lambda i: i[1])):
            log().info("Committing %d/%d: %d", i + 1, len(updated), series_id)
            with self.tvafdb.db:
                #assert prev_ts == self.tvafdb.get_global("tvdb_sync_ts")
                tvaf_ts = self.tvafdb.tick()
                syncers[series_id].apply(tvaf_ts)
                self.tvafdb.set_global("tvdb_sync_ts", u)
            prev_ts = u
        with self.tvafdb.db:
            assert prev_ts == self.tvafdb.get_global("tvdb_sync_ts")
            self.tvafdb.set_global("tvdb_sync_ts", next_ts)

        log().info("Synced %s -> %s.", ts, next_ts)

        return done

    def sync(self):
        while True:
            if self.sync_step():
                break
        log().info("Caught up.")
        log().info("Ensuring indexes are built.")
        self.tvafdb.ensure_indexes()
