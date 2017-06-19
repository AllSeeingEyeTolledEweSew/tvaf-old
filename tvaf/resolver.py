import tvaf.btn
import tvaf.sync


class YatfsResolver(object):

    def __init__(self, tvafdb, inodb, mountpoint, btnapi):
        self.tvafdb = tvafdb
        self.inodb = inodb
        self.mountpoint = mountpoint
        self.btnapi = btnapi

    def __call__(self, path):
        bt_path = self.tvafdb.get(path, "version_bittorrent_path")
        m = tvaf.btn.BTN_TORRENT_ENTRY_PATH_REGEX.match(bt_path)
        if not m:
            return
        id = int(m.group("torrent_entry_id"))
        te = self.btnapi.getTorrentByIdCached(id)
        assert te
        _ = te.raw_torrent
        with self.btnapi.db:
            te.serialize()
        sc = tvaf.sync.SyncConfig(sync_btn=True, sync_btn_fs=True)
        syncer = tvaf.sync.Syncer(
            sc, tvafdb=self.tvafdb, inodb=self.inodb, btnapi=self.btnapi,
            mountpoint=self.mountpoint)
        syncer.sync()
