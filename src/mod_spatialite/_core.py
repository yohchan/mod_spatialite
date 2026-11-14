#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
SpatiaLiteを利用する
同じディレクトリにmod_spatialiteディレクトリを配置し、直下にmod_spatialiteほかを入れておく。
mod_spatialite入手先：https://www.gaia-gis.it/gaia-sins/

c.f.
v4.3.0a win-x64: http://www.gaia-gis.it/gaia-sins/windows-bin-amd64/mod_spatialite-4.3.0a-win-amd64.7z
v5.0.0beta win-x64: http://www.gaia-gis.it/gaia-sins/windows-bin-NEXTGEN-amd64/mod_spatialite-NG-win-amd64.7z

Author: yoh_chan
"""

from __future__ import annotations  # 同モジュール内の別クラスを参照させるのに必要
import os
import sys
import types
import sqlite3
import contextlib
import inspect
import logging
from collections.abc import Iterator

# import pdb  # for debug

# メッセージは logging で出す（ロガー名 mod_spatialite._core）。ハンドラは設定しない。
# WARNING 以上は Python の既定で stderr に出る。INFO を見るには利用側で
# logging.basicConfig(level=logging.INFO) 等を設定する。
logger = logging.getLogger(__name__)


class SpatiaLiteConnection(sqlite3.Connection):
    """SpatiaLite 拡張を読み込んだ sqlite3 の接続。

    トランザクション: 初期化のあと isolation_level=None（自動コミット）にしている。
    このため execute / executemany は 1 文ごとにコミットされ、大量の行を書き込むと
    遅い（ファイルシステムによっては数万行で数秒が十数分になる）。まとめて書き込むときは
    明示的に BEGIN / COMMIT（失敗時は ROLLBACK）で囲む。
    自動コミットの接続では、Python の `with con:` はトランザクションを開始しない
    （ブロックの終わりにコミットするだけで、BEGIN は出さない）ため、速くはならない。
    begin() は BEGIN を出すだけで、例外時の ROLLBACK はしない。

    ref: https://stackoverflow.com/questions/23683886
    ref: https://stackoverflow.com/questions/15556181
    ref: https://codereview.stackexchange.com/questions/134535
    fef: https://docs.python.jp/3/library/sqlite3.html#sqlite3.connect
    """

    def __init__(self, *args, p_mod_spatialite=None, i_page_size=None, **kwargs):
        """SpatiaLite 拡張を読み込んだ接続を開く。

        引数は sqlite3.connect と同じ（URI は uri=True で渡す。例: "file:a.sqlite?mode=ro"）。
        ファイルが無い場合は sqlite3 自身が新しく作る（mode=ro では作らずに失敗する）。

        p_mod_spatialite: Windows で SpatiaLite の DLL を置いたフォルダ
        i_page_size: 新しく DB を作るときのページサイズ（例: 65536）。既存の DB では
            無視する（変えるには set_page_size を使う）
        acv（キーワード）: True なら接続前に DB を <名前>_acv.<拡張子> へ複製する
        """
        # SpatiaLite のファイル入出力系の関数（ImportSHP・ExportSHP 等）を使えるようにする。
        # 拡張を読み込む前に設定する必要がある。利用側が既に設定していれば上書きしない。
        os.environ.setdefault("SPATIALITE_SECURITY", "relaxed")
        logger.info("SPATIALITE_SECURITY=%s", os.environ.get("SPATIALITE_SECURITY"))

        try:
            if kwargs["acv"]:
                if args[0] == ":memory:":
                    logger.info(
                        "Connecting with memory db mode. You cannot archive existing data..."
                    )
                elif str(args[0]).startswith("file:") or not os.path.isfile(args[0]):
                    logger.warning(
                        "acv: %s is a URI or does not exist yet; no archive was made.",
                        args[0],
                    )
                else:
                    import shutil

                    p_dir, s_db = os.path.split(args[0])
                    shutil.copyfile(
                        args[0],
                        os.path.join(
                            p_dir,
                            "{}_acv{}".format(
                                os.path.splitext(s_db)[0], os.path.splitext(s_db)[1]
                            ),
                        ),
                    )

                del kwargs[
                    "acv"
                ]  # 辞書の要素を削除（そのまま残っているとsqlite3.ConnectionでInvalidになる。）

        except KeyError:
            pass

        sqlite3.Connection.__init__(self, *args, **kwargs)  # 継承
        self.p_mod_spatialite = p_mod_spatialite
        self.i_page_size = i_page_size
        self.b_uri = bool(
            kwargs.get("uri", False)
        )  # URI を解釈する接続か（attach_db で使う）
        s_ver = self.initialize()

        logger.info("Successfully connected to SQLite DB with mod_spatialite.")
        logger.info(("DB-path: {}".format(*args)))
        logger.info(("Spatialite version: {}".format(s_ver)))

        # need to set proj db file SpatiaLite5<= and PROJ5<=
        # 同梱proj.dbはWindows同梱PROJ専用。Linux/WSLではOS側PROJが自前のproj.dbを持つため設定しない
        if sys.platform == "win32" and float(s_ver[:3]) >= 5:
            # 配置換え（src/mod_spatialite/ への移動）前と同じく、リポジトリ直下の proj.db を読む。
            # Windows での SpatiaLite・proj.db の場所の決め方は見直し予定（OPEN_ITEMS 参照）
            p_projdb = os.path.join(
                os.path.dirname(
                    os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
                ),
                "proj.db",
            )
            self.execute(f"""SELECT PROJ_SetDatabasePath('{p_projdb}');""")
            logger.info(
                self.execute(f"""SELECT PROJ_GetDatabasePath();""").fetchone()[0]
            )
            logger.info(f"Successflly append proj.db path: {p_projdb}")

    # mod_spatialiteの適用と初期化
    def initialize(self, row_factory=sqlite3.Row):
        # 辞書アクセスを可能にする http://docs.python.jp/2/library/sqlite3.html#row
        # sqlite3.Row型でカーソルを利用できるようになる。行要素に対して列名による辞書アクセスが可能。
        # .keys()で列名のリストも取得可能。
        self.row_factory = row_factory  # 辞書アクセスしないならNoneを指定

        # 自動コミットモードを有効にする
        # http://docs.python.jp/2/library/sqlite3.html
        self.isolation_level = None

        # PATHの追加
        # DLL探索パスの操作はWindows同梱DLL専用。Linux/WSLではOSのlibsqlite3-mod-spatialiteを
        # 標準ライブラリパスからロードするため不要
        if sys.platform == "win32":
            if self.p_mod_spatialite is not None:
                os.environ["PATH"] = f"{self.p_mod_spatialite};{os.environ['PATH']}"
                os.environ["PATH"] = (
                    f"{os.path.join(self.p_mod_spatialite, 'mod_spatialite')};{os.environ['PATH']}"
                )
            else:
                p_mod = (
                    "C:/Dropbox/Files/Yoh_scripts/Git_yoh/mod_spatialite/mod_spatialite"
                )
                os.environ["PATH"] = (
                    f"{os.path.abspath(p_mod)};{'mod_spatialite'};{os.environ['PATH']}"
                )

        # mod_spatialiteの読み込み
        self.isolation_level = ""
        self.enable_load_extension(True)
        self.execute("SELECT load_extension('mod_spatialite');")

        # 新しい DB（まだページが無い）なら、テーブルを作る前にページサイズを設定する
        if getattr(self, "i_page_size", None) is not None:
            if self.execute("PRAGMA main.page_count;").fetchone()[0] == 0:
                self.execute(f"PRAGMA main.page_size = {int(self.i_page_size)};")
            else:
                logger.info(
                    "i_page_size is ignored for an existing database (use set_page_size)."
                )

        # メタデータが無い（未初期化の）DB のときだけ登録して SpatiaLite DB とする。
        # 判定は主接続（main）のみを見る（ATTACH 先の影響を受けない）。
        # 既存 DB で毎回実行すると、"table spatial_ref_sys already exists" が出るため。
        if not self.execute(
            "SELECT count(*) FROM main.sqlite_master"
            " WHERE type = 'table' AND name = 'spatial_ref_sys';"
        ).fetchone()[0]:
            i_res = self.execute("""SELECT InitSpatialMetaData(1);""").fetchone()[
                0
            ]  # 1はトランザクション有効（高速）
            if i_res != 1:
                logger.warning(
                    "InitSpatialMetaData failed (read-only connection?): result=%s",
                    i_res,
                )
        self.isolation_level = None

        # SpatiaLiteのバージョンを返す
        return self.get_spatialite_version()

    def get_spatialite_version(self):
        """
        SpatiaLiteのバージョンを返す
        """

        return self.execute("""SELECT spatialite_version();""").fetchone()[0]

    # 切断
    def disconnect(self, flg_vacuum=False, flg_upgrade=False):
        """接続を閉じる。

        既定では閉じるだけ。VACUUM（DB の作り直し）と upgrade_db（統計・トリガの更新）は
        大きい DB では時間がかかるため、必要なときに flg_vacuum=True / flg_upgrade=True で指定する。
        """
        if flg_vacuum:
            logger.info("vacuuming db...")
            self.vacuum()

        if flg_upgrade:
            logger.info("upgrading db...")
            self.upgrade_db()

        p_db = self.get_dbpath()
        self.close()
        logger.info(("the database {} was successfully closed.".format(p_db)))

    # begin
    def begin(self):
        self.execute("""BEGIN;""")

    @contextlib.contextmanager
    def transaction(self) -> Iterator[SpatiaLiteConnection]:
        """BEGIN で始め、正常に抜けたら COMMIT、例外なら ROLLBACK するコンテキストマネージャ。

        接続は自動コミットのため、大量の書き込みはこれで囲むと 1 回のコミットで済む。

            with con.transaction():
                con.executemany("INSERT INTO t VALUES (?, ?)", l_rows)

        既にトランザクション中（begin() 等で BEGIN 済み）のときは入れ子にできないため、
        sqlite3.OperationalError になる。
        """
        self.execute("BEGIN;")
        try:
            yield self
        except BaseException:
            self.rollback()
            raise
        else:
            self.commit()

    def set_page_size(self, i_page_size: int) -> int:
        """既存の DB のページサイズを変える（PRAGMA page_size のあと VACUUM で作り直す）。

        WAL モードでは変えられないため、その場合は何もせず警告する。変更後のページサイズを返す。
        """
        s_mode = self.execute("PRAGMA main.journal_mode;").fetchone()[0]
        if str(s_mode).lower() == "wal":
            logger.warning("set_page_size: page size cannot be changed in WAL mode.")
        else:
            self.execute(f"PRAGMA main.page_size = {int(i_page_size)};")
            self.vacuum()
        return self.execute("PRAGMA main.page_size;").fetchone()[0]

    # vacuum
    def vacuum(self):
        self.execute("""vacuum;""")

    # attach
    def attach_db(self, p_db, s_alias="a", flg_readonly=False):
        """DB を別名 s_alias で ATTACH する。

        p_db: ファイルのパス、または URI（"file:...?mode=ro" 等。uri=True で開いた接続のみ）
        flg_readonly: True ならパスから読み取り専用の URI を作って ATTACH する
            （uri=True で開いた接続のみ。例: SpatiaLiteConnection(p, uri=True)）

        パスで指定してファイルが無い場合は、従来どおり WARNING を出して何もしない。
        URI を解釈しない接続に URI・flg_readonly を渡すと ValueError（SQLite は URI を
        ファイル名として扱い、別のファイルを作ってしまうため）。
        """
        from urllib.parse import quote

        s_db = str(p_db)
        b_is_uri = s_db.startswith("file:")
        if (b_is_uri or flg_readonly) and not getattr(self, "b_uri", False):
            raise ValueError(
                "attach_db: URI / flg_readonly=True requires a connection opened with"
                " uri=True (e.g. SpatiaLiteConnection(p_db, uri=True))."
            )

        if b_is_uri:
            s_target = s_db
        else:
            if not os.path.isfile(s_db):
                logger.warning(("oops... db file: {} is not exist.".format(s_db)))
                return
            s_abs = os.path.abspath(s_db)
            s_target = f"file:{quote(s_abs)}?mode=ro" if flg_readonly else s_abs

        self.execute(f"""ATTACH DATABASE ? AS "{s_alias}";""", (s_target,))
        logger.info(
            "attached database: %s as %s into: %s",
            s_target,
            s_alias,
            os.path.split(self.get_dbpath())[1],
        )

    # detach
    def detach_db(self, s_alias="a"):
        sql = """detach database {};""".format(s_alias)
        self.execute(sql)

    # dbpathを得る
    def get_dbpath(self, dbname="main"):
        sql = """PRAGMA database_list;"""
        ld_fetch = self.execute(sql).fetchall()

        for d_fetch in ld_fetch:
            if d_fetch["name"] == dbname:
                return d_fetch["file"]
            else:
                pass

        logger.warning(('no such table: "{}"'.format(dbname)))
        return 0

    # check Routing
    def ch_able_routing(self):
        import sys

        logger.info(("{}check routing libraries...".format(os.linesep)))

        # check SpatiaLite version
        sql = """SELECT spatialite_version();"""
        try:
            s_ver = self.execute(sql).fetchone()[0]
        except self.OperationalError:
            logger.info(
                "Error: spatialite version was not able to fetch... Did you mount mod_spatialite?"
            )
            sys.exit()

        if int(s_ver[0]) < 5:
            logger.warning(
                ('mounted spatialite version "{}" is not suitable...'.format(s_ver))
            )
            sys.exit()

        # has VirtualRouting?
        sql = """SELECT HasRouting();"""
        try:
            s_has = self.execute(sql).fetchone()[0]
        except self.OperationalError:
            logger.warning(
                "Error: spatialite has not VirtualRouting... Please check libraries."
            )
            sys.exit()

        if int(s_has) != 1:
            logger.warning(
                "Error: VirtualRouting is not active... Sequences will be stopped."
            )
            sys.exit()

        logger.info("check OK. routing available.")

    # check table exists or not
    def ch_exists_table(
        self, s_tbl, s_schema=None, flg_include_views=False, flg_exact=False
    ):
        """表があるかを返す（s_schema・flg_include_views は get_tbllist と同じ）。

        flg_exact: True なら名前を完全一致（大文字小文字は区別しない）で照合する。
            False（既定）は従来どおり LIKE で照合するため、_ と % がワイルドカードとして働く。
        """
        if flg_exact:
            s_master = (
                "sqlite_master" if s_schema is None else f'"{s_schema}".sqlite_master'
            )
            s_types = "('table', 'view')" if flg_include_views else "('table')"
            i_n = self.execute(
                f"""SELECT count(*) FROM {s_master}
                WHERE "type" IN {s_types} AND "name" = ? COLLATE NOCASE;""",
                (s_tbl,),
            ).fetchone()[0]
            return i_n > 0

        result = self.get_tbllist(
            s_tbl, s_schema=s_schema, flg_include_views=flg_include_views
        )
        if len(result) == 0:
            return False
        else:
            return True

    # check table exists or not
    def ch_exists_col(self, gt_or_tbl, s_col=None, s_schema=None):
        if isinstance(gt_or_tbl, GTBL):
            s_tbl = gt_or_tbl.name
            s_col = gt_or_tbl.gc
        else:
            s_tbl = gt_or_tbl
            if s_col is None:
                logger.warning("if gt_or_tbl is not GTBL, s_col is mandatory.")

        result = [
            d_colsinfo["name"]
            for d_colsinfo in self.get_columnsdef(s_tbl, s_schema=s_schema)
        ]

        if s_col in result:
            return True
        else:
            return False

    # update layer statistics
    def update_layerstats(self, s_tbl=None, s_gc=None):
        if s_gc is None:
            if s_tbl is None:
                self.execute("""SELECT UpdateLayerStatistics();""")
            else:
                self.execute("""SELECT UpdateLayerStatistics('{}');""".format(s_tbl))
        else:
            self.execute(
                """SELECT UpdateLayerStatistics('{}', '{}');""".format(s_tbl, s_gc)
            )

    # update layer statistics
    def upgrade_geomtriggers(self, flg_transaction=True):
        self.execute(
            """SELECT UpgradeGeometryTriggers({});""".format(flg_transaction * 1)
        )

    # upgrade database (free memory and update metadata)
    def upgrade_db(self, flg_transaction=True):
        self.update_layerstats()
        self.upgrade_geomtriggers(flg_transaction)

    # 連番を持つテーブルを生成する（カラムはPK_UID）をそのまま使用する
    def create_sequential_data(self, s_tbl, limit):
        self.create_table(s_tbl)
        self.begin()
        for n in range(limit):
            self.execute("""insert into {} values (NULL);""".format(s_tbl))
        self.commit()

    # 長方形を生成する
    def create_window(self, gt_window, l_nesw):
        n = l_nesw[0]
        e = l_nesw[1]
        s = l_nesw[2]
        w = l_nesw[3]

        self.create_table(gt_window)
        self.execute(
            """
            INSERT INTO "{tbl}" VALUES (NULL, BuildMbr({w}, {s}, {e}, {n}, {epsg}));
        """.format(tbl=gt_window.name, w=w, s=s, e=e, n=n, epsg=gt_window.epsg)
        )

        return gt_window

    # テーブルの生成（gtblを与えるとジオテーブルを生成する）
    def create_table(
        self, gt_or_tbl, ll_coldef=None, flg_pk=True, overwrite=True, flg_temp=False
    ):
        s_tbl = self.get_tblname(gt_or_tbl)
        if overwrite:
            self.drop_geotable(s_tbl)
        if ll_coldef is None:
            if flg_pk:
                sql = """create table "{}" ("PK_UID" INTEGER PRIMARY KEY AUTOINCREMENT);""".format(
                    s_tbl
                )
                if flg_temp:
                    sql = sql.replace("create table", "create temp table")
                self.execute(sql)
            else:
                logger.warning("You must give at least one of ll_coldef and flg_pk.")
                return
        else:
            ll_coldef = [
                [l_coldef[0], self.get_dtype(l_coldef[1])] for l_coldef in ll_coldef
            ]
            if flg_pk:
                s_coldef = ", ".join(
                    [""""PK_UID" INTEGER PRIMARY KEY AUTOINCREMENT"""]
                    + [f""""{l_coldef[0]}" {l_coldef[1]}""" for l_coldef in ll_coldef]
                )
            else:
                s_coldef = ", ".join(
                    [f""""{l_coldef[0]}" {l_coldef[1]}""" for l_coldef in ll_coldef]
                )
            s_temp = "TEMP " if flg_temp else ""
            sql = f"""CREATE {s_temp}TABLE "{s_tbl}" ( {s_coldef} );"""
            self.execute(sql)

        # ジオテーブルを生成する場合
        if isinstance(gt_or_tbl, GTBL):
            self.add_geomcol(gt_or_tbl)

        return gt_or_tbl

    # drop table
    def drop_table(self, gt_or_tbl, db_prefix=None):
        if self.get_spatialite_version() < "5.0.0":
            logger.info(
                f"Spatialite version is older for DropTable ({self.get_spatialite_version()})."
            )
            logger.info(f"So the prosess uses DropGeoTable instead.")
            self.drop_geotable(gt_or_tbl, db_prefix=db_prefix)
        else:
            s_tbl = self.get_tblname(gt_or_tbl)
            if db_prefix is None:
                self.execute(f"""SELECT DropTable(NULL, '{s_tbl}', 1);""")
            else:
                self.execute(f"""SELECT DropTable('{db_prefix}', '{s_tbl}', 1);""")

    # ジオテーブルの削除（ジオメトリがないテーブルでもよい）
    def drop_geotable(self, gt_or_tbl, i_transaction=1, db_prefix=None):
        if self.get_spatialite_version() < "5.0.0":
            s_tbl = self.get_tblname(gt_or_tbl)
            if db_prefix is None:
                sql = f"""SELECT DropGeoTable('{s_tbl}', {i_transaction});"""
            else:
                sql = f"""SELECT DropGeoTable('{db_prefix}', '{s_tbl}', {i_transaction});"""
            self.execute(sql)

            # 何故かテーブル本体が削除されないので、こちらで削除
            self.execute(f"""DROP TABLE IF EXISTS "{s_tbl}";""")  # deprecated

        else:
            logger.info(
                f"Spatialite version is newer for DropGeoTable ({self.get_spatialite_version()})."
            )
            logger.info(f"So the prosess uses DropTable instead.")
            self.drop_table(gt_or_tbl, db_prefix=db_prefix)

    # add unique index
    def add_uindex(self, gt_or_tbl, s_col):
        s_tbl = self.get_tblname(gt_or_tbl)
        sql = """create unique index "uix_{0}_{1}" on "{0}" ("{1}");""".format(
            s_tbl, s_col
        )
        try:
            self.execute(sql)
            logger.info(
                (
                    'A unique index was successfully added on "{tbl}"."{col}"'.format(
                        tbl=s_tbl, col=s_col
                    )
                )
            )
        except self.OperationalError:
            logger.info(
                (
                    'It seems to already have unique index "{}_{}". Pass process.'.format(
                        s_tbl, s_col
                    )
                )
            )

    # add index
    def add_index(self, gt_or_tbl, s_col):
        s_tbl = self.get_tblname(gt_or_tbl)
        sql = """create index "ix_{0}_{1}" on "{0}" ("{1}");""".format(s_tbl, s_col)
        try:
            self.execute(sql)
            logger.info(
                (
                    'An index was successfully added on "{tbl}"."{col}"'.format(
                        tbl=s_tbl, col=s_col
                    )
                )
            )

        except self.OperationalError:
            logger.info(
                (
                    'It seems to already have index "{}_{}". Process was skipped.'.format(
                        s_tbl, s_col
                    )
                )
            )

    # drop index
    def drop_index(self, gt_or_tbl, s_col):
        s_tbl = self.get_tblname(gt_or_tbl)
        sql = """
          DROP INDEX IF EXISTS "{tbl}_{col}";  -- compatible old module
          -- drop index if exists "ix_{tbl}_{col}";
          -- drop index if exists "uix_{tbl}_{col}";
        """
        self.execute(sql.format(tbl=s_tbl, col=s_col))

        logger.info(('The index "{}_{}" was dropped.'.format(s_tbl, s_col)))

    # add spatial index
    def add_spindex(self, gt_or_tbl, s_gc=None):
        s_tbl, s_gc = self.get_tblname_and_gc(gt_or_tbl, s_gc)
        sql = """select CheckSpatialIndex('{}', '{}');"""
        flg = self.execute(sql.format(s_tbl, s_gc)).fetchone()[0]

        if flg is None:  # if there is no spatial index, create it.
            logger.info(
                ('creating spatial index on "{}" of table "{}".'.format(s_gc, s_tbl))
            )
            sql = """select CreateSpatialIndex('{}','{}');""".format(s_tbl, s_gc)
            self.execute(sql)
        elif flg == 1:
            logger.info("It seems to be already spatial index. Pass process.")

    # drop spatial index
    def drop_spindex(self, gt_or_tbl, s_gc=None):
        s_tbl, s_gc = self.get_tblname_and_gc(gt_or_tbl, s_gc)
        sql = """select DisableSpatialIndex('{}','{}');"""
        self.execute(sql.format(s_tbl, s_gc))

    # add column and return column name
    def add_column(
        self, s_tbl, s_col, dtype, default_value=None, refresh=False, if_exists="skip"
    ):
        ld_coldef = self.get_columnsdef(s_tbl)
        l_col = [d["name"] for d in ld_coldef]
        if s_col in l_col:
            if if_exists == "skip":
                logger.info(f'Column "{s_col}" already exists. Process was skipped.')
                return s_col
            elif if_exists == "replace":
                logger.info(f'Column "{s_col}" already exists. It will be replaced.')
                self.drop_column(s_tbl, s_col)
            else:
                raise ValueError(
                    f'if_exists must be "skip" or "replace", but {if_exists} was set.'
                )

        dtype = self.get_dtype(dtype)
        if default_value is None:
            self.execute(f"""ALTER TABLE "{s_tbl}" ADD COLUMN "{s_col}" {dtype};""")
        else:
            self.execute(
                f"""ALTER TABLE "{s_tbl}" ADD COLUMN "{s_col}" {dtype} DEFAULT {default_value};"""
            )

        self.refresh_table(s_tbl) if refresh else None

        return s_col

    # add columns
    def add_columns(self, s_tbl, ll_coldef, refresh=False):
        for l_coldef in ll_coldef:
            if len(l_coldef) == 2:
                self.add_column(s_tbl, l_coldef[0], l_coldef[1])
            else:  # len(l_coldef) == 3 のみ考慮
                self.add_column(s_tbl, l_coldef[0], l_coldef[1], l_coldef[2])

        self.refresh_table(s_tbl) if refresh else None

    # add PK_UID column
    def add_pkuid_column(self, gt_or_tbl, refresh=False):
        s_tbl = gt_or_tbl.name if isinstance(gt_or_tbl, GTBL) else gt_or_tbl
        logger.info(f"""adding PK_UID column to "{s_tbl}"...""")

        # PRIMARY KEY はテーブル作成後には追加できないので、新規にテーブルを追加する必要がある
        s_tbl_tmp = self.get_temptblname()
        ld_coldef = self.get_columnsdef(s_tbl)
        ll_coldef = [
            [d_coldef["name"], d_coldef["type"]]
            for d_coldef in ld_coldef
            if d_coldef["pk"] != 1
        ]

        # create new table with PK_UID
        self.create_table(s_tbl_tmp, ll_coldef, flg_pk=True)

        # copy data
        sql = f"""
          INSERT INTO "{s_tbl_tmp}" ({", ".join([f'"{l_coldef[0]}"' for l_coldef in ll_coldef])})
          SELECT {", ".join([f'"{l_coldef[0]}"' for l_coldef in ll_coldef])}
          FROM "{s_tbl}"
        ;"""
        self.begin()
        self.execute(sql)
        self.commit()

        # drop old table
        self.drop_geotable(s_tbl)

        # rename new table
        self.rename_geotable(s_tbl_tmp, s_tbl)

        # refresh table
        self.refresh_table(s_tbl) if refresh else None

        logger.info(f"""adding PK_UID column to "{s_tbl}" was completed.""")

    # drop column
    def drop_column(self, gt_or_tbl, s_col=None):
        if isinstance(gt_or_tbl, GTBL):
            s_tbl = gt_or_tbl.name
            s_col = gt_or_tbl.gc if s_col is None else s_col
        elif isinstance(gt_or_tbl, (str,)):
            s_tbl = gt_or_tbl
        else:
            logger.warning(("given parameter {} is invalid.".format(gt_or_tbl)))
            return 0

        s_tbl_temp = self.get_temptblname()
        self.clone_table(s_tbl, s_tbl_temp, l_options=["::ignore::{}".format(s_col)])
        self.drop_geotable(s_tbl)
        self.rename_geotable(s_tbl_temp, s_tbl)

    # drop columns
    def drop_columns(self, gt_or_tbl, l_col):
        if isinstance(gt_or_tbl, GTBL):
            s_tbl = gt_or_tbl.name
        elif isinstance(gt_or_tbl, (str,)):
            s_tbl = gt_or_tbl
        else:
            logger.warning(("given parameter {} is invalid.".format(gt_or_tbl)))
            return 0

        s_tbl_temp = self.get_temptblname()
        self.clone_table(
            s_tbl,
            s_tbl_temp,
            l_options=["::ignore::{}".format(s_col) for s_col in l_col],
        )
        self.drop_geotable(s_tbl)
        self.rename_geotable(s_tbl_temp, s_tbl)

    # add geometry column
    def add_geomcol(
        self,
        gt_or_tbl,
        i_epsg=None,
        s_geomtype=None,
        s_gc=None,
        s_dimension="XY",
        i_notnull=0,
        add_spidx=False,
    ):
        gt = self.ch_and_get_gtbl(gt_or_tbl, s_gc, i_epsg, s_geomtype)
        sql = """select AddGeometryColumn('{}','{}',{},'{}','{}',{});"""
        self.execute(
            sql.format(gt.name, gt.gc, gt.epsg, gt.type, s_dimension, i_notnull)
        )

        if add_spidx:
            self.add_spindex(gt)

        return gt

    # recover geometry column
    def recov_geomcol(
        self, gt_or_tbl, i_epsg=None, s_geomtype=None, s_gc=None, s_dimension="XY"
    ):
        gt = self.ch_and_get_gtbl(gt_or_tbl, s_gc, i_epsg, s_geomtype)
        sql = f"""select RecoverGeometryColumn('{gt.name}','{gt.gc}',{gt.epsg},'{gt.type}','{s_dimension}');"""
        self.execute(sql)

        return gt

    # discard geometry column
    def discard_geomcol(self, gt_or_tbl, s_gc=None):
        s_tbl, s_gc = self.get_tblname_and_gc(gt_or_tbl, s_gc)
        sql = """select DiscardGeometryColumn('{}','{}');"""
        self.execute(sql.format(s_tbl, s_gc))

    # update column
    def update_column(self, s_tbl, s_col, col_val, flg_sqlvalue=False):
        if type(col_val) in (str, str) and flg_sqlvalue is False:  # 文字列値の場合
            sql = """update "{}" set "{}" = '{}';""".format(s_tbl, s_col, col_val)
        else:  # それ以外（値がSQLの場合も含む）
            sql = """update "{}" set "{}" = {};""".format(s_tbl, s_col, col_val)
        self.execute(sql)

    # clone table
    def clone_table(
        self,
        s_tbl_src,
        s_tbl_dst=None,
        s_db_src="main",
        i_transaction=1,
        l_options=None,
        overwrite=False,
    ):
        if s_tbl_dst is None:
            s_tbl_dst = s_tbl_src

        if self.ch_exists_table(s_tbl_dst):
            if overwrite:
                self.drop_geotable(s_tbl_dst)
            else:
                logger.warning(("{}--- Warning ---".format(os.linesep)))
                logger.warning(
                    (
                        'table "{}" is already exists and overwrite flg is not assigned to True...'.format(
                            s_tbl_dst
                        )
                    )
                )
                logger.warning(
                    'Cloning table process to "%s" is not applied.', s_tbl_dst
                )
                return 0

        if l_options is None:
            sql = """SELECT CloneTable('{}', '{}', '{}', {});""".format(
                s_db_src, s_tbl_src, s_tbl_dst, i_transaction
            )
        else:
            sql = """SELECT CloneTable('{}', '{}', '{}', {}, {});""".format(
                s_db_src,
                s_tbl_src,
                s_tbl_dst,
                i_transaction,
                ", ".join(["'{}'".format(s_opt) for s_opt in l_options]),
            )

        self.execute(sql)

    # create cloned table
    def create_cloned_table(
        self,
        s_tbl_src,
        s_tbl_dst=None,
        s_db_src="main",
        i_transaction=1,
        l_options=None,
        overwrite=False,
    ):
        if s_tbl_dst is None:
            s_tbl_dst = s_tbl_src

        if self.ch_exists_table(s_tbl_dst):
            if overwrite:
                self.drop_geotable(s_tbl_dst)
            else:
                logger.warning(("{}--- Warning ---".format(os.linesep)))
                logger.warning(
                    (
                        'table "{}" is already exists and overwrite flg is not assigned to True...'.format(
                            s_tbl_dst
                        )
                    )
                )
                logger.warning(
                    'Cloning table process to "%s" is not applied.', s_tbl_dst
                )
                return 0

        if l_options is None:
            sql = f"""SELECT CreateClonedTable('{s_db_src}', '{s_tbl_src}', '{s_tbl_dst}', {i_transaction});"""
        else:
            sql = f"""
                SELECT CreateClonedTable(
                    '{s_db_src}', '{s_tbl_src}', '{s_tbl_dst}', {i_transaction},
                    {", ".join([f"'{s_opt}'" for s_opt in l_options])}
                );
            """
        self.execute(sql)

    # create geometry view
    def create_geometry_view(
        self,
        s_gv_name: str,
        s_slct_stmt: str,
        gt_ref: GTBL,
        pk_uid: str = None,
        overwrite: bool = True,
    ):
        """ビューを作り、views_geometry_columns に登録する（QGIS 等から空間ビューとして見える）。

        s_slct_stmt: ビューの SELECT 文。ジオメトリ列は gt_ref.gc と同じ名前で出力すること
        gt_ref: ジオメトリの元になる表（GTBL）
        pk_uid: ビューの行を識別する列（一意な整数）。None なら gt_ref の主キー
        overwrite: True なら同名のビューとその登録を消してから作り直す。同名の「表」は
            消さない（その場合は CREATE VIEW が失敗する）

        views_geometry_columns の各列は小文字で登録する（SpatiaLite のトリガが小文字を要求する）。
        """
        pk_uid = self.get_primary_key(gt_ref) if pk_uid is None else pk_uid

        if overwrite:
            # DROP VIEW では登録行が消えず、同じ行の再登録が一意制約違反になるため先に消す
            self.execute(
                """DELETE FROM "views_geometry_columns" WHERE "view_name" = lower(?);""",
                (s_gv_name,),
            )
            self.execute(f"""DROP VIEW IF EXISTS "{s_gv_name}";""")
        self.execute(
            f"""
            CREATE VIEW "{s_gv_name}" AS
            {s_slct_stmt}
        """
        )
        self.execute(
            """
            INSERT INTO "views_geometry_columns" (
              "view_name", "view_geometry", "view_rowid",
              "f_table_name", "f_geometry_column", "read_only"
            ) VALUES (lower(?), lower(?), lower(?), lower(?), lower(?), 1);
            """,
            (s_gv_name, gt_ref.gc, pk_uid, gt_ref.name, gt_ref.gc),
        )

    # refresh geotable
    def refresh_table(self, gt_or_tbl):
        if isinstance(gt_or_tbl, GTBL):
            s_tbl_src = gt_or_tbl.name
        else:
            s_tbl_src = gt_or_tbl

        s_tbl_tmp = self.get_temptblname()
        self.clone_table(s_tbl_src, s_tbl_tmp)
        self.drop_geotable(s_tbl_src)
        self.rename_geotable(s_tbl_tmp, s_tbl_src)

    # cast to multi geometry
    def cast_to_multi(self, gt_or_tbl, s_gc_src=None, s_gc_dst=None):
        """ジオメトリを MULTI 型に変換し、変換後の GTBL を返す。

        s_gc_dst: 変換後のジオメトリ列名。None なら geom_<MULTI の型>_<EPSG>
        """
        s_tbl, s_gc_src = self.get_tblname_and_gc(gt_or_tbl, s_gc_src)
        geom_type, i_epsg = self.get_type_and_epsg(s_tbl, s_gc_src)
        s_abr = self.get_type_abr(geom_type)
        s_abr_multi = s_abr if s_abr.startswith("m") else "m" + s_abr

        # 登録を外してから変換し、MULTI 型として登録し直す
        self.discard_geomcol(GTBL(self, s_tbl, i_epsg, s_abr, s_gc_src))
        self.execute(f"""UPDATE "{s_tbl}" SET "{s_gc_src}" = ST_Multi("{s_gc_src}");""")
        gt_multi = GTBL(self, s_tbl, i_epsg, s_abr_multi, s_gc_src)
        self.recov_geomcol(gt_multi)

        if s_gc_dst is None:
            s_gc_dst = "geom_{}_{}".format(s_abr_multi, i_epsg)
        if s_gc_dst.lower() != s_gc_src.lower():
            return self.rename_geomcol(gt_multi, s_gc_dst=s_gc_dst)
        return gt_multi

    # append src table to dst table
    def append_table(
        self,
        gt_or_tbl_src,
        s_tbl_dst,
        s_gc=None,
        flg_multi=False,
        flg_drop=False,
        s_db="main",
    ):
        """
        テーブルへのデータ挿入（追加）
        appendモードは便利で、テーブルがなければ新設、あれば追加で機能する。
        """

        if isinstance(gt_or_tbl_src, GTBL):
            s_tbl_src = gt_or_tbl_src.name
            s_gc = gt_or_tbl_src.gc
        elif isinstance(gt_or_tbl_src, (str,)):
            s_tbl_src = gt_or_tbl_src
        else:
            logger.warning(("given parameter {} is invalid.".format(gt_or_tbl_src)))
            return 0

        # マルチモードへの統合が有効になっている場合、いったんキャストしたテーブルを生成する
        if flg_multi and s_gc is not None:
            s_tbl_temp_multi = self.get_temptblname("multi")
            sql = """select CloneTable('{}','{}','{}',1,'::cast2multi::{}');""".format(
                s_db, s_tbl_src, s_tbl_temp_multi, s_gc
            )
            self.execute(sql)

            sql = """select CloneTable('{}','{}','{}',1,'::append::');""".format(
                s_db, s_tbl_temp_multi, s_tbl_dst
            )
            self.execute(sql)
            self.drop_geotable(s_tbl_temp_multi)
        else:  # それ以外は（ジオメトリがない場合も含め）通常のCloneで対応
            self.commit()
            sql = """select CloneTable('{}','{}','{}',1,'::append::');""".format(
                s_db, s_tbl_src, s_tbl_dst
            )
            result = self.execute(sql).fetchone()[0]
            logger.info(("Clone execution result is {}".format(result)))

        if flg_drop is True:  # オリジナルのテーブルを削除
            self.drop_geotable(s_tbl_src)

    # rename geotable
    def rename_geotable(self, s_tbl_old, s_tbl_new, l_options=None):
        """
        it may have multiple geometry column, no GTBL object will be returned.
        """

        logger.info(('rename "{}" to "{}"'.format(s_tbl_old, s_tbl_new)))

        # 新しいテーブルを生成
        self.clone_table(s_tbl_old, s_tbl_new, l_options=l_options)

        # オリジナルを削除
        self.drop_geotable(s_tbl_old)

        # メタデータ更新
        self.upgrade_db()

    # get table list (return in utf)
    def get_tbllist(
        self, s_likephrase=None, s_schema=None, flg_include_views=False
    ):  # 名称条件はLikeなので％利用可能
        """表の名前の一覧（名前順）を返す。

        s_likephrase: LIKE の条件（% と _ がワイルドカード）。None なら全件
        s_schema: 見るスキーマ（"main"・ATTACH の別名等）。None なら従来どおり main
        flg_include_views: True ならビューも含める
        """
        s_master = (
            "SQLITE_MASTER" if s_schema is None else f'"{s_schema}".sqlite_master'
        )
        s_types = "('table', 'view')" if flg_include_views else "('table')"
        sql = f"""SELECT "name" FROM {s_master} WHERE "type" IN {s_types}"""
        t_params: tuple = ()
        if s_likephrase is not None:
            sql += """ AND "name" LIKE ?"""
            t_params = (s_likephrase,)
        rows = self.execute(sql + ";", t_params)
        l_tbl = sorted([i["name"] for i in rows])

        return l_tbl

    # テーブルの列情報を得る。戻り値は各カラムの辞書（name, type, notnull, dflt_value, pk）のリスト
    def get_columnsdef(self, gt_or_tbl, flg_except_gc=False, s_schema=None) -> dict:
        """
        This function returns the column information of the table.
        return :dict of columns info [name, type, notnull, default_value, pk]
        """

        s_tbl = self.get_tblname(gt_or_tbl)

        if isinstance(gt_or_tbl, (str,)) and flg_except_gc is True:
            logger.warning("when tblname is given then flg_except_gc must be set False")
            return 0  # 処理終了

        # s_schema が None なら従来どおり（temp → main → ATTACH 先の順に探す）
        s_prefix = "" if s_schema is None else f'"{s_schema}".'
        sql = f"""pragma {s_prefix}table_info("{s_tbl}");"""
        ld_coldef = self.execute(sql).fetchall()

        if flg_except_gc:
            ld_coldef = [
                d_columninfo
                for d_columninfo in ld_coldef
                if d_columninfo["name"].lower() != gt_or_tbl.gc.lower()
            ]

        return ld_coldef

    # get geometry type of target column
    def get_geomtype(
        self, gt_or_tbl, s_gc=None
    ):  # （登録されている）ジオメトリタイプを取得する
        s_tbl, s_gc = self.get_tblname_and_gc(gt_or_tbl, s_gc)
        sql = """pragma table_info("{}");""".format(s_tbl)
        # print(sql)
        rows = self.execute(sql).fetchall()
        s_geomtype = [i["type"] for i in rows if i["name"].lower() == s_gc.lower()][
            0
        ]  # Geom と geom を等価とする。

        return s_geomtype

    # get srid of geometry
    def get_epsg(self, gt_or_tbl, s_gc=None):  # ジオメトリのSRIDを取得する
        s_tbl, s_gc = self.get_tblname_and_gc(gt_or_tbl, s_gc)
        sql = """select distinct Srid("{}") as srid from "{}" where srid is not NULL;""".format(
            s_gc, s_tbl
        )
        i_epsg = self.execute(sql).fetchone()[0]

        return i_epsg

    def ch_epsg_coordtype(self, i_epsg):
        sql = """SELECT "is_geographic" FROM "spatial_ref_sys_all" WHERE "srid" == {};""".format(
            i_epsg
        )
        i = self.execute(sql).fetchone()[0]
        if i == 1:
            return "geographic"  # 地理座標系
        elif i == 0:
            return "projected"  # 投影座標系
        else:
            logger.warning(('Error: given epsg "{}" is invalid.'.format(i_epsg)))
            return 0

    # make valid geometry （return geometry column name）
    def make_valid(self, gt_or_tbl, s_gc=None, flg_addcol=False):
        s_tbl, s_gc = self.get_tblname_and_gc(gt_or_tbl, s_gc)
        if flg_addcol:  # 修正カラムを生成する場合
            s_gc_valid = "{}_valid".format(s_gc)
            i_epsg = self.get_epsg(s_tbl, s_gc)
            s_geomtype = self.get_geomtype(s_tbl, s_gc)
            self.add_geomcol(
                s_tbl, i_epsg, s_geomtype, s_gc_valid, s_dimension="XY", i_notnull=0
            )
            self.discard_geomcol(s_tbl, s_gc)  # 古いジオメトリカラムをDiscard

            # ジオメトリを複写
            sql = """update "{}" set "{}" = "{}";""".format(s_tbl, s_gc_valid, s_gc)
            self.execute(sql)
            s_gc = s_gc_valid  # 追加したカラムをメインカラムとする

        sql = """
            update "{tbl}" set "{gc}" = ST_MakeValid("{gc}") where ST_IsValid("{gc}") != 1;
        """.format(tbl=s_tbl, gc=s_gc)
        self.execute(sql)

        return (
            s_gc  # flg_addcol = True の場合はカラム名が変わるので、戻り値として与える
        )

    # get x,y of each min and max with list
    def get_extent(self, gt_or_tbl_cover, i_epsg_dst=None):
        """
        :param gt_or_tbl_cover:
        :param i_epsg_dst:
        :return: l_nesw
        """

        s_tbl_cover = self.get_tblname(gt_or_tbl_cover)
        if i_epsg_dst is None:
            sql = """
                select ST_MaxY(geom) as y2, ST_MaxX(geom) as x2, ST_MinY(geom) as y1, ST_MinX(geom) as x1
                from (select GetLayerExtent('{tbl}') as geom
                );
            """.format(tbl=s_tbl_cover)
        elif isinstance(i_epsg_dst, int):
            sql = """
                select ST_MaxY(geom) as y2, ST_MaxX(geom) as x2, ST_MinY(geom) as y1, ST_MinX(geom) as x1
                from (select ST_Transform(GetLayerExtent('{tbl}'), {epsg}) as geom
                );
            """.format(tbl=s_tbl_cover, epsg=i_epsg_dst)
        else:
            logger.warning(
                f"{self.get_method_name()}: given parameter is something wrowg...: "
            )
            return

        l_extent_nesw = list(self.execute(sql).fetchone())

        return l_extent_nesw  # NESW

    # transform geotable ジオメトリカラムの名称と内容が合致しなくなる可能性があるので注意。
    def transform_geotable(
        self, gt_or_tbl, i_epsg_dst, s_gc=None, s_dimension="XY", s_gc_dst=None
    ):
        s_tbl, s_gc = self.get_tblname_and_gc(gt_or_tbl, s_gc)

        # ジオメトリカラムを差し替える場合、テーブルを差し替える
        if s_gc_dst is not None and s_gc != s_gc_dst:
            s_temptbl = self.get_temptblname()
            sql = """select CreateClonedTable('main','{}','{}', 1, '::ignore::{}');"""
            self.execute(sql.format(s_tbl, s_temptbl, s_gc))
            self.add_geomcol(s_temptbl, s_gc=s_gc_dst, i_epsg=i_epsg_dst)
            self.execute(
                """insert into "{}" select * from "{}";""".format(s_temptbl, s_tbl)
            )
            self.drop_geotable(s_tbl)
            self.rename_geotable(s_temptbl, s_tbl)
            s_gc = s_gc_dst

        # 投影変換
        s_geomtype = self.get_geomtype(s_tbl, s_gc)
        self.discard_geomcol(s_tbl, s_gc)
        sql = """update "{0}" set "{1}" = ST_Transform("{1}",{2});""".format(
            s_tbl, s_gc, i_epsg_dst
        )
        self.execute(sql.format(s_tbl, s_gc, i_epsg_dst))
        gt = self.recov_geomcol(s_tbl, i_epsg_dst, s_geomtype, s_gc, s_dimension)

        return gt

    # build point geometry
    def build_point(
        self,
        s_tbl,
        s_gc,
        i_epsg,
        s_geomtype,
        r_x,
        r_y,
        r_z=None,
        s_dimension="XY",
        i_notnull=0,
    ):
        # 対象テーブルにジオメトリカラムを追加
        self.add_geomcol(s_tbl, i_epsg, s_geomtype, s_gc, s_dimension, i_notnull)

        # ポイントジオメトリをupdate
        if s_dimension == "XY":
            self.update_column(
                s_tbl, s_gc, "MakePoint({},{},{})".format(r_x, r_y, i_epsg), True
            )
            s_type_abr = "pt"
        elif s_dimension == "XYZ":
            self.update_column(
                s_tbl,
                s_gc,
                "MakePointZ({},{},{},{})".format(r_x, r_y, r_z, i_epsg),
                True,
            )
            s_type_abr = "ptz"
        else:
            logger.warning("given parameter: s_dimension is strange.")
            return 0

        return GTBL(self, s_tbl, i_epsg, s_type_abr, s_gc)

    def get_type_and_epsg(self, s_tbl, s_gc):
        sql = """SELECT DISTINCT GeometryType("{gc}") AS type, SRID("{gc}") AS srid FROM "{tbl}";""".format(
            tbl=s_tbl, gc=s_gc
        )
        # print(sql)
        fetched = self.execute(sql).fetchone()
        geom_type = fetched[0]
        i_epsg = fetched[1]

        return geom_type, i_epsg

    # rename geometry column and return gtbl
    def rename_geomcol(self, gt_or_tbl, s_gc_src=None, s_gc_dst=None):
        s_tbl = self.get_tblname(gt_or_tbl)
        if isinstance(gt_or_tbl, GTBL) and s_gc_src is None:
            s_gc_src = gt_or_tbl.gc
            geom_type = gt_or_tbl.type
            geom_type_abr = gt_or_tbl.type_abr
            i_epsg = gt_or_tbl.epsg
        elif isinstance(gt_or_tbl, (str,)) and s_gc_src is not None:
            geom_type, i_epsg = self.get_type_and_epsg(s_tbl, s_gc_src)
            geom_type_abr = self.get_type_abr(geom_type)
        else:
            logger.warning(
                f"{self.get_method_name()}: given parameter is something wrong"
            )
            logger.warning(("given gt_or_tbl is {}".format(gt_or_tbl)))
            logger.warning(("given s_gc_src is {}".format(s_gc_src)))
            logger.warning(("given s_gc_dst is {}".format(s_gc_dst)))
            return 0  # 処理終了

        if s_gc_dst is None:  # ジオメトリカラム名を与えない場合は自動付与
            s_gc_dst = "geom_{}_{}".format(geom_type_abr, i_epsg)

        # ジオメトリカラムの差し替え
        logger.info(
            (
                'geometry column "{gc_src}" of table "{tbl}" will be renamed to "{gc_dst}".'.format(
                    tbl=s_tbl, gc_src=s_gc_src, gc_dst=s_gc_dst
                )
            )
        )
        s_tbl_temp = self.get_temptblname()
        self.rename_geotable(s_tbl, s_tbl_temp)
        sql = """select CreateClonedTable('main','{tbl_src}','{tbl_dst}',1,'::ignore::{gc}');""".format(
            tbl_src=s_tbl_temp, tbl_dst=s_tbl, gc=s_gc_src
        )
        self.execute(sql)
        self.add_geomcol(s_tbl, i_epsg, geom_type, s_gc_dst)
        self.begin()
        self.execute(f"""INSERT INTO "{s_tbl}" SELECT * FROM "{s_tbl_temp}";""")
        self.commit()
        self.drop_geotable(s_tbl_temp)
        gt_out = GTBL(self, s_tbl, i_epsg, geom_type_abr, s_gc_dst)

        return gt_out

    # divide elementary geometries and return gtbl
    def divide_elem(
        self,
        gt_or_tbl_in,
        s_gc=None,
        s_tbl_dst=None,
        out_pk="PK_UID_elem",
        out_multi_id="PK_UID_multi",
        transaction=1,
        flg_replace=True,
        l_col_ignore=None,
    ):
        s_tbl_src, s_gc = self.get_tblname_and_gc(gt_or_tbl_in, s_gc)

        if s_tbl_dst is None:
            s_tbl_dst = "{}_elem".format(s_tbl_src)

        # 同名のものがあれば削除する
        if flg_replace:
            self.drop_geotable(s_tbl_dst)

        if l_col_ignore is None:
            sql = """
                SELECT ElementaryGeometries('{}', '{}', '{}', '{}', '{}', {});
            """.format(s_tbl_src, s_gc, s_tbl_dst, out_pk, out_multi_id, transaction)

        else:
            sql = """
                SELECT ElementaryGeometries('{}', '{}', '{}', '{}', '{}', {}, {});
            """.format(
                s_tbl_src,
                s_gc,
                s_tbl_dst,
                out_pk,
                out_multi_id,
                transaction,
                ", ".join(["'::ignore::{}'".format(s_col) for s_col in l_col_ignore]),
            )

        # print(sql)
        self.execute(sql)

        # ジオメトリカラム名を変更し、新たなgtblを得る。
        gt_out = self.rename_geomcol(s_tbl_dst, s_gc)

        return gt_out

    # convert dbf to existing sqlite database
    def dbf2spatialite(
        self, path_dbf, s_tbl_dst, s_chcode_src, flg_append=True, flg_overwrite=True
    ):
        import os

        if flg_append:
            # dbfを読み込み -> temp
            s_temptbl = self.get_temptblname("dbftemp")
        else:
            if not self.ch_exists_table(s_tbl_dst):
                s_temptbl = s_tbl_dst
            elif (
                flg_overwrite
            ):  # テーブルが存在する場合、上書きフラグが有効なら先だって削除する
                self.drop_geotable(s_tbl_dst)
                s_temptbl = s_tbl_dst
            else:
                logger.warning(
                    (
                        'the table "{}" cannot be created because it is already exists.'.format(
                            s_tbl_dst
                        )
                    )
                )
                return 0

        # ImportDBFは拡張子ありでよい（ややこしい、、）
        sql = """select ImportDBF('{}','{}','{}');""".format(
            path_dbf, s_temptbl, s_chcode_src
        )
        self.execute(sql)

        # dbf名をデータに追加
        self.add_column(s_temptbl, "dbf_name", "TEXT")
        self.update_column(
            s_temptbl, "dbf_name", os.path.split(os.path.splitext(path_dbf)[0])[1]
        )

        if flg_append:
            # 読み込んだデータを統合 -> s_tbl_dst
            self.append_table(s_temptbl, s_tbl_dst)
            self.drop_geotable(s_temptbl)

    # import shp to spatialite database and return gtbl
    def shp2spatialite(
        self,
        p_shp,
        i_epsg_src,
        s_chcode_src,
        s_tbl_dst=None,
        s_gc_dst=None,
        flg_multi=False,
        flg_append=True,
        flg_overwrite=False,
    ):
        import os

        # shp名
        s_shp_name = os.path.splitext(os.path.split(p_shp)[1])[0]

        # テーブル名が与えられない場合は、shp名を与える
        if s_tbl_dst is None:
            s_tbl_dst = s_shp_name

        if flg_append:
            # shpを読み込み -> temp
            s_tbl_temp = self.get_temptblname()

        else:
            if not self.ch_exists_table(s_tbl_dst):
                s_tbl_temp = s_tbl_dst
            elif (
                flg_overwrite
            ):  # テーブルが存在する場合、上書きフラグが有効なら先だって削除する
                self.drop_geotable(s_tbl_dst)
                s_tbl_temp = s_tbl_dst
            else:
                logger.warning(
                    (
                        'the table "{}" cannot be created because it is already exists.'.format(
                            s_tbl_dst
                        )
                    )
                )
                return 0

        # shpの読み込み
        if s_gc_dst is None:
            sql = """select ImportSHP('{}','{}','{}',{});""".format(
                os.path.splitext(p_shp)[0], s_tbl_temp, s_chcode_src, i_epsg_src
            )
        else:
            sql = """select ImportSHP('{}','{}','{}',{},'{}');""".format(
                os.path.splitext(p_shp)[0],
                s_tbl_temp,
                s_chcode_src,
                i_epsg_src,
                s_gc_dst,
            )
        self.execute(sql)

        # 読み込まれたデータタイプを検出してGTBLを設定
        if s_gc_dst is None:
            geom_type = self.get_type_and_epsg(s_tbl_temp, "Geometry")[0]
        else:
            geom_type = self.get_type_and_epsg(s_tbl_temp, s_gc_dst)[0]
        if geom_type is not None:
            type_abr = self.get_type_abr(geom_type)
            if s_gc_dst is None:
                gt = GTBL(self, s_tbl_temp, i_epsg_src, type_abr, "Geometry")
            else:
                gt = GTBL(self, s_tbl_temp, i_epsg_src, type_abr, s_gc_dst)
        else:
            logger.warning(
                ("imported data may be invalid...: {}".format(os.path.split(p_shp)[1]))
            )
            return 0

        # shp名をデータに追加
        self.add_column(s_tbl_temp, "shp_name", "TEXT")
        self.update_column(s_tbl_temp, "shp_name", s_shp_name)

        # 読み込んだデータを統合 -> s_tbl_dst
        if flg_append:
            logger.info(('append table "{}" to "{}"'.format(gt.name, s_tbl_dst)))

            # 追記先のジオメトリ列名を決める。
            # - 追記先が既にあれば、その列名に合わせる（以前の版で作った表にも追記できる）
            # - 無ければ s_gc_dst、未指定なら geom_<型>_<EPSG>（flg_multi なら MULTI の型）
            gt_dst_exist = (
                self.get_gtbl_from_metadata(s_tbl_dst)
                if self.ch_exists_table(s_tbl_dst)
                else None
            )
            if gt_dst_exist is not None:
                s_gc_target = gt_dst_exist.gc
            elif s_gc_dst is not None:
                s_gc_target = s_gc_dst
            else:
                s_abr = gt.type_abr
                if flg_multi and not s_abr.startswith("m"):
                    s_abr = "m" + s_abr
                s_gc_target = "geom_{}_{}".format(s_abr, gt.epsg)

            # 一時表の側でキャスト・改名してから、名前を揃えて追記する
            if flg_multi:
                gt = self.cast_to_multi(gt, s_gc_dst=s_gc_target)
            elif gt.gc.lower() != s_gc_target.lower():
                gt = self.rename_geomcol(gt, s_gc_dst=s_gc_target)
            s_tbl_temp_final = gt.name
            self.append_table(gt, s_tbl_dst)
            self.drop_geotable(s_tbl_temp_final)

            gt_out = self.get_gtbl_from_metadata(s_tbl_dst)
            if gt_out is not None:
                gt = gt_out
            else:
                gt.name = s_tbl_dst

        elif flg_multi:  # s_geomcol_dstが指定されない場合、gcはジオメトリのタイプに応じて書き換えられる
            gt = self.cast_to_multi(gt, s_gc_dst=s_gc_dst)

        elif s_gc_dst is None:
            gt = self.rename_geomcol(gt)

        return gt

    # import shapefile to spatialite database via ogr library and return gtbl
    def shp2spatialite_ogr(self, p_shp, i_epsg_dst=None, s_tbl_dst=None, s_gc_dst=None):
        from osgeo import ogr

        s_tbl_dst = (
            s_tbl_dst
            if s_tbl_dst is not None
            else os.path.splitext(os.path.split(p_shp)[1])[0]
        )

        # database path
        p_db = self.get_dbpath()

        # ソースShapefileを開きます
        driver = ogr.GetDriverByName("ESRI Shapefile")
        shapefile = driver.Open(str(p_shp))

        # SpatiaLiteデータベースに接続します
        sqlite_driver = ogr.GetDriverByName("SQLite")
        database = sqlite_driver.Open(str(p_db), 1)  # 1 means writeable

        # Shapefileからレイヤを取得し、その名前を取得します
        layer = shapefile.GetLayer()

        # EPSGを設定する
        if i_epsg_dst is None:
            # レイヤからEPSGを取得する
            i_epsg = int(layer.GetSpatialRef().GetAuthorityCode(None))
            if i_epsg is None:
                raise ValueError(
                    "EPSG code is not found from shapefile. Please specify EPSG code in i_epsg_src."
                )
        else:
            i_epsg = i_epsg_dst

        # ジオメトリカラムの名前を設定する
        if s_gc_dst is None:
            # Get geometry type from layer
            s_geom_type = layer.GetGeomType()

            # Convert geometry type to human-readable form
            s_geom_type_name = ogr.GeometryTypeToName(s_geom_type)

            # Get type abr
            s_type_abr = self.get_type_abr(s_geom_type_name)
            s_gc_dst = f"geom_{s_type_abr}_{i_epsg}"

        # Shapefileのレイヤをデータベースにコピーします
        new_layer = database.CopyLayer(
            layer,
            s_tbl_dst,
            ["SPATIALITE=YES", f"GEOMETRY_NAME={s_gc_dst}", "FID=PK_UID"],
        )

        if new_layer is None:
            logger.warning("Error while copying layer")
        else:
            logger.info("Copy layer succeeded")

        # データベースとShapefileを閉じます
        database.Destroy()

        # update metadata
        self.update_layerstats(s_tbl_dst)

        # get gtbl object
        gtbl = self.get_gtbl_from_metadata(s_tbl_dst)

        # If i_epsg_src is not None, reproject geometry
        if i_epsg_dst is not None:
            self.discard_geomcol(gtbl)
            self.begin()
            self.execute(
                f"""
                UPDATE "{s_tbl_dst}"
                SET "{s_gc_dst}" = ST_Transform("{s_gc_dst}", {i_epsg_dst})
                ;
            """
            )
            self.commit()
            gtbl = self.recov_geomcol(s_tbl_dst, i_epsg_dst, gtbl.type_abr, s_gc_dst)

        logger.info("Import process was done.")

        return gtbl

    # zip を一時ディレクトリに展開し、指定拡張子のファイルのパスを順に渡す
    # zip のあるディレクトリには何も書かない（原票の置き場を読み取り専用のまま扱えるように）。
    # 一時ディレクトリは処理の成否にかかわらず削除する。
    @staticmethod
    def _iter_zip_members(p_zip, s_ext, p_dir_tmp=None, l_ext_extract=None):
        import os
        import tempfile
        import zipfile

        l_ext_extract = [s_ext] if l_ext_extract is None else l_ext_extract
        with tempfile.TemporaryDirectory(dir=p_dir_tmp) as p_tmp:
            with zipfile.ZipFile(p_zip) as zf:
                l_members = [
                    s
                    for s in zf.namelist()
                    if os.path.splitext(s)[1].lower() in l_ext_extract
                ]
                zf.extractall(p_tmp, members=l_members)

            # 階層構造になっている場合があるので walk で探す
            l_path = sorted(
                os.path.join(root, s_file)
                for root, _dirs, files in os.walk(p_tmp)
                for s_file in files
                if os.path.splitext(s_file)[1].lower() == s_ext
            )
            for p_member in l_path:
                yield p_member

    @staticmethod
    def _list_zips(p_zipdir_or_zips, s_pattern="*.zip"):
        import glob
        import os

        if isinstance(p_zipdir_or_zips, (list, tuple)):
            return [str(p) for p in p_zipdir_or_zips]
        return sorted(glob.glob(os.path.join(str(p_zipdir_or_zips), s_pattern)))

    # import zipped shp files to spatialite database
    def zipshps2spatialite(
        self,
        p_zipshpdir,
        s_tbl="merge",
        i_epsg_src=4612,
        s_chcode_src="utf-8",
        flg_multi=False,
        p_dir_tmp=None,
    ):
        """
        ディレクトリ内（またはリストで与えた）zip 中のシェープファイルをすべて1つの表に追記する。

        p_zipshpdir: zip のあるディレクトリ、または zip のパスのリスト
        p_dir_tmp: 展開用の一時ディレクトリを作る場所。None ならシステムの一時ディレクトリ。
            2026-11-13 変更: 以前は zip と同じディレクトリに展開していた。
        """
        gt = None
        for path_zip in self._list_zips(p_zipshpdir):
            for p_shp in self._iter_zip_members(
                path_zip, ".shp", p_dir_tmp, [".shp", ".shx", ".dbf", ".prj", ".cpg"]
            ):
                gt = self.shp2spatialite(
                    p_shp, i_epsg_src, s_chcode_src, s_tbl, flg_multi=flg_multi
                )

        return gt

    # import attributes (dbf only) of zipped shp files to a table without geometry
    def zipdbfs2spatialite(
        self,
        p_zipdir_or_zips,
        s_tbl,
        s_chcode_src="CP932",
        p_dir_tmp=None,
        s_pattern="*.zip",
    ):
        """
        zip 中の dbf（シェープの属性）だけを、ジオメトリなしの1つの表に追記する。
        各行には元の dbf 名（拡張子なし）が dbf_name 列に入る。

        p_zipdir_or_zips: zip のあるディレクトリ、または zip のパスのリスト
        s_chcode_src: dbf の文字コード（国土数値情報は CP932）
        p_dir_tmp: 展開用の一時ディレクトリを作る場所。None ならシステムの一時ディレクトリ。
            zip のあるディレクトリには何も書かない。
        戻り値: 取り込んだ dbf の数
        """
        i_n = 0
        for path_zip in self._list_zips(p_zipdir_or_zips, s_pattern):
            for p_dbf in self._iter_zip_members(path_zip, ".dbf", p_dir_tmp):
                self.dbf2spatialite(p_dbf, s_tbl, s_chcode_src, flg_append=True)
                i_n += 1

        return i_n

    # generate stdmesh
    def generate_stdmesh(
        self,
        s_tbl,
        lat_max_or_t_nesw,
        lon_max=None,
        lat_min=None,
        lon_min=None,
        mesh_level=3,
        ellipsoid="wgs84",
    ):
        """範囲内の標準地域メッシュの表（PK_UID, mesh_code INTEGER, geom_pg_<EPSG>）を作り、GTBL を返す。

        範囲: lat_max_or_t_nesw に (北, 東, 南, 西) の tuple、または北端を与えて
            lon_max・lat_min・lon_min を指定する。範囲（境界を含む）と重なるメッシュをすべて作る
            （北端・東端がメッシュ境界にちょうど一致する場合は、その北側・東側のメッシュも含む）。
        mesh_level: 1, 2, 3, "5x", "2x", "half", "quarter", "eighth", "tenth"（1/10 細分）
        ellipsoid: "wgs84"（EPSG:4612）または "bessel"（EPSG:4301）

        注意: "quarter" と "tenth" はどちらも 10 桁のコードになり、コードだけではレベルを
        区別できない（get_stdmeshcode2wkt 等は 10 桁を quarter とみなす）。
        メッシュコードと区画は格子番号から SQL でまとめて作る（1 区画ずつ Python で作らない）。
        """
        from . import standardmesh as mod_stdms

        # check given extent format
        if isinstance(lat_max_or_t_nesw, (list, tuple)):
            lat_max = lat_max_or_t_nesw[0]  # N
            lon_max = lat_max_or_t_nesw[1]  # E
            lat_min = lat_max_or_t_nesw[2]  # S
            lon_min = lat_max_or_t_nesw[3]  # W
        elif (
            isinstance(lat_max_or_t_nesw, (int, float))
            and lat_min is not None
            and lon_max is not None
            and lon_min is not None
        ):
            lat_max = lat_max_or_t_nesw
        else:
            logger.warning("invalid some of coordinate values.")

            return 0

        # do nothing when given mesh_level is invalid
        if mesh_level not in mod_stdms.D_MESH_GRID_DIV:
            logger.warning(("invalid mesh_level: {}".format(mesh_level)))

            return 0

        # メッシュのテーブルを生成
        if ellipsoid == "wgs84":
            i_epsg = 4612
        elif ellipsoid == "bessel":
            i_epsg = 4301
        else:
            logger.warning('given parameter ellipsoid: "%s" is invalid.', ellipsoid)
            return 0

        # 範囲の南西端・北東端を含むメッシュの格子番号（境界上の点は北東側のメッシュに入る）
        i_div = mod_stdms.D_MESH_GRID_DIV[mesh_level]
        i_lat0 = mod_stdms.get_mesh_grid_index(lat_min * 1.5, i_div)
        i_lat1 = mod_stdms.get_mesh_grid_index(lat_max * 1.5, i_div)
        i_lon0 = mod_stdms.get_mesh_grid_index(lon_min, i_div, 100.0)
        i_lon1 = mod_stdms.get_mesh_grid_index(lon_max, i_div, 100.0)

        ll_coldef = [["mesh_code", "INTEGER"]]
        gt_mesh = GTBL(self, s_tbl, i_epsg, "pg")
        self.create_table(gt_mesh, ll_coldef)

        # 格子番号を再帰 CTE で作り、メッシュコードと区画を SQL でまとめて INSERT する
        s_code = mod_stdms.get_meshcode_sql_from_grid(mesh_level, "g.ilat", "g.ilon")
        sql = f"""
            WITH RECURSIVE
              ys(ilat) AS (SELECT ? UNION ALL SELECT ilat + 1 FROM ys WHERE ilat < ?),
              xs(ilon) AS (SELECT ? UNION ALL SELECT ilon + 1 FROM xs WHERE ilon < ?),
              g AS (SELECT ilat, ilon FROM ys, xs)
            INSERT INTO "{s_tbl}" ("mesh_code", "{gt_mesh.gc}")
            SELECT {s_code},
              BuildMbr(
                100.0 + g.ilon * 1.0 / {i_div}, g.ilat / (1.5 * {i_div}),
                100.0 + (g.ilon + 1) * 1.0 / {i_div}, (g.ilat + 1) / (1.5 * {i_div}),
                {i_epsg})
            FROM g ORDER BY g.ilat, g.ilon;
        """
        with self.transaction():
            self.execute(sql, (i_lat0, i_lat1, i_lon0, i_lon1))
        self.add_uindex(s_tbl, "mesh_code")

        return gt_mesh

    # generate gridmesh
    def generate_gridmesh(
        self, gt_src, s_tbl_dst, i_gridsize, edges_only=False, t_origing=(0, 0)
    ):
        # メッシュのgtblを作成する
        if edges_only:
            s_type_abr = "mln"
        else:
            s_type_abr = "mpg"
        i_epsg = self.get_epsg(gt_src)
        gt_dst = GTBL(self, s_tbl_dst, i_epsg, s_type_abr)
        self.create_table(gt_dst)

        # グリッドのジオメトリを挿入する
        sql = """
            insert into "{tbl_grid}"
            select NULL, ST_SquareGrid(ST_Collect("{geom}"), {size}, {edges_only}, MakePoint({x}, {y}))
            from "{tbl_src}"
            ;
        """.format(
            tbl_grid=gt_dst.name,
            tbl_src=gt_src.name,
            geom=gt_src.gc,
            size=i_gridsize,
            edges_only=edges_only,
            x=t_origing[0],
            y=t_origing[1],
        )

        self.execute(sql)

        return gt_dst

    # split selected geometry area and generate area polygon
    def split_mbr(
        self,
        s_tbl_dst,
        s_tbl_src,
        s_gc_src,
        sql_select_geom,
        i_split_x=2,
        i_split_y=2,
        flg_overwrite=True,
    ):
        """
        与えられたselect stmtにもとづいて、対象のエリアを分割するポリゴンを生成する。デフォルトは四等分。
        対象ジオメトリのselect stmtは、s_gc_srcのみ選択する内容である必要がある。
        """

        # get epsg
        i_epsg = self.get_epsg(s_tbl_src, s_gc_src)

        # create geotable
        gt_dst = self.set_gtbl(s_tbl_dst, i_epsg, "pg")
        self.create_table(gt_dst, overwrite=flg_overwrite)

        # create split data
        sql = """
            select ST_MinX(extent), ST_MinY(extent), ST_MaxX(extent), ST_MaxY(extent)
            from (select ST_Collect({gc}) as extent from ({slct_stmt}));
        """.format(gc=s_gc_src, slct_stmt=sql_select_geom)
        # print(sql)

        t_extent = self.execute(sql).fetchone()
        min_x = t_extent[0]
        min_y = t_extent[1]
        max_x = t_extent[2]
        max_y = t_extent[3]
        dx = 1.0 * (max_x - min_x) / i_split_x
        dy = 1.0 * (max_y - min_y) / i_split_y

        self.begin()

        for n_y in range(i_split_y):
            y1 = min_y + dy * n_y
            y2 = y1 + dy
            for n_x in range(i_split_x):
                x1 = min_x + dx * n_x
                x2 = x1 + dx

                # wkt
                wkt = "POLYGON(({0} {1}, {0} {3}, {2} {3}, {2} {1}, {0} {1}))".format(
                    x1, y1, x2, y2
                )
                sql = """insert into "{tbl}" ("{gc}") values (GeomFromtext('{wkt}', {epsg}));""".format(
                    tbl=s_tbl_dst, gc=gt_dst.gc, wkt=wkt, epsg=i_epsg
                )
                # print(sql)
                self.execute(sql)

        self.commit()

        return gt_dst

    # split line to equidistant points
    def split_line_equidistant(self, gt_line, s_tbl_out, len_split_m, i_epsg4len=3857):
        """
        :param gt_line
        :param s_tbl_out
        :param len_split_m: 点の間隔[m]
        :param i_epsg4len: 等間隔点を得る際に距離の基準とするepsg
        :return: 切断後lineのGTBL
        """

        # 切断したデータを入れるテーブルを作成する（マルチジオメトリになるのでジオメトリカラムを変更する）
        self.drop_geotable(s_tbl_out)
        self.create_cloned_table(gt_line.name, s_tbl_out)
        self.discard_geomcol(s_tbl_out, gt_line.gc)
        gt_line_out = self.recov_geomcol(
            s_tbl_out, gt_line.epsg, "MULTILINESTRING", gt_line.gc
        )
        gt_line_out = self.rename_geomcol(
            gt_line_out, s_gc_dst="geom_mln_{}".format(gt_line_out.epsg)
        )

        # GeometryNで利用するため分割数の最大値を取得し、連番テーブルを生成する
        sql = """
            select
                Max(
                    ST_NumGeometries(
                        ST_Line_Interpolate_Equidistant_Points(
                            ST_Transform("{}", {}), {}  -- いったん投影してからメートル単位で等間隔点を得る
                        )
                    )
                )
            from "{}";
        """.format(gt_line.gc, i_epsg4len, len_split_m, gt_line.name)
        i_limit = self.execute(sql).fetchone()[0]
        s_tbl_numlist = self.get_temptblname("tbl_numlist")
        self.create_sequential_data(s_tbl_numlist, i_limit)

        # 線データのジオメトリ数（レコード数）を取得
        i_numlines = self.execute(
            """select count(*) from "{}";""".format(gt_line.name)
        ).fetchone()[0]

        # 切断したデータを入れ込む
        self.execute("""BEGIN;""")

        for i in range(i_numlines):
            ld_columns = self.get_columnsdef(gt_line, flg_except_gc=True)
            sql = ", ".join(
                ['a."{}"'.format(d_columns["name"]) for d_columns in ld_columns]
            )
            sql = """
                insert into "{0}"
                select
                    {1},
                    case
                        when b.blades is not NULL then ST_Multi(ST_Split(a."{2}", b.blades))
                        else ST_Multi(a."{2}")
                    end
                from (select * from "{3}" limit 1 offset {6}) as a, (
                    select ST_Collect(blade) as blades
                    from (
                        select
                            n
                            , MakeLine(
                                ShiftCoords(GeometryN(eqpoints, n), -0.000001, -0.000001),
                                ShiftCoords(GeometryN(eqpoints, n), 0.000001, 0.000001)
                            ) as blade
                        from
                            (
                                select
                                    ST_Transform(
                                        ST_Line_Interpolate_Equidistant_Points(ST_Transform("{2}", {4}), {5}), {8}
                                    ) as eqpoints
                                from "{3}"
                                limit 1 offset {6}
                            )
                            , (select "PK_UID" as n from "{7}")
                        where n <= ST_NumGeometries(eqpoints)
                    )
                ) as b
                ;
            """.format(
                s_tbl_out,
                sql,
                gt_line.gc,
                gt_line.name,
                i_epsg4len,
                len_split_m,
                i,
                s_tbl_numlist,
                gt_line.epsg,
            )
            # if i == 0:  # for debug
            #     print(sql)
            self.execute(sql)

        self.commit()
        self.drop_geotable(s_tbl_numlist)

        return gt_line_out

    # 与えられた座標を投影変換する
    def transform_pt(self, t_xy, i_epsg_src, i_epsg_dst):
        x = t_xy[0]
        y = t_xy[1]
        sql = """
            select
                ST_X(ST_Transform(MakePoint({x}, {y}, {epsg_src}), {epsg_dst})) as x
                , ST_Y(ST_Transform(MakePoint({x}, {y}, {epsg_src}), {epsg_dst})) as y
            ;
        """.format(x=x, y=y, epsg_src=i_epsg_src, epsg_dst=i_epsg_dst)
        l_xy_dst = self.execute(sql).fetchone()
        # print('tranfomed value: x={}, y={}'.format(l_xy_dst['x'], l_xy_dst['y']))

        return l_xy_dst

    # 与えられた範囲座標を投影変換する
    def transform_coverage(self, l_nesw, i_epsg_src, i_epsg_dst):
        n = l_nesw[0]
        e = l_nesw[1]
        s = l_nesw[2]
        w = l_nesw[3]
        sql = """
            select
                ST_MaxY(geom) as y2
                , ST_MaxX(geom) as x2
                , ST_MinY(geom) as y1
                , ST_MinX(geom) as x1
            from (
                select ST_Transform(BuildMbr({}, {}, {}, {}, {}), {}) as geom
            );
        """.format(w, s, e, n, i_epsg_src, i_epsg_dst)
        l_nesw_dst = list(self.execute(sql).fetchone())
        logger.info(
            (
                "tranfomed value: x1={}, x2={}, y1={}, y2={}".format(
                    l_nesw_dst[3], l_nesw_dst[1], l_nesw_dst[2], l_nesw_dst[0]
                )
            )
        )

        return l_nesw_dst

    # get and update Nearest-Neighbour
    def get_nearest(
        self,
        gt_src: GTBL,
        gt_dst: GTBL,
        s_col_id: str,
        s_col_dist: str | None = None,
        s_dst_id_col: str | None = None,
        flg_geodesic: bool | None = None,
        flg_create_index: bool = True,
    ) -> int:
        """gt_src の各行について gt_dst の最近傍を求め、その ID（と距離）を gt_src に書き込む。

        s_col_id: 最近傍の ID を書く gt_src の列（無ければ INTEGER で追加）
        s_col_dist: 距離を書く gt_src の列（None なら書かない。無ければ REAL で追加）
        s_dst_id_col: 書き込む gt_dst の ID の列。None なら主キー、無ければ ROWID
        flg_geodesic: True なら測地線距離（メートル。ST_Distance(a, b, 1)）、False なら
            CRS の単位の平面距離。None なら gt_dst の SRID が経緯度かで自動で決める
        flg_create_index: gt_dst に空間インデックスが無ければ作る（False なら ValueError）

        求め方: 仮想表 KNN2 で候補を 1 件得て、その距離 d を上限に、空間インデックスで
        「d 以内に入りうる範囲」の gt_dst をすべて取り出して正確な距離で最小を選ぶ
        （KNN2 は探索範囲によって真の最近傍を取りこぼすことがあるため）。
        ジオメトリが NULL の行と、gt_dst が空のときは NULL を書く。書き込んだ行数を返す。
        制約: 経緯度で日付変更線（経度 ±180 度）をまたぐ範囲は扱わない（範囲の絞り込みが
        経度の折り返しを考慮しないため）。極の近く（緯度 89.9 度超）は範囲を広めに取る。
        """
        import math

        i_geo = self.execute("SELECT SridIsGeographic(?);", (gt_dst.epsg,)).fetchone()[
            0
        ]
        b_geographic = bool(i_geo)
        if flg_geodesic is None:
            flg_geodesic = b_geographic
        s_dist = (
            'ST_Distance(?, d."{gc}", 1)'
            if flg_geodesic
            else 'ST_Distance(?, d."{gc}")'
        )
        s_dist = s_dist.format(gc=gt_dst.gc)

        if s_dst_id_col is None:
            s_dst_id_col = self.get_primary_key(gt_dst) or "ROWID"

        b_index = self.execute(
            """SELECT spatial_index_enabled FROM geometry_columns
            WHERE lower(f_table_name) = lower(?) AND lower(f_geometry_column) = lower(?);""",
            (gt_dst.name, gt_dst.gc),
        ).fetchone()
        if not (b_index and b_index[0]):
            if not flg_create_index:
                raise ValueError(f'get_nearest: "{gt_dst.name}" has no spatial index.')
            self.add_spindex(gt_dst)

        self.add_column(gt_src.name, s_col_id, "INTEGER")
        if s_col_dist is not None:
            self.add_column(gt_src.name, s_col_dist, "REAL")

        # KNN2 の初期の探索半径（CRS の単位。経緯度なら度）。expand=1 で見つかるまで広げる
        f_radius0 = 0.05 if b_geographic else 5000.0
        sql_knn = """SELECT fid FROM KNN2 WHERE f_table_name = ? AND f_geometry_column = ?
            AND ref_geometry = ? AND radius = ? AND expand = 1 AND max_items = 1;"""
        sql_cand = f"""SELECT d."{s_dst_id_col}", {s_dist} AS dd FROM "{gt_dst.name}" AS d
            WHERE d.ROWID = ?;"""
        sql_exact = f"""SELECT d."{s_dst_id_col}", {s_dist} AS dd FROM "{gt_dst.name}" AS d
            WHERE d.ROWID IN (
                SELECT ROWID FROM SpatialIndex WHERE f_table_name = ?
                AND f_geometry_column = ? AND search_frame = BuildMbr(?, ?, ?, ?)
            )
            ORDER BY dd LIMIT 1;"""

        l_src = self.execute(
            f"""SELECT ROWID, "{gt_src.gc}", MbrMinX("{gt_src.gc}"), MbrMinY("{gt_src.gc}"),
            MbrMaxX("{gt_src.gc}"), MbrMaxY("{gt_src.gc}") FROM "{gt_src.name}";"""
        ).fetchall()
        l_updates: list[tuple] = []
        for i_rowid, b_geom, f_x0, f_y0, f_x1, f_y1 in l_src:
            if b_geom is None:
                l_updates.append((None, None, i_rowid))
                continue
            r_knn = self.execute(
                sql_knn, (gt_dst.name, gt_dst.gc, b_geom, f_radius0)
            ).fetchone()
            if r_knn is None:
                l_updates.append((None, None, i_rowid))
                continue
            r_cand = self.execute(sql_cand, (b_geom, r_knn[0])).fetchone()
            f_d = r_cand[1]

            # 距離 f_d 以内に入りうる範囲（外接矩形）を少し広めに取る
            if flg_geodesic:
                f_dlat = f_d / 110_000.0 * 1.01 + 1e-9
                f_lat_far = min(89.9, max(abs(f_y0), abs(f_y1)) + f_dlat)
                f_dlon = (
                    f_d / (110_000.0 * math.cos(math.radians(f_lat_far))) * 1.01 + 1e-9
                )
            else:
                f_dlat = f_dlon = f_d * 1.000001 + 1e-9
            r_best = self.execute(
                sql_exact,
                (
                    b_geom,
                    gt_dst.name,
                    gt_dst.gc,
                    f_x0 - f_dlon,
                    f_y0 - f_dlat,
                    f_x1 + f_dlon,
                    f_y1 + f_dlat,
                ),
            ).fetchone()
            if r_best is None:  # 候補自身は必ず範囲内のため通常は起きない（念のため）
                r_best = r_cand
            l_updates.append((r_best[0], r_best[1], i_rowid))

        s_set = f'"{s_col_id}" = ?' + (f', "{s_col_dist}" = ?' if s_col_dist else "")
        with self.transaction():
            for i_id, f_dd, i_rowid in l_updates:
                t_params = (i_id, f_dd, i_rowid) if s_col_dist else (i_id, i_rowid)
                self.execute(
                    f"""UPDATE "{gt_src.name}" SET {s_set} WHERE ROWID = ?;""", t_params
                )
        return len(l_updates)

    def get_nearest_neighbour(
        self, gt_src_pt, gt_dst_pt, s_col_update, use_virtual_knn=False
    ):
        """互換用: get_nearest で gt_dst_pt の ROWID を s_col_update に書く。

        2026-11-14 から get_nearest を使う。経緯度の SRID では測地線距離で比べるため、
        以前（度のまま平面距離で比べていた）と結果が変わる行がありうる。以前と同じ結果が
        要る場合は get_nearest(gt_src, gt_dst, s_col, s_dst_id_col="ROWID", flg_geodesic=False)。
        use_virtual_knn は無視する（SpatiaLite 5 には仮想表 KNN が無く、常に失敗していた）。
        """
        if use_virtual_knn:
            logger.warning(
                "get_nearest_neighbour: use_virtual_knn is ignored (uses KNN2 via get_nearest)."
            )
        self.get_nearest(gt_src_pt, gt_dst_pt, s_col_update, s_dst_id_col="ROWID")

    # create routing nodes id
    def routing_addcol_routing_id(
        self,
        gt_lines,
        s_col_from="node_from",
        s_col_to="node_to",
        s_tbl_nodes_gt=None,
        s_tbl_nodes_gv=None,
        s_db_alies=None,
    ):
        """
        始点ノードIDと終点ノードIDをラインのジオテーブルにカラムとして付与する
        :return 追加されたfrom、toのカラム名および、s_tbl_nodes_gtを与えた場合はそのgtblオブジェクトが返る
        """

        self.ch_able_routing()
        logger.info("adding node id column for routing...")

        s_db_alies = f"'{s_db_alies}'" if s_db_alies is not None else "NULL"
        sql = f"""
            SELECT CreateRoutingNodes({s_db_alies}, '{gt_lines.name}', '{gt_lines.gc}', '{s_col_from}', '{s_col_to}');
        """
        try:
            self.execute(sql)
            logger.info(f'successfully added column "{s_col_from}" and "{s_col_to}"')
        except sqlite3.OperationalError as e:
            logger.warning(e)
            logger.warning(
                f"Column {s_col_from} and {s_col_to} are already exists for routing."
            )

        if s_tbl_nodes_gt is None:
            return s_col_from, s_col_to

        # create geotable/spatialview of nodes with node id

        logger.info("creating geotable of nodes.")
        gt_nodes = self.set_gtbl(s_tbl_nodes_gt, gt_lines.epsg, "pt")
        self.create_table(gt_nodes)
        if s_tbl_nodes_gv is None:
            sql = f"""
                INSERT INTO "{s_tbl_nodes_gt}"
                SELECT * FROM (
                  SELECT
                    "{s_col_from}" AS "id",
                    ST_StartPoint("{gt_lines.gc}")
                  FROM "{gt_lines.name}"
                  UNION
                  SELECT
                    "{s_col_to}",
                    ST_EndPoint("{gt_lines.gc}")
                  FROM "{gt_lines.name}"
                )
                ORDER BY "id"
                ;
            """
            self.execute(sql)

        # if creates spatial view, no geometries will be inserted to geotable. (but knn too slow...
        else:
            sql = f"""
                DROP VIEW IF EXISTS "{s_tbl_nodes_gv}";
                CREATE VIEW "{s_tbl_nodes_gv}" AS
                SELECT * FROM (
                    SELECT
                        "{s_col_from}" AS "id",
                        ST_StartPoint("{gt_lines.gc}") AS "{gt_nodes.gc}"
                    FROM "{gt_lines.name}"
                    UNION
                    SELECT
                        "{s_col_to}",
                        ST_EndPoint("{gt_lines.gc}")
                    FROM "{gt_lines.name}"
                )
                ORDER BY "id"
                ;
                INSERT INTO "views_geometry_columns"
                VALUES ('{s_tbl_nodes_gv}', '{gt_nodes.gc}', 'id', '{gt_nodes.name}', '{gt_nodes.gc}', 1)
                ;
            """

            self.executescript(sql)

        self.add_spindex(gt_nodes)  # for using KNN, spatial index is necessary.

        return s_col_from, s_col_to, gt_nodes

    # create routing data
    def routing_create_routing_data(
        self,
        gt_lines,
        s_col_from,
        s_col_to,
        s_col_cost=None,
        s_col_name=None,
        i_argorithm=1,
        i_bidirectional=0,
        s_col_fwd=None,
        s_col_inv=None,
        i_overwrite=1,
        s_tbl_rtg_data="routing_net_data",
        s_vtbl_rtg="routing_net",
    ):
        self.ch_able_routing()

        logger.info("creating routing data.")

        s_col_cost = "'{}'".format(s_col_cost) if s_col_cost is not None else "NULL"
        s_col_name = "'{}'".format(s_col_name) if s_col_name is not None else "NULL"
        s_col_fwd = "'{}'".format(s_col_fwd) if s_col_fwd is not None else "NULL"
        s_col_inv = "'{}'".format(s_col_inv) if s_col_inv is not None else "NULL"
        sql = """
            SELECT CreateRouting(
                '{tbl_rtg_data}', '{vtbl_rtg}', '{tbl_lines}', '{col_from}', '{col_to}', '{gc_lines}'
                , {col_cost}  -- cost column when NULL then length
                , {col_name}  -- road name or NULL
                , {i_argorithm}  -- enable A-star (boolean)
                , {i_bidirectional}  -- directed graph or not (boolean)
                , {col_flg_fwd}  -- flag to forward
                , {col_flg_inv}  -- flag to inverse
                , {i_flg_overwrite}  -- boolean
            )
            ;
        """.format(
            tbl_rtg_data=s_tbl_rtg_data,
            vtbl_rtg=s_vtbl_rtg,
            tbl_lines=gt_lines.name,
            col_from=s_col_from,
            col_to=s_col_to,
            gc_lines=gt_lines.gc,
            col_cost=s_col_cost,
            col_name=s_col_name,
            i_argorithm=i_argorithm,
            i_bidirectional=i_bidirectional,
            col_flg_fwd=s_col_fwd,
            col_flg_inv=s_col_inv,
            i_flg_overwrite=i_overwrite,
        )

        if self.execute(sql).fetchone()[0] == 1:
            logger.info("routing data was successfully created.")
        else:
            s_msg = self.execute("""SELECT CreateRouting_GetLastError();""").fetchone()[
                0
            ]
            logger.warning(('create routing exit with exception: "{}"'.format(s_msg)))

    # extract nodes point data from routing network
    def extract_routing_nodes(
        self,
        gt_lines: "GTBL",
        s_col_from: str,
        s_col_to: str,
        s_gt_road_nodes: str = None,
    ) -> "GTBL":
        s_gt_road_nodes = (
            f"{gt_lines.name}_nodes" if s_gt_road_nodes is None else s_gt_road_nodes
        )
        gt_road_nodes = self.set_gtbl(s_gt_road_nodes, gt_lines.epsg, "pt")
        self.create_table(gt_road_nodes)
        self.begin()
        self.execute(
            f"""
            INSERT INTO "{gt_road_nodes.name}"
            SELECT * FROM (
                SELECT
                    "{s_col_from}" AS "id",
                    ST_StartPoint("{gt_lines.gc}")
                FROM "{gt_lines.name}"
                UNION
                SELECT
                    "{s_col_to}" AS "id",
                    ST_EndPoint("{gt_lines.gc}")
                FROM "{gt_lines.name}"
            )
            ORDER BY id
        ;"""
        )
        self.commit()
        self.add_spindex(gt_road_nodes)

        return gt_road_nodes

    # ある点からのネットワーク上の近接点IDと、その点までの距離を得る。戻り値はid、dist_mの辞書
    def routing_get_nearest_netpoint(
        self, gt_node, x, y, i_epsg=4612, s_uid_nodes="PK_UID"
    ):
        # ch coord type
        s_coordtype = self.ch_epsg_coordtype(i_epsg)
        s_coordtype_nodes = self.ch_epsg_coordtype(gt_node.epsg)
        if i_epsg == gt_node.epsg:
            # get the closest point
            sql = """
                SELECT
                  "{uid}" AS "id"
                  , Min(ST_Distance(MakePoint({x}, {y}, {epsg}), "{gc_node}")) AS "dist_m"
                FROM "{tbl_node}"
                ;
            """.format(
                tbl_node=gt_node.name,
                gc_node=gt_node.gc,
                x=x,
                y=y,
                epsg=i_epsg,
                uid=s_uid_nodes,
            )
            d_closest_point = dict(self.execute(sql).fetchone())

            if s_coordtype == "geographic":  # get distance with ellipsoidal
                sql = """
                    SELECT ST_Distance(MakePoint({x}, {y}, {epsg}), "{gc_node}", 1)
                    FROM "{tbl_node}"
                    WHERE "{uid}" == {id}
                    ;
                """.format(
                    tbl_node=gt_node.name,
                    gc_node=gt_node.gc,
                    x=x,
                    y=y,
                    epsg=i_epsg,
                    uid=s_uid_nodes,
                    id=d_closest_point["id"],
                )
                d_closest_point["dist_m"] = self.execute(sql).fetchone()[0]
        else:
            # get the closest point
            sql = """
                SELECT
                  "{uid}" AS "id"
                  , Min(ST_Distance(ST_Transform(MakePoint({x}, {y}, {epsg}), {epsg_nodes}), "{gc_node}")) AS "dist_m"
                FROM "{tbl_node}"
                ;
            """.format(
                tbl_node=gt_node.name,
                gc_node=gt_node.gc,
                x=x,
                y=y,
                epsg=i_epsg,
                uid=s_uid_nodes,
                epsg_nodes=gt_node.epsg,
            )
            d_closest_point = dict(self.execute(sql).fetchone())
            if s_coordtype_nodes == "geographic":  # get distance with ellipsoidal
                sql = """
                    SELECT ST_Distance(ST_Transform(MakePoint({x}, {y}, {epsg}), {epsg_nodes}), "{gc_node}", 1)
                    FROM "{tbl_node}" WHERE "{uid}" == {id};
                """.format(
                    tbl_node=gt_node.name,
                    gc_node=gt_node.gc,
                    x=x,
                    y=y,
                    epsg=i_epsg,
                    uid=s_uid_nodes,
                    id=d_closest_point["id"],
                    epsg_nodes=gt_node.epsg,
                )
                d_closest_point["dist_m"] = self.execute(sql).fetchone()[0]
        # print('closest point id:{}, dist_m:{}'.format(d_closest_point['id'], d_closest_point['dist_m']))

        return d_closest_point

    # KNNを利用してある点からのネットワーク上の近接点IDと、その点までの距離を得る。戻り値はid、dist_mの辞書
    def routing_get_nearest_netpoint_knn(self, gt_node, x, y, i_epsg_lonlat=4612):
        """
        KNNを利用して近接点を得る。もしかしてpython-sqlite3からは利用できないのでは…？
        :param gt_node:
        :param x: lon
        :param y: lat
        :param i_epsg_lonlat:
        :return:
        """

        sql = """
            SELECT "fid" AS id, "distance" AS dist_m
            FROM "KNN"
            WHERE "f_table_name" = '{tbl_node}'
            AND "f_geometry_column" = '{gc_node}'
            AND "ref_geometry" = MakePoint({x}, {y}, {epsg})
            AND "max_items" = 1
            ;
        """.format(
            tbl_node=gt_node.name, gc_node=gt_node.gc, x=x, y=y, epsg=i_epsg_lonlat
        )

        d_closest_point = dict(self.execute(sql).fetchone())
        logger.info(
            (
                "closest point id:{}, dist_m:{}".format(
                    d_closest_point["id"], d_closest_point["dist_m"]
                )
            )
        )

        return d_closest_point

    # 登録されているジオメトリカラムの情報をもとに、テーブル名からgtblを得る
    def get_gtbl_from_metadata(self, s_tbl, s_gc=None, s_schema=None):
        """geometry_columns から GTBL を作って返す（見つからない・複数なら None）。

        s_schema: 読む geometry_columns のスキーマ（ATTACH の別名等）。None なら従来どおり
            （修飾なし。通常は main）。返す GTBL の name はスキーマを含まない表名。
        """
        s_meta = (
            '"geometry_columns"'
            if s_schema is None
            else f'"{s_schema}"."geometry_columns"'
        )
        # geometry_columns の表名・列名は小文字で登録されるため、大小を区別せずに照合する
        sql = f"""SELECT * FROM {s_meta} WHERE lower("f_table_name") = lower(?)"""
        t_params: tuple = (s_tbl,)
        if s_gc is not None:
            sql += """ AND lower("f_geometry_column") = lower(?)"""
            t_params += (s_gc,)
        ll_fetch = self.execute(sql + ";", t_params).fetchall()
        d_type = {
            1: "pt",
            2: "ln",
            3: "pg",
            4: "mpt",
            5: "mln",
            6: "mpg",
        }  # M、Z次元には未対応（dimension列見てない）
        if len(ll_fetch) == 1:
            l_fetch = ll_fetch[0]
            gt = GTBL(
                self,
                s_tbl,
                l_fetch["srid"],
                d_type[l_fetch["geometry_type"]],
                l_fetch["f_geometry_column"],
            )
        elif len(ll_fetch) == 0:
            logger.warning(
                "no table and/or geometry column was found. please check them."
            )
            return None
        else:
            logger.warning(
                (
                    'geotable "{}" has multiple geometry column. please identify the geometry column.'.format(
                        s_tbl
                    )
                )
            )
            return None

        return gt

    # GTBLならそれを、違うなら作成してGTBLを返す
    def ch_and_get_gtbl(self, gt_or_tbl, s_gc, i_epsg, s_geomtype):
        if isinstance(gt_or_tbl, GTBL):
            gt = gt_or_tbl
        elif (
            isinstance(gt_or_tbl, (str,))
            and i_epsg is not None
            and s_geomtype is not None
        ):
            if s_geomtype in (
                "pt",
                "ln",
                "pg",
                "mpt",
                "mln",
                "mpg",
                "mptm",
                "mlnm",
                "mpgm",
                "ptzptzm",
                "mptz",
                "mptzm",
            ):
                s_type_abr = s_geomtype
            else:
                s_type_abr = self.get_type_abr(s_geomtype)
            gt = GTBL(self, gt_or_tbl, i_epsg, s_type_abr, s_gc)
        else:
            logger.warning(
                f"{self.get_method_name()}: given parameter is something wrong"
            )
            return 0  # 処理終了

        return gt

    # set gtbl
    def set_gtbl(self, gt_or_tbl, epsg=None, s_type_abr=None, gc=None):
        if isinstance(gt_or_tbl, GTBL):
            s_tbl = gt_or_tbl.name
            epsg = gt_or_tbl.epsg if epsg is None else epsg
            s_type_abr = gt_or_tbl.type_abr if s_type_abr is None else s_type_abr

            # epsg/s_typeどちらか指定 => 通常gcは書き換わるため、gcについてはそれ以外の場合のみ指定。
            if epsg is None and s_type_abr is None:
                gc = gt_or_tbl.gc if gc is None else gc

        else:
            if epsg is None:
                logger.warning(
                    'if you give s_tbl instead of gtbl object, parameter "epsg" must not be None.'
                )
                return 0

            if s_type_abr is None:
                logger.warning(
                    'if you give s_tbl instead of gtbl object, parameter "s_type_abr" must not be None.'
                )
                return 0

            s_tbl = gt_or_tbl

        if s_type_abr in (
            "pt",
            "ln",
            "pg",
            "mpt",
            "mln",
            "mpg",
            "mptm",
            "mlnm",
            "mpgm",
            "ptzptzm",
            "mptz",
            "mptzm",
        ):
            s_type_abr = s_type_abr
        else:
            s_type_abr = self.get_type_abr(s_type_abr)

        gt = GTBL(self, s_tbl, epsg, s_type_abr, gc)

        return gt

    # レイヤ名を返す
    @staticmethod
    def get_tblname(gt_or_tbl):
        if isinstance(gt_or_tbl, GTBL):
            s_tbl = gt_or_tbl.name
        elif isinstance(gt_or_tbl, (str,)):
            s_tbl = gt_or_tbl
        else:
            logger.warning("get_tblname: given parameter is something wrong")
            return 0  # 処理終了

        return s_tbl

    # レイヤ名とジオメトリカラム名を返す
    @staticmethod
    def get_tblname_and_gc(gt_or_tbl, s_gc=None):
        if isinstance(gt_or_tbl, GTBL) and s_gc is None:
            s_tbl = gt_or_tbl.name
            s_gc = gt_or_tbl.gc
        elif isinstance(gt_or_tbl, (str,)) and s_gc is not None:
            s_tbl = gt_or_tbl
        else:
            logger.warning("get_tblname_and_gc: given parameter is something wrong")
            logger.warning(("given gt_or_tbl is {}".format(gt_or_tbl)))
            logger.warning(("given s_gc is {}".format(s_gc)))
            return 0  # 処理終了

        return s_tbl, s_gc

    # ジオメトリタイプから略形表記を取得する
    @staticmethod
    def get_type_abr(s_geomtype):
        type_dict = {
            "pt": "POINT",
            "ln": "LINESTRING",
            "pg": "POLYGON",
            "mpt": "MULTIPOINT",
            "mln": "MULTILINESTRING",
            "mpg": "MULTIPOLYGON",
            "mptm": "MULTIPOINT M",
            "mlnm": "MULTILINESTRING M",
            "mpgm": "MULTIPOLYGON M",
            "ptz": "POINT Z",
            "ptzm": "POINT ZM",
            "mptz": "MULTIPOINT Z",
            "mptzm": "MULTIPOINT ZM",
        }

        # typeからtype_abrを呼ぶために辞書を反転する。辞書を反転させるときにスペースを削除し、すべて小文字に変換
        type_dict_inv = {
            v.replace(" ", "").lower(): k for k, v in list(type_dict.items())
        }

        # Remove spaces and convert to lower case for matching
        s_geomtype_lower = s_geomtype.replace(" ", "").lower()

        # Try to match geometry type directly
        s_type_abr = type_dict_inv.get(s_geomtype_lower)

        # If no match found, raise an error
        if s_type_abr is None:
            raise ValueError(f"No abbreviation found for geometry type {s_geomtype}")

        return s_type_abr

    # 適当な一時テーブル名を作成
    @staticmethod
    def get_temptblname(s_prefix="geotemp"):
        # 適当な一時テーブル名を作成 http://qiita.com/FGtatsuro/items/92bca9ed665449ab047
        import string
        import random

        s_temptbl = "".join([random.choice(string.ascii_letters) for i in range(10)])
        s_temptbl = "{}_{}".format(s_prefix, s_temptbl)

        return s_temptbl

    # SQLiteで定義できるカラムタイプに変換する
    @staticmethod
    def get_dtype(dtype):
        if isinstance(dtype, str):
            return dtype
        elif dtype is bytes or dtype is str:
            return "TEXT"
        elif dtype is int:
            return "INTEGER"
        elif dtype is float:
            return "REAL"
        elif dtype is bytes:
            return "BLOB"
        else:
            logger.warning(('given type "{}" might be strange.'.format(dtype)))
            return dtype

    # get primary key column name
    def get_primary_key(
        self, gt_or_tbl: GTBL | str, s_schema: str | None = None
    ) -> str | None:
        """
        get name of primary key column
        :param gt_or_tbl: table name or GTBL object
        :return: name of primary key column
        """

        s_tbl = gt_or_tbl.name if isinstance(gt_or_tbl, GTBL) else gt_or_tbl
        s_prefix = "" if s_schema is None else f'"{s_schema}".'
        rows = self.execute(f"""PRAGMA {s_prefix}table_info("{s_tbl}")""").fetchall()
        for row in rows:
            if row[5] == 1:
                return row[1]
        logger.warning(f'the table "{s_tbl}" has no primary key column. return None.')
        return None

    # 呼び出し元のメソッド名を返す（メッセージ表示用）
    @staticmethod
    def get_method_name():
        return inspect.currentframe().f_back.f_code.co_name


class GTBL(object):
    def __init__(
        self, con: SpatiaLiteConnection, name: str, epsg: int, type_abr: str, gc=None
    ):
        self.con = con
        self.name = name
        self.epsg = epsg
        self.type_abr = type_abr

        if gc is None:
            gc = f"geom_{type_abr}_{epsg}"

        self.gc = gc

        type_dict = {
            "pt": "POINT",
            "ln": "LINESTRING",
            "pg": "POLYGON",
            "mpt": "MULTIPOINT",
            "mln": "MULTILINESTRING",
            "mpg": "MULTIPOLYGON",
            "mptm": "MULTIPOINT M",
            "mlnm": "MULTILINESTRING M",
            "mpgm": "MULTIPOLYGON M",
            "ptz": "POINT Z",
            "ptzm": "POINT ZM",
            "mptz": "MULTIPOINT Z",
            "mptzm": "MULTIPOINT ZM",
        }

        self.type = type_dict[type_abr]

        # Get Primary Key column of geo table
        l_row = self.con.execute(f"""PRAGMA table_info("{self.name}");""").fetchall()
        if len([r["pk"] for r in l_row if r["pk"] == 1]) == 1:
            s_pk = [r["name"] for r in l_row if r["pk"] == 1][0]
        elif len([r["pk"] for r in l_row if r["pk"] == 1]) > 1:
            logger.warning(f"""Multiple Primary Key columns in "{self.name}".""")
            s_pk = None
        else:
            s_pk = None
        self.pk = s_pk

    @property
    def is_geographic(self):
        i_result = self.con.execute(
            f"""SELECT SridIsGeographic({self.epsg});"""
        ).fetchone()[0]

        return bool(i_result)

    @property
    def is_projected(self):
        i_result = self.con.execute(
            f"""SELECT SridIsProjected({self.epsg});"""
        ).fetchone()[0]

        return bool(i_result)
