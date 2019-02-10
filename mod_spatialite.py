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
import os
import types
import sqlite3


class SpatiaLiteConnection(sqlite3.Connection):
    """
    ref: https://stackoverflow.com/questions/23683886
    ref: https://stackoverflow.com/questions/15556181
    ref: https://codereview.stackexchange.com/questions/134535
    fef: https://docs.python.jp/3/library/sqlite3.html#sqlite3.connect
    """

    def __init__(self, *args, **kwargs):
        # If no db file is exist, then create it.
        if args[0] != ':memory:' and not os.path.isfile(args[0]):
            print('the db file {} is not exist and create it.'.format(args[0]))
            open(args[0], 'w').close()

        try:
            if kwargs['acv']:
                if args[0] == ':memory:':
                    print('Connectting with memory db mode. You cannot archive existing data...')
                else:
                    import shutil
                    p_dir, s_db = os.path.split(args[0])
                    shutil.copyfile(
                        args[0],
                        os.path.join(p_dir, u'{}_acv{}'.format(os.path.splitext(s_db)[0], os.path.splitext(s_db)[1]))
                    )
                del kwargs['acv']  # 辞書の要素を削除（そのまま残っているとsqlite3.ConnectionでInvalidになる。）
        except KeyError:
            pass

        sqlite3.Connection.__init__(self, *args, **kwargs)  # 継承
        s_ver = self.initialize()

        print('Successfully connected to SQLite DB with mod_spatialite.')
        print('DB-path: {}'.format(*args))
        print('Spatialite version: {}'.format(s_ver))

    # mod_spatialiteの適用と初期化
    def initialize(self, p_mod_spatialite=None, row_factory=sqlite3.Row):
        import os
        # 辞書アクセスを可能にする http://docs.python.jp/2/library/sqlite3.html#row
        # sqlite3.Row型でカーソルを利用できるようになる。行要素に対して列名による辞書アクセスが可能。
        # .keys()で列名のリストも取得可能。
        self.row_factory = row_factory  # 辞書アクセスしないならNoneを指定

        # 自動コミットモードを有効にする
        # http://docs.python.jp/2/library/sqlite3.html
        self.isolation_level = None

        # PATHの追加
        if p_mod_spatialite is not None:
            os.environ['PATH'] = '{};{}'.format(os.environ['PATH'], p_mod_spatialite)

        # mod_spatialiteディレクトリのPATH追加
        if p_mod_spatialite is not None:
            os.environ['PATH'] = '{};{}'.format(os.environ['PATH'], os.path.join(p_mod_spatialite, 'mod_spatialite'))
        else:
            os.environ['PATH'] = '{};{}'.format(os.environ['PATH'], 'mod_spatialite')

        # mod_spatialiteの読み込み
        self.isolation_level = ''
        self.enable_load_extension(True)
        self.execute("SELECT load_extension('mod_spatialite');")

        # メタデータを登録してSpatiaLiteDBとする。
        self.execute("""SELECT InitSpatialMetaData(1);""")  # 1はトランザクション有効（高速）

        # SpatiaLiteのバージョンを返す
        return self.get_spatialite_version()

    def get_spatialite_version(self):
        """
        SpatiaLiteのバージョンを返す
        """

        return self.execute("""SELECT spatialite_version();""").fetchone()[0]

    # 切断
    def disconnect(self, flg_vacuum=True, flg_upgrade=True):
        if flg_vacuum:
            print('vacuuming db...')
            self.vacuum()
        if flg_upgrade:
            print('upgrading db...')
            self.upgrade_db()

        p_db = self.get_dbpath()
        self.close()
        print(u'the database {} was successfully closed.'.format(p_db))

    # begin
    def begin(self):
        self.execute("""begin;""")

    # vacuum
    def vacuum(self):
        self.execute("""vacuum;""")

    # attach
    def attach_db(self, p_db, s_alias='a'):
        import os
        if not os.path.isfile(p_db):
            print('oops... db file: {} is not exist.'.format(p_db))
            return
        sql = u"""attach database '{}' as {};""".format(os.path.abspath(p_db), s_alias)
        print(sql)
        self.execute(sql)
        print(u'attached database: {} into: {}'.format(os.path.split(p_db)[1], os.path.split(self.get_dbpath())[1]))

    # detach
    def detach_db(self, s_alias='a'):
        sql = """detach database {};""".format(s_alias)
        self.execute(sql)

    # dbpathを得る
    def get_dbpath(self, dbname='main'):
        sql = u"""PRAGMA database_list;"""
        ld_fetch = self.execute(sql).fetchall()
        for d_fetch in ld_fetch:
            if d_fetch['name'] == dbname:
                return d_fetch['file']
            else:
                pass

        print('no such table: "{}"'.format(dbname))
        return 0

    # check Routing
    def ch_able_routing(self):
        import sys

        print('{}check routing libraries...'.format(os.linesep))

        # check SpatiaLite version
        sql = u"""SELECT spatialite_version();"""
        try:
            s_ver = self.execute(sql).fetchone()[0]
        except self.OperationalError:
            print('Error: spatialite version was not able to fetch... Did you mount mod_spatialite?')
            sys.exit()
        if int(s_ver[0]) < 5:
            print('mounted spatialite version "{}" is not suitable...'.format(s_ver))
            sys.exit()

        # has VirtualRouting?
        sql = u"""SELECT HasRouting();"""
        try:
            s_has = self.execute(sql).fetchone()[0]
        except self.OperationalError:
            print('Error: spatialite has not VirtualRouting... Please check libraries.')
            sys.exit()
        if int(s_has) != 1:
            print('Error: VirtualRouting is not active... Sequences will be stopped.')
            sys.exit()

        print('check OK. routing available.')

    # check table exists or not
    def ch_exists_table(self, s_tbl):
        result = self.get_tbllist(s_tbl)

        if len(result) == 0:
            return False
        else:
            return True

    # check table exists or not
    def ch_exists_col(self, gt_or_tbl, s_col=None):
        if isinstance(gt_or_tbl, GTBL):
            s_tbl = gt_or_tbl.name
            s_col = gt_or_tbl.gc
        else:
            s_tbl = gt_or_tbl
            if s_col is None:
                print('if gt_or_tbl is not GTBL, s_col is mandatory.')
        result = [d_colsinfo['name'] for d_colsinfo in self.get_columnsdef(s_tbl)]

        if s_col in result:
            return True
        else:
            return False

    # update layer statistics
    def update_layerstats(self, s_tbl=None, s_gc=None):
        if s_gc is None:
            if s_tbl is None:
                self.execute(u"""SELECT UpdateLayerStatistics();""")
            else:
                self.execute(u"""SELECT UpdateLayerStatistics('{}');""".format(s_tbl))
        else:
            self.execute(u"""SELECT UpdateLayerStatistics('{}', '{}');""".format(s_tbl, s_gc))

    # update layer statistics
    def upgrade_geomtriggers(self, flg_transaction=True):
        self.execute(u"""SELECT UpgradeGeometryTriggers({});""".format(flg_transaction * 1))

    # upgrade database (free memory and update metadata)
    def upgrade_db(self, flg_transaction=True):
        self.update_layerstats()
        self.upgrade_geomtriggers(flg_transaction)

    # 連番を持つテーブルを生成する（カラムはPK_UID）をそのまま使用する
    def create_sequential_data(self, s_tbl, limit):
        self.create_table(s_tbl)
        self.begin()
        for n in range(limit):
            self.execute(u"""insert into {} values (NULL);""".format(s_tbl))
        self.commit()

    # 長方形を生成する
    def create_window(self, gt_window, l_nesw):
        n = l_nesw[0]
        e = l_nesw[1]
        s = l_nesw[2]
        w = l_nesw[3]

        self.create_table(gt_window)
        self.execute(u"""
            INSERT INTO "{tbl}" VALUES (NULL, BuildMbr({w}, {s}, {e}, {n}, {epsg}));
        """.format(tbl=gt_window.name, w=w, s=s, e=e, n=n, epsg=gt_window.epsg))

        return gt_window

    # テーブルの生成（gtblを与えるとジオテーブルを生成する）
    def create_table(self, gt_or_tbl, ll_coldef=None, flg_pk=True, overwrite=True, flg_temp=False):
        s_tbl = self.get_tblname(gt_or_tbl)

        if overwrite:
            self.drop_geotable(s_tbl)

        if ll_coldef is None:
            if flg_pk:
                sql = u"""create table "{}" ("PK_UID" INTEGER PRIMARY KEY AUTOINCREMENT);""".format(s_tbl)
                if flg_temp:
                    sql = sql.replace(u'create table', u'create temp table')
                self.execute(sql)
            else:
                print('You must give at least one of ll_coldef and flg_pk.')
                return
        else:
            s_coldef = u", ".join([u""""{}" {}""".format(l_coldef[0], l_coldef[1]) for l_coldef in ll_coldef])
            if flg_pk:
                sql = u"""create table "{}" ("PK_UID" INTEGER PRIMARY KEY AUTOINCREMENT, {});""".format(s_tbl, s_coldef)
            else:
                sql = u"""create table "{}" ({});""".format(s_tbl, s_coldef)
            if flg_temp:
                sql = sql.replace(u'create table', u'create temp table')
            self.execute(sql)

        # ジオテーブルを生成する場合
        if isinstance(gt_or_tbl, GTBL):
            self.add_geomcol(gt_or_tbl)

        return gt_or_tbl

    # テーブルの削除（ジオメトリがないテーブルでもよい）
    def drop_geotable(self, gt_or_tbl, i_transaction=1):
        s_tbl = self.get_tblname(gt_or_tbl)

        sql = u"""select DropGeoTable('{tbl}', {transaction});""".format(tbl=s_tbl, transaction=i_transaction)
        self.execute(sql)
        # 何故かテーブル本体が削除されないので、こちらで削除
        sql = u"""drop table if exists "{}";""".format(s_tbl)
        self.execute(sql)

    # add unique index
    def add_uindex(self, gt_or_tbl, s_col):
        s_tbl = self.get_tblname(gt_or_tbl)

        sql = u"""create unique index "uix_{0}_{1}" on "{0}" ("{1}");""".format(s_tbl, s_col)
        try:
            self.execute(sql)
            print('A unique index was successfully added on "{tbl}"."{col}"'.format(tbl=s_tbl, col=s_col))
        except self.OperationalError:
            print('It seems to already have unique index "{}_{}". Pass process.'.format(s_tbl, s_col))

    # add index
    def add_index(self, gt_or_tbl, s_col):
        s_tbl = self.get_tblname(gt_or_tbl)

        sql = u"""create index "ix_{0}_{1}" on "{0}" ("{1}");""".format(s_tbl, s_col)
        try:
            self.execute(sql)
            print('An index was successfully added on "{tbl}"."{col}"'.format(tbl=s_tbl, col=s_col))
        except self.OperationalError:
            print('It seems to already have index "{}_{}". Process was skipped.'.format(s_tbl, s_col))

    # drop index
    def drop_index(self, gt_or_tbl, s_col):
        s_tbl = self.get_tblname(gt_or_tbl)

        sql = u"""
          drop index if exists "{tbl}_{col}";  -- compatible old module
          -- drop index if exists "ix_{tbl}_{col}";
          -- drop index if exists "uix_{tbl}_{col}";
        """
        self.execute(sql.format(tbl=s_tbl, col=s_col))
        print('The index "{}_{}" was dropped.'.format(s_tbl, s_col))

    # add spatial index
    def add_spindex(self, gt_or_tbl, s_gc=None):
        s_tbl, s_gc = self.get_tblname_and_gc(gt_or_tbl, s_gc)

        sql = u"""select CheckSpatialIndex('{}', '{}');"""
        flg = self.execute(sql.format(s_tbl, s_gc)).fetchone()[0]

        if flg is None:  # if there is no spatial index, create it.
            print('creating spatial index on "{}" of table "{}".'.format(s_gc, s_tbl))
            sql = u"""select CreateSpatialIndex('{}','{}');""".format(s_tbl, s_gc)
            self.execute(sql)
        elif flg == 1:
            print('It seems to be already spatial index. Pass process.')

    # drop spatial index
    def drop_spindex(self, gt_or_tbl, s_gc=None):
        s_tbl, s_gc = self.get_tblname_and_gc(gt_or_tbl, s_gc)

        sql = u"""select DisableSpatialIndex('{}','{}');"""
        self.execute(sql.format(s_tbl, s_gc))

    # add column and return column name
    def add_column(self, s_tbl, s_col, s_datatype, default_value=None):
        if default_value is None:
            self.execute(
                u"""ALTER TABLE "{}" ADD COLUMN "{}" {};""".format(s_tbl, s_col, s_datatype)
            )
        else:
            self.execute(
                u"""ALTER TABLE "{}" ADD COLUMN "{}" {} DEFAULT {};""".format(s_tbl, s_col, s_datatype, default_value)
            )

        return s_col

    # add columns
    def add_columns(self, s_tbl, ll_coldef):
        for l_coldef in ll_coldef:
            if len(l_coldef) == 2:
                self.add_column(s_tbl, l_coldef[0], l_coldef[1])
            else:  # len(l_coldef) == 3 のみ考慮
                self.add_column(s_tbl, l_coldef[0], l_coldef[1], l_coldef[2])

    # drop column
    def drop_column(self, gt_or_tbl, s_col=None):
        if isinstance(gt_or_tbl, GTBL):
            s_tbl = gt_or_tbl.name
            s_col = gt_or_tbl.gc if s_col is None else s_col
        elif isinstance(gt_or_tbl, types.StringTypes):
            s_tbl = gt_or_tbl
        else:
            print('given parameter {} is invalid.'.format(gt_or_tbl))
            return 0
        s_tbl_temp = self.get_temptblname()
        self.clone_table(s_tbl, s_tbl_temp, l_options=[u'::ignore::{}'.format(s_col)])
        self.drop_geotable(s_tbl)
        self.rename_geotable(s_tbl_temp, s_tbl)

    # drop columns
    def drop_columns(self, gt_or_tbl, l_col):
        if isinstance(gt_or_tbl, GTBL):
            s_tbl = gt_or_tbl.name
        elif isinstance(gt_or_tbl, types.StringTypes):
            s_tbl = gt_or_tbl
        else:
            print('given parameter {} is invalid.'.format(gt_or_tbl))
            return 0
        s_tbl_temp = self.get_temptblname()
        self.clone_table(s_tbl, s_tbl_temp, l_options=[u'::ignore::{}'.format(s_col) for s_col in l_col])
        self.drop_geotable(s_tbl)
        self.rename_geotable(s_tbl_temp, s_tbl)

    # add geometry column
    def add_geomcol(self, gt_or_tbl, i_epsg=None, s_geomtype=None, s_gc=None, s_dimension='XY', i_notnull=0):
        gt = self.ch_and_get_gtbl(gt_or_tbl, s_gc, i_epsg, s_geomtype)

        sql = u"""select AddGeometryColumn('{}','{}',{},'{}','{}',{});"""
        self.execute(sql.format(gt.name, gt.gc, gt.epsg, gt.type, s_dimension, i_notnull))

        return gt

    # recover geometry column
    def recov_geomcol(self, gt_or_tbl, i_epsg=None, s_geomtype=None, s_gc=None, s_dimension='XY'):
        gt = self.ch_and_get_gtbl(gt_or_tbl, s_gc, i_epsg, s_geomtype)

        sql = u"""select RecoverGeometryColumn('{}','{}',{},'{}','{}');""".format(
            gt.name, gt.gc, gt.epsg, gt.type, s_dimension
        )
        self.execute(sql)

        return gt

    # discard geometry column
    def discard_geomcol(self, gt_or_tbl, s_gc=None):
        s_tbl, s_gc = self.get_tblname_and_gc(gt_or_tbl, s_gc)

        sql = u"""select DiscardGeometryColumn('{}','{}');"""
        self.execute(sql.format(s_tbl, s_gc))

    # update column
    def update_column(self, s_tbl, s_col, col_val, flg_sqlvalue=False):
        if type(col_val) in (str, unicode) and flg_sqlvalue is False:  # 文字列値の場合
            sql = u"""update "{}" set "{}" = '{}';""".format(s_tbl, s_col, col_val)
        else:  # それ以外（値がSQLの場合も含む）
            sql = u"""update "{}" set "{}" = {};""".format(s_tbl, s_col, col_val)
        self.execute(sql)

    # clone table
    def clone_table(self, s_tbl_src, s_tbl_dst=None, s_db='main', i_transaction=1, l_options=None):  # テーブルの複製
        if s_tbl_dst is None:
            s_tbl_dst = s_tbl_src
        if l_options is None:
            sql = u"""SELECT CloneTable('{}', '{}', '{}', {});""".format(
                s_db, s_tbl_src, s_tbl_dst, i_transaction
            )
        else:
            sql = u"""SELECT CloneTable('{}', '{}', '{}', {}, {});""".format(
                s_db, s_tbl_src, s_tbl_dst, i_transaction, u', '.join([u"'{}'".format(s_opt) for s_opt in l_options])
            )
        # print(sql)
        self.execute(sql)

    # create cloned table
    def create_cloned_table(self, s_tbl_src, s_tbl_dst=None, s_db='main', i_transaction=1, l_options=None):
        if s_tbl_dst is None:
            s_tbl_dst = s_tbl_src
        if l_options is None:
            sql = u"""SELECT CreateClonedTable('{}', '{}', '{}', {});""".format(
                s_db, s_tbl_src, s_tbl_dst, i_transaction
            )
        else:
            sql = u"""SELECT CreateClonedTable('{}', '{}', '{}', {}, {});""".format(
                s_db, s_tbl_src, s_tbl_dst, i_transaction, u', '.join([u"'{}'".format(s_opt) for s_opt in l_options])
            )
        # print(sql)
        self.execute(sql)

    # cast to multi geometry
    def cast_to_multi(self, gt_or_tbl, s_gc_src=None, s_gc_dst=None):
        s_tbl, s_gc_src = self.get_tblname_and_gc(gt_or_tbl, s_gc_src)

        # discard gc
        self.discard_geomcol(s_tbl, s_gc_src)

        # cast
        sql = u"""UPDATE "{tbl}" SET "{gc}" = ST_Multi("{gc}");""".format(tbl=s_tbl, gc=s_gc_src)
        self.execute(sql)

        # recover gc
        self.recov_geomcol(s_tbl, s_gc_src)

        if s_gc_dst is None:
            # 既存の名称がジオメトリに合っているかどうかを確認
            geom_type, epsg = self.get_type_and_epsg(s_tbl, s_gc_src)
            type_abr = self.get_type_abr(geom_type)
            s_gc_dst = 'geom_{}_{}'.format(type_abr, epsg)
            if s_gc_src != s_gc_dst:
                gt_dst = self.rename_geomcol(s_tbl, s_gc_src)
            else:  # 既にキャスト後のジオメトリカラム名に適合しているならばそのままでよい
                gt_dst = GTBL(s_tbl, epsg, type_abr, s_gc_dst)
        else:
            gt_dst = self.rename_geomcol(s_tbl, s_gc_src, s_gc_dst)

        return gt_dst

    # append src table to dst table
    def append_table(self, gt_or_tbl_src, s_tbl_dst, s_gc=None, flg_multi=False, flg_drop=False, s_db='main'):
        """
        テーブルへのデータ挿入（追加）
        appendモードは便利で、テーブルがなければ新設、あれば追加で機能する。
        """

        if isinstance(gt_or_tbl_src, GTBL):
            s_tbl_src = gt_or_tbl_src.name
            s_gc = gt_or_tbl_src.gc
        elif isinstance(gt_or_tbl_src, types.StringTypes):
            s_tbl_src = gt_or_tbl_src
        else:
            print('given parameter {} is invalid.'.format(gt_or_tbl_src))
            return 0

        # マルチモードへの統合が有効になっている場合、いったんキャストしたテーブルを生成する
        if flg_multi and s_gc is not None:
            s_tbl_temp_multi = self.get_temptblname('multi')
            sql = u"""select CloneTable('{}','{}','{}',1,'::cast2multi::{}');""".format(
                s_db, s_tbl_src, s_tbl_temp_multi, s_gc
            )
            self.execute(sql)

            sql = u"""select CloneTable('{}','{}','{}',1,'::append::');""".format(s_db, s_tbl_temp_multi, s_tbl_dst)
            self.execute(sql)
            self.drop_geotable(s_tbl_temp_multi)
        else:  # それ以外は（ジオメトリがない場合も含め）通常のCloneで対応
            self.commit()
            sql = u"""select CloneTable('{}','{}','{}',1,'::append::');""".format(s_db, s_tbl_src, s_tbl_dst)
            result = self.execute(sql).fetchone()[0]
            print('Clone execution result is {}'.format(result))

        if flg_drop is True:  # オリジナルのテーブルを削除
            self.drop_geotable(s_tbl_src)

    # rename geotable
    def rename_geotable(self, s_tbl_old, s_tbl_new, l_options=None):
        """
        it may have multiple geometry column, no GTBL object will be returned.
        """
        print(u'rename "{}" to "{}"'.format(s_tbl_old, s_tbl_new))

        # 新しいテーブルを生成
        self.clone_table(s_tbl_old, s_tbl_new, l_options=l_options)
        # オリジナルを削除
        self.drop_geotable(s_tbl_old)
        # メタデータ更新
        self.upgrade_db()

    # get table list (return in utf)
    def get_tbllist(self, s_likephrase=None):  # 名称条件はLikeなので％利用可能
        if s_likephrase is None:
            sql = u"""select "name" from "SQLITE_MASTER" where "type" is 'table';"""
        else:
            sql = u"""
                select "name" from "SQLITE_MASTER" where "type" is 'table' and "name" like '{}';
            """.format(s_likephrase)
        rows = self.execute(sql)
        l_tbl = [i['name'] for i in rows]

        return l_tbl

    # テーブルの列情報を得る。戻り値は各カラムの辞書（name, type, notnull, dflt_value, pk）のリスト
    def get_columnsdef(self, gt_or_tbl, flg_except_gc=False):
        """
        This function returns the column information of the table.
        Return values are dict of columns info [name, type, notnull, dflt_value, pk]
        """
        s_tbl = self.get_tblname(gt_or_tbl)

        if isinstance(gt_or_tbl, types.StringTypes) and flg_except_gc is True:
            print('when tblname is given then flg_except_gc must be set False')
            return 0  # 処理終了

        sql = u"""pragma table_info("{}");""".format(s_tbl)
        ld_coldef = self.execute(sql).fetchall()
        if flg_except_gc:
            ld_coldef = [
                d_tableinfo for d_tableinfo in ld_coldef
                if d_tableinfo['name'].lower() != gt_or_tbl.gc.lower()
            ]

        return ld_coldef

    # get geometry type of target column
    def get_geomtype(self, gt_or_tbl, s_gc=None):  # （登録されている）ジオメトリタイプを取得する
        s_tbl, s_gc = self.get_tblname_and_gc(gt_or_tbl, s_gc)

        sql = u"""pragma table_info("{}");""".format(s_tbl)
        # print(sql)
        rows = self.execute(sql).fetchall()
        s_geomtype = [i['type'] for i in rows if i['name'].lower() == s_gc.lower()][0]  # Geom と geom を等価とする。

        return s_geomtype

    # get srid of geometry
    def get_epsg(self, gt_or_tbl, s_gc=None):  # ジオメトリのSRIDを取得する
        s_tbl, s_gc = self.get_tblname_and_gc(gt_or_tbl, s_gc)

        sql = u"""select distinct Srid("{}") as srid from "{}" where srid is not NULL;""".format(s_gc, s_tbl)
        i_epsg = self.execute(sql).fetchone()[0]

        return i_epsg

    def ch_epsg_coordtype(self, i_epsg):
        sql = u"""SELECT "is_geographic" FROM "spatial_ref_sys_all" WHERE "srid" == {};""".format(i_epsg)
        i = self.execute(sql).fetchone()[0]
        if i == 1:
            return 'geographic'  # 地理座標系
        elif i == 0:
            return 'projected'  # 投影座標系
        else:
            print(os.linesep)
            print('Error: given epsg "{}" is invalid.'.format(i_epsg))
            return 0

    # make valid geometry （return geometry column name）
    def make_valid(self, gt_or_tbl, s_gc=None, flg_addcol=False):
        s_tbl, s_gc = self.get_tblname_and_gc(gt_or_tbl, s_gc)

        if flg_addcol:  # 修正カラムを生成する場合
            s_gc_valid = '{}_valid'.format(s_gc)
            i_epsg = self.get_epsg(s_tbl, s_gc)
            s_geomtype = self.get_geomtype(s_tbl, s_gc)
            self.add_geomcol(s_tbl, i_epsg, s_geomtype, s_gc_valid, s_dimension='XY', i_notnull=0)
            self.discard_geomcol(s_tbl, s_gc)  # 古いジオメトリカラムをDiscard
            # ジオメトリを複写
            sql = u"""update "{}" set "{}" = "{}";""".format(s_tbl, s_gc_valid, s_gc)
            self.execute(sql)
            s_gc = s_gc_valid  # 追加したカラムをメインカラムとする

        sql = u"""
            update "{tbl}" set "{gc}" = ST_MakeValid("{gc}") where ST_IsValid("{gc}") != 1;
        """.format(tbl=s_tbl, gc=s_gc)
        self.execute(sql)

        return s_gc  # flg_addcol = True の場合はカラム名が変わるので、戻り値として与える

    # get x,y of each min and max with list
    def get_extent(self, gt_or_tbl_cover, i_epsg_dst=None):
        """

        :param gt_or_tbl_cover:
        :param i_epsg_dst:
        :return: l_nesw
        """
        s_tbl_cover = self.get_tblname(gt_or_tbl_cover)
        if i_epsg_dst is None:
            sql = u"""
                select ST_MaxY(geom) as y2, ST_MaxX(geom) as x2, ST_MinY(geom) as y1, ST_MinX(geom) as x1
                from (select GetLayerExtent('{tbl}') as geom
                );
            """.format(tbl=s_tbl_cover)
        elif isinstance(i_epsg_dst, int):
            sql = u"""
                select ST_MaxY(geom) as y2, ST_MaxX(geom) as x2, ST_MinY(geom) as y1, ST_MinX(geom) as x1
                from (select ST_Transform(GetLayerExtent('{tbl}'), {epsg}) as geom
                );
            """.format(tbl=s_tbl_cover, epsg=i_epsg_dst)
        else:
            print('given parameter is something wrowg...: ')
            return

        l_extent_nesw = list(self.execute(sql).fetchone())

        return l_extent_nesw  # NESW

    # transform geotable ジオメトリカラムの名称と内容が合致しなくなる可能性があるので注意。
    def transform_geotable(self, gt_or_tbl, i_epsg_dst, s_gc=None, s_dimension='XY', s_gc_dst=None):
        s_tbl, s_gc = self.get_tblname_and_gc(gt_or_tbl, s_gc)

        # ジオメトリカラムを差し替える場合、テーブルを差し替える
        if s_gc_dst is not None and s_gc != s_gc_dst:
            s_temptbl = self.get_temptblname()
            sql = u"""select CreateClonedTable('main','{}','{}', 1, '::ignore::{}');"""
            self.execute(sql.format(s_tbl, s_temptbl, s_gc))
            self.add_geomcol(s_temptbl, s_gc=s_gc_dst, i_epsg=i_epsg_dst)
            self.execute(u"""insert into "{}" select * from "{}";""".format(s_temptbl, s_tbl))
            self.drop_geotable(s_tbl)
            self.rename_geotable(s_temptbl, s_tbl)
            s_gc = s_gc_dst

        # 投影変換
        s_geomtype = self.get_geomtype(s_tbl, s_gc)
        self.discard_geomcol(s_tbl, s_gc)
        sql = u"""update "{0}" set "{1}" = ST_Transform("{1}",{2});""".format(s_tbl, s_gc, i_epsg_dst)
        self.execute(sql.format(s_tbl, s_gc, i_epsg_dst))
        gt = self.recov_geomcol(s_tbl, i_epsg_dst, s_geomtype, s_gc, s_dimension)

        return gt

    # build point geometry
    def build_point(self, s_tbl, s_gc, i_epsg, s_geomtype, r_x, r_y, r_z=None, s_dimension='XY', i_notnull=0):
        # 対象テーブルにジオメトリカラムを追加
        self.add_geomcol(s_tbl, i_epsg, s_geomtype, s_gc, s_dimension, i_notnull)
        # ポイントジオメトリをupdate
        if s_dimension is 'XY':
            self.update_column(s_tbl, s_gc, 'MakePoint({},{},{})'.format(r_x, r_y, i_epsg), True)
            s_type_abr = 'pt'
        elif s_dimension is 'XYZ':
            self.update_column(s_tbl, s_gc, 'MakePointZ({},{},{},{})'.format(r_x, r_y, r_z, i_epsg), True)
            s_type_abr = 'ptz'
        else:
            print('given parameter: s_dimension is strange.')
            return 0

        return GTBL(s_tbl, i_epsg, s_type_abr, s_gc)

    def get_type_and_epsg(self, s_tbl, s_gc):
        sql = u"""SELECT DISTINCT GeometryType("{gc}") AS type, SRID("{gc}") AS srid FROM "{tbl}";""".format(
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
        elif isinstance(gt_or_tbl, types.StringTypes) and s_gc_src is not None:
            geom_type, i_epsg = self.get_type_and_epsg(s_tbl, s_gc_src)
            geom_type_abr = self.get_type_abr(geom_type)
        else:
            print('given parameter is something wrong')
            print('given gt_or_tbl is {}'.format(gt_or_tbl))
            print('given s_gc_src is {}'.format(s_gc_src))
            print('given s_gc_dst is {}'.format(s_gc_dst))
            return 0  # 処理終了

        if s_gc_dst is None:  # ジオメトリカラム名を与えない場合は自動付与
            s_gc_dst = 'geom_{}_{}'.format(geom_type_abr, i_epsg)

        # ジオメトリカラムの差し替え
        print('geometry column "{gc_src}" of table "{tbl}" will be renamed to "{gc_dst}".'.format(
            tbl=s_tbl, gc_src=s_gc_src, gc_dst=s_gc_dst
        ))
        s_tbl_temp = self.get_temptblname()
        self.rename_geotable(s_tbl, s_tbl_temp)
        sql = u"""select CreateClonedTable('main','{tbl_src}','{tbl_dst}',1,'::ignore::{gc}');""".format(
            tbl_src=s_tbl_temp, tbl_dst=s_tbl, gc=s_gc_src
        )
        self.execute(sql)
        self.add_geomcol(s_tbl, i_epsg, geom_type, s_gc_dst)
        self.execute(u"""insert into "{}" select * from "{}";""".format(s_tbl, s_tbl_temp))
        self.drop_geotable(s_tbl_temp)

        gt_out = GTBL(s_tbl, i_epsg, geom_type_abr, s_gc_dst)

        return gt_out

    # divide elementary geometries and return gtbl
    def divide_elem(self, gt_or_tbl_in, s_gc=None, s_tbl_dst=None, out_pk='PK_UID_elem'
                    , out_multi_id='PK_UID_multi', transaction=1, flg_replace=True, l_col_ignore=None):
        s_tbl_src, s_gc = self.get_tblname_and_gc(gt_or_tbl_in, s_gc)

        if s_tbl_dst is None:
            s_tbl_dst = '{}_elem'.format(s_tbl_src)

        # 同名のものがあれば削除する
        if flg_replace:
            self.drop_geotable(s_tbl_dst)

        if l_col_ignore is None:
            sql = u"""
                SELECT ElementaryGeometries('{}', '{}', '{}', '{}', '{}', {});
            """.format(s_tbl_src, s_gc, s_tbl_dst, out_pk, out_multi_id, transaction)
        else:
            sql = u"""
                SELECT ElementaryGeometries('{}', '{}', '{}', '{}', '{}', {}, {});
            """.format(
                s_tbl_src, s_gc, s_tbl_dst, out_pk, out_multi_id, transaction
                , u', '.join([u"'::ignore::{}'".format(s_col) for s_col in l_col_ignore])
            )
        # print(sql)
        self.execute(sql)

        # ジオメトリカラム名を変更し、新たなgtblを得る。
        gt_out = self.rename_geomcol(s_tbl_dst, s_gc)

        return gt_out

    # convert dbf to existing sqlite database
    def dbf2spatialite(self, path_dbf, s_tbl_dst, s_chcode_src, flg_append=True, flg_overwrite=True):
        import os

        if flg_append:
            # dbfを読み込み -> temp
            s_temptbl = self.get_temptblname('dbftemp')
        else:
            if not self.ch_exists_table(s_tbl_dst):
                s_temptbl = s_tbl_dst
            elif flg_overwrite:  # テーブルが存在する場合、上書きフラグが有効なら先だって削除する
                self.drop_geotable(s_tbl_dst)
                s_temptbl = s_tbl_dst
            else:
                print('the table "{}" cannot be created because it is already exists.'.format(s_tbl_dst))
                return 0

        # ImportDBFは拡張子ありでよい（ややこしい、、）
        sql = u"""select ImportDBF('{}','{}','{}');""".format(path_dbf, s_temptbl, s_chcode_src)
        self.execute(sql)

        # dbf名をデータに追加
        self.add_column(s_temptbl, 'dbf_name', 'TEXT')
        self.update_column(s_temptbl, 'dbf_name', os.path.split(os.path.splitext(path_dbf)[0])[1])

        if flg_append:
            # 読み込んだデータを統合 -> s_tbl_dst
            self.append_table(s_temptbl, s_tbl_dst)
            self.drop_geotable(s_temptbl)

    # import shp to spatialite database and return gtbl
    def shp2spatialite(self, p_shp, i_epsg_src, s_chcode_src, s_tbl_dst=None, s_gc_dst=None, flg_multi=False
                       , flg_append=True, flg_overwrite=False):
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
            elif flg_overwrite:  # テーブルが存在する場合、上書きフラグが有効なら先だって削除する
                self.drop_geotable(s_tbl_dst)
                s_tbl_temp = s_tbl_dst
            else:
                print('the table "{}" cannot be created because it is already exists.'.format(s_tbl_dst))
                return 0

        # shpの読み込み
        if s_gc_dst is None:
            sql = u"""select ImportSHP('{}','{}','{}',{});""".format(
                os.path.splitext(p_shp)[0], s_tbl_temp, s_chcode_src, i_epsg_src
            )
        else:
            sql = u"""select ImportSHP('{}','{}','{}',{},'{}');""".format(
                os.path.splitext(p_shp)[0], s_tbl_temp, s_chcode_src, i_epsg_src, s_gc_dst
            )
        self.execute(sql)

        # 読み込まれたデータタイプを検出してGTBLを設定
        if s_gc_dst is None:
            geom_type = self.get_type_and_epsg(s_tbl_temp, 'Geometry')[0]
        else:
            geom_type = self.get_type_and_epsg(s_tbl_temp, s_gc_dst)[0]
        if geom_type is not None:
            type_abr = self.get_type_abr(geom_type)
            if s_gc_dst is None:
                gt = GTBL(s_tbl_temp, i_epsg_src, type_abr, 'Geometry')
            else:
                gt = GTBL(s_tbl_temp, i_epsg_src, type_abr, s_gc_dst)
        else:
            print('imported data may be invalid...: {}'.format(os.path.split(p_shp)[1]))
            return 0

        # shp名をデータに追加
        self.add_column(s_tbl_temp, 'shp_name', 'TEXT')
        self.update_column(s_tbl_temp, 'shp_name', s_shp_name)

        # 読み込んだデータを統合 -> s_tbl_dst
        if flg_append:
            print(u'append table "{}" to "{}"'.format(gt.name, s_tbl_dst))
            # 読み込んだデータを統合 -> s_tbl_dst
            self.append_table(gt, s_tbl_dst, flg_multi=flg_multi)
            self.drop_geotable(gt)
            gt.name = s_tbl_dst
            # gt = GTBL(s_tbl_dst, gt.epsg, gt.type_abr)
            if flg_multi and s_gc_dst is None:
                gt = self.rename_geomcol(gt)  # todo: 何のためにあるのかイマイチ、、、？　flg_multi必要？
            # gt = gt_new
        elif flg_multi:  # s_geomcol_dstが指定されない場合、gcはジオメトリのタイプに応じて書き換えられる
            gt = self.cast_to_multi(gt, s_gc_dst=s_gc_dst)
        elif s_gc_dst is None:
            gt = self.rename_geomcol(gt)

        return gt

    # import zipped shp files to spatialite database
    def zipshps2spatialite(self, p_zipshpdir, s_tbl='merge', i_epsg_src=4612, s_chcode_src='utf-8', flg_multi=False):
        import os
        import glob
        import shutil

        # path_zipのzipファイルを一時ディレクトリに展開する
        def zip_extract(_p_zip):
            import zipfile
            import tempfile
            import os
            tempdir = tempfile.mkdtemp(dir=os.path.split(_p_zip)[0])  # 解凍先一時ディレクトリを生成
            zipfile.ZipFile(_p_zip).extractall(tempdir)  # 全部展開
            return tempdir  # 一時ディレクトリパスを戻り値とする

        # フォルダ内の結合するshpファイルのリストを作成
        l_path_zip = glob.glob('{}/*.zip'.format(p_zipshpdir))

        for path_zip in l_path_zip:
            # zip展開
            path_shpdir = zip_extract(path_zip)

            # ディレクトリ内のshpのリストを生成（階層構造になっている場合があるので、globでは足りない）
            l_path_shp = []
            for root, dirs, files in os.walk(path_shpdir):
                for _file in files:
                    if os.path.splitext(_file)[1] == '.shp':
                        l_path_shp.append(os.path.join(root, _file))

            # shpの読み込み
            for p_shp in l_path_shp:
                gt = self.shp2spatialite(p_shp, i_epsg_src, s_chcode_src, s_tbl, flg_multi=flg_multi)

            # 一時ディレクトリの削除
            shutil.rmtree(path_shpdir)

        return gt

    # generate stdmesh
    def generate_stdmesh(self, s_tbl, lat_max_or_t_nesw, lon_max=None, lat_min=None, lon_min=None, mesh_level=3
                         , ellipsoid='wgs84'):
        import mod_standardmesh as mod_stdms

        # check given extent format
        if isinstance(lat_max_or_t_nesw, (list, tuple)):
            lat_max = lat_max_or_t_nesw[0]  # N
            lon_max = lat_max_or_t_nesw[1]  # E
            lat_min = lat_max_or_t_nesw[2]  # S
            lon_min = lat_max_or_t_nesw[3]  # W
        elif isinstance(lat_max_or_t_nesw, (int, long, float)) and lat_min is not None and lon_max is not None\
                and lon_min is not None:
            lat_max = lat_max_or_t_nesw
        else:
            print('invalid some of coordinate values.')
            return 0

        # do nothing when given mesh_level is invalid
        if mesh_level not in (1, 2, 3, '5x', '2x', 'half', 'quarter', 'eighth'):
            print('invalid mesh_level: {}'.format(mesh_level))
            return 0

        # obtain unit mesh size
        unit_lon, unit_lat = mod_stdms.get_unitsize(mesh_level)

        # メッシュコード群を生成
        l_meshcode = list()
        lat = lat_min
        while lat < lat_max + unit_lat:
            lon = lon_min
            while lon < lon_max + unit_lon:
                meshcode = mod_stdms.get_mesh_index(lon, lat, mesh_level)
                l_meshcode.append(meshcode)
                lon += unit_lon
            lat += unit_lat

        # メッシュのテーブルを生成
        if ellipsoid == 'wgs84':
            i_epsg = 4612
        elif ellipsoid == 'bessel':
            i_epsg = 4301
        else:
            print('given parameter ellipsoid: "{}" is invalid.')
            return 0
        ll_coldef = [['mesh_code', 'INTEGER']]
        gt_mesh = GTBL(s_tbl, i_epsg, 'pg')
        self.create_table(gt_mesh, ll_coldef)

        # 連続的にメッシュポリゴンをInsert（左下から右上まで）
        self.begin()
        for s_meshcode in l_meshcode:
            sql = u"""insert into "{}" select NULL, '{}', ST_GeomFromText('{}', {});""".format(
                s_tbl, s_meshcode, mod_stdms.get_stdmeshcode2wkt(s_meshcode)[0], i_epsg
            )
            self.execute(sql)
        self.add_uindex(s_tbl, 'mesh_code')
        self.commit()

        return gt_mesh

    # generate gridmesh
    def generate_gridmesh(self, gt_src, s_tbl_dst, i_gridsize, edges_only=False, t_origing=(0, 0)):

        # メッシュのgtblを作成する
        if edges_only:
            s_type_abr = 'mln'
        else:
            s_type_abr = 'mpg'
        i_epsg = self.get_epsg(gt_src)
        gt_dst = GTBL(s_tbl_dst, i_epsg, s_type_abr)
        self.create_table(gt_dst)

        # グリッドのジオメトリを挿入する
        sql = u"""
            insert into "{tbl_grid}"
            select NULL, ST_SquareGrid(ST_Collect("{geom}"), {size}, {edges_only}, MakePoint({x}, {y}))
            from "{tbl_src}"
            ;
        """.format(
            tbl_grid=gt_dst.name, tbl_src=gt_src.name, geom=gt_src.gc, size=i_gridsize
            , edges_only=edges_only, x=t_origing[0], y=t_origing[1]
        )
        self.execute(sql)

        return gt_dst

    # split selected geometry area and generate area polygon
    def split_mbr(self, s_tbl_dst, s_tbl_src, s_gc_src, sql_select_geom, i_split_x=2, i_split_y=2
                  , flg_overwrite=True):
        """
        与えられたselect stmtにもとづいて、対象のエリアを分割するポリゴンを生成する。デフォルトは四等分。
        対象ジオメトリのselect stmtは、s_gc_srcのみ選択する内容である必要がある。
        """
        # get epsg
        i_epsg = self.get_epsg(s_tbl_src, s_gc_src)

        # create geotable
        gt_dst = self.set_gtbl(s_tbl_dst, i_epsg, 'pg')
        self.create_table(gt_dst, overwrite=flg_overwrite)

        # create split data
        sql = u"""
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
                wkt = u'POLYGON(({0} {1}, {0} {3}, {2} {3}, {2} {1}, {0} {1}))'.format(x1, y1, x2, y2)
                sql = u"""insert into "{tbl}" ("{gc}") values (GeomFromtext('{wkt}', {epsg}));""".format(
                    tbl=s_tbl_dst, gc=gt_dst.gc, wkt=wkt, epsg=i_epsg
                )
                # print(sql)
                self.execute(sql)
        self.commit()

        return gt_dst

    # split line to equidistant points
    def split_line_equidistant(self, gt_line, s_tbl_out, len_split_m, i_epsg4len=3857):
        """
        :param len_split_m: 点の間隔[m]
        :param i_epsg4len: 等間隔点を得る際に距離の基準とするepsg
        :return: 切断後lineのGTBL
        """
        # 切断したデータを入れるテーブルを作成する（マルチジオメトリになるのでジオメトリカラムを変更する）
        self.drop_geotable(s_tbl_out)
        self.create_cloned_table(gt_line.name, s_tbl_out)
        self.discard_geomcol(s_tbl_out, gt_line.gc)
        gt_line_out = self.recov_geomcol(s_tbl_out, gt_line.epsg, 'MULTILINESTRING', gt_line.gc)
        gt_line_out = self.rename_geomcol(gt_line_out, s_gc_dst='geom_mln_{}'.format(gt_line_out.epsg))

        # GeometryNで利用するため分割数の最大値を取得し、連番テーブルを生成する
        sql = u"""
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
        s_tbl_numlist = self.get_temptblname('tbl_numlist')
        self.create_sequential_data(s_tbl_numlist, i_limit)

        # 線データのジオメトリ数（レコード数）を取得
        i_numlines = self.execute(u"""select count(*) from "{}";""".format(gt_line.name)).fetchone()[0]

        # 切断したデータを入れ込む
        self.execute("""begin;""")
        for i in range(i_numlines):
            ld_columns = self.get_columnsdef(gt_line, flg_except_gc=True)
            sql = ', '.join(['a."{}"'.format(d_columns['name']) for d_columns in ld_columns])
            sql = u"""
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
                s_tbl_out, sql, gt_line.gc, gt_line.name, i_epsg4len, len_split_m, i, s_tbl_numlist
                , gt_line.epsg
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

        sql = u"""
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

        sql = u"""
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

        print('tranfomed value: x1={}, x2={}, y1={}, y2={}'.format(
            l_nesw_dst[3], l_nesw_dst[1], l_nesw_dst[2], l_nesw_dst[0]
        ))

        return l_nesw_dst

    # get and update Nearest-Neighbour
    def get_nearest_neighbour(self, gt_src_pt, gt_dst_pt, s_col_update, use_virtual_knn=False):
        if use_virtual_knn:
            # KNN使用モード ：KNNはPythonからのループでは落ちる（原因不明）ので、ひとまず利用しない。
            print('get and update nearest neighbor using Virtual KNN')
            rows = self.execute(u"""SELECT "ROWID" FROM "{src}" WHERE "{gc_src}" IS NOT NULL;""".format(
                src=gt_src_pt.name, gc_src=gt_src_pt.gc
            ))
            self.begin()
            for row in rows:  # sqlite3ではwithが使えないのでforで対応
                i_id_src = row[0]
                # print('knn process: {}'.format(i_id_src))
                sql = u"""
                    SELECT "fid" FROM "KNN"
                    WHERE "f_table_name" = '{dst}'
                    AND "ref_geometry" = (SELECT "{gc_src}" FROM "{src}" WHERE "ROWID" == {src_id})
                    AND "max_items" = 1
                    ;
                """.format(src=gt_src_pt.name, gc_src=gt_src_pt.gc, src_id=i_id_src, dst=gt_dst_pt.name)
                id_closest_road = self.execute(sql).fetchone()[0]
                self.execute(u"""
                    UPDATE "{src}" SET "{col_update_id}" = {dst_id} WHERE "ROWID" == {src_id};
                """.format(src=gt_src_pt.name, col_update_id=s_col_update, dst_id=id_closest_road,
                           src_id=i_id_src))
            self.commit()
        else:
            # KNNを使用しない場合、Min(ST_Distance)で対応する
            print('get and update nearest neighbor without using Virtual KNN')
            s_tbl_temp = self.get_temptblname('temp')
            sql = u"""
                DROP TABLE IF EXISTS "{temp}";
                CREATE TABLE "{temp}" ("src_id" INTEGER PRIMARY KEY, "dst_id" INTEGER);
                INSERT INTO "{temp}"
                SELECT srcid, dstid FROM (
                    SELECT
                      src."ROWID" AS srcid
                      , dst."ROWID" AS dstid
                      , Min(ST_Distance(src."{gc_src}", dst."{gc_dst}"))
                    FROM "{src}" AS src, "{dst}" AS dst
                    WHERE src."{gc_src}" IS NOT NULL
                    GROUP BY src."ROWID"
                )
                ;
                UPDATE "{src}"
                SET "{col_update_id}" = (SELECT "dst_id" FROM "{temp}" WHERE "{src}"."ROWID" == "{temp}"."src_id")
                WHERE "{gc_src}" IS NOT NULL
                ;
                DROP TABLE IF EXISTS "{temp}";
            """.format(
                src=gt_src_pt.name, gc_src=gt_src_pt.gc, dst=gt_dst_pt.name, gc_dst=gt_dst_pt.gc,
                temp=s_tbl_temp, col_update_id=s_col_update
            )
            self.executescript(sql)

    # create routing nodes id
    def routing_addcol_routing_id(self, gt_lines, s_col_from='node_from', s_col_to='node_to'
                                  , s_tbl_nodes_gt=None, s_tbl_nodes_gv=None
                                  , s_db_alies=None):
        """
        始点ノードIDと終点ノードIDをラインのジオテーブルにカラムとして付与する
        :return 追加されたfrom、toのカラム名および、s_tbl_nodes_gtを与えた場合はそのgtblオブジェクトが返る
        """
        self.ch_able_routing()

        print('adding node id column for routing...')
        s_db_alies = "'{}'".format(s_db_alies) if s_db_alies is not None else 'NULL'
        sql = u"""
            SELECT CreateRoutingNodes({db_alies}, '{tbl_lines}', '{gc_lines}', '{id_node_from}', '{id_node_to}');
        """.format(
            db_alies=s_db_alies, tbl_lines=gt_lines.name, gc_lines=gt_lines.gc
            , id_node_from=s_col_from, id_node_to=s_col_to
        )
        self.execute(sql)
        print('successfully added column "{}" and "{}"'.format(s_col_from, s_col_to))

        if s_tbl_nodes_gt is None:
            return s_col_from, s_col_to

        # create geotable/spatialview of nodes with node id
        print('creating geotable of nodes.')

        gt_nodes = self.set_gtbl(s_tbl_nodes_gt, gt_lines.epsg, 'pt')
        self.create_table(gt_nodes)
        if s_tbl_nodes_gv is None:
            sql = u"""
                INSERT INTO "{tbl}"
                SELECT * FROM (
                  SELECT
                    "{node_from}" AS "id",
                    ST_StartPoint("{gc_lines}")
                  FROM "{tbl_lines}"
                  UNION
                  SELECT
                    "{node_to}",
                    ST_EndPoint("{gc_lines}")
                  FROM "{tbl_lines}"
                )
                ORDER BY "id"
                ;
            """.format(
                tbl=s_tbl_nodes_gt, tbl_lines=gt_lines.name, gc_lines=gt_lines.gc, node_from=s_col_from, node_to=s_col_to
            )
            self.execute(sql)
        # if creates spatial view, no geometries will be inserted to geotable. (but knn too slow...
        else:
            sql = u"""
                DROP VIEW IF EXISTS "{gv}";
                CREATE VIEW "{gv}" AS
                SELECT * FROM (
                    SELECT
                        "{node_from}" AS "id",
                        ST_StartPoint("{gc_lines}") AS "{gc_nodes}"
                    FROM "{tbl_lines}"
                    UNION
                    SELECT
                        "{node_to}",
                        ST_EndPoint("{gc_lines}")
                    FROM "{tbl_lines}"
                )
                ORDER BY "id"
                ;
                INSERT INTO "views_geometry_columns"
                VALUES ('{gv}', '{gc_nodes}', 'id', '{tbl_nodes}', '{gc_nodes}', 1)
                ;
            """.format(
                gv=s_tbl_nodes_gv, tbl_lines=gt_lines.name, gc_lines=gt_lines.gc, node_from=s_col_from, node_to=s_col_to
                , tbl_nodes=gt_nodes.name, gc_nodes=gt_nodes.gc
            )
            self.executescript(sql)
        self.add_spindex(gt_nodes)  # for using KNN, spatial index is necessary.

        return s_col_from, s_col_to, gt_nodes

    # create routing data
    def routing_create_routing_data(self, gt_lines, s_col_from, s_col_to
                                    , s_col_cost=None, s_col_name=None, i_argorithm=1, i_bidirectional=0
                                    , s_col_fwd=None, s_col_inv=None, i_overwrite=1
                                    , s_tbl_rtg_data='routing_data', s_vtbl_rtg='routing'
                                    ):
        self.ch_able_routing()

        print('creating routing data.')
        s_col_cost = "'{}'".format(s_col_cost) if s_col_cost is not None else 'NULL'
        s_col_name = "'{}'".format(s_col_name) if s_col_name is not None else 'NULL'
        s_col_fwd = "'{}'".format(s_col_fwd) if s_col_fwd is not None else 'NULL'
        s_col_inv = "'{}'".format(s_col_inv) if s_col_inv is not None else 'NULL'
        sql = u"""
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
            tbl_rtg_data=s_tbl_rtg_data, vtbl_rtg=s_vtbl_rtg, tbl_lines=gt_lines.name
            , col_from=s_col_from, col_to=s_col_to, gc_lines=gt_lines.gc
            , col_cost=s_col_cost, col_name=s_col_name, i_argorithm=i_argorithm
            , i_bidirectional=i_bidirectional, col_flg_fwd=s_col_fwd, col_flg_inv=s_col_inv
            , i_flg_overwrite=i_overwrite
        )
        if self.execute(sql).fetchone()[0] == 1:
            print('routing data was successfully created.')
        else:
            s_msg = self.execute(u"""SELECT CreateRouting_GetLastError();""").fetchone()[0]
            print('create routing exit with exception: "{}"'.format(s_msg))

    # ある点からのネットワーク上の近接点IDと、その点までの距離を得る。戻り値はid、dist_mの辞書
    def routing_get_nearest_netpoint(self, gt_node, x, y, i_epsg=4612, s_uid_nodes='PK_UID'):
        # ch coord type
        s_coordtype = self.ch_epsg_coordtype(i_epsg)
        s_coordtype_nodes = self.ch_epsg_coordtype(gt_node.epsg)

        if i_epsg == gt_node.epsg:
            # get closest point
            sql = u"""
                SELECT
                  "{uid}" AS "id"
                  , Min(ST_Distance(MakePoint({x}, {y}, {epsg}), "{gc_node}")) AS "dist_m"
                FROM "{tbl_node}"
                ;
            """.format(tbl_node=gt_node.name, gc_node=gt_node.gc, x=x, y=y, epsg=i_epsg, uid=s_uid_nodes)
            d_closest_point = dict(self.execute(sql).fetchone())

            if s_coordtype == 'geographic':  # get distance with ellipsoidal
                sql = u"""
                    SELECT ST_Distance(MakePoint({x}, {y}, {epsg}), "{gc_node}", 1)
                    FROM "{tbl_node}" WHERE "{uid}" == 
                    {id};
                """.format(
                    tbl_node=gt_node.name, gc_node=gt_node.gc, x=x, y=y, epsg=i_epsg, uid=s_uid_nodes
                    , id=d_closest_point['id']
                )
                d_closest_point['dist_m'] = self.execute(sql).fetchone()[0]

        else:
            # get closest point
            sql = u"""
                SELECT
                  "{uid}" AS "id"
                  , Min(ST_Distance(ST_Transform(MakePoint({x}, {y}, {epsg}), {epsg_nodes}), "{gc_node}")) AS "dist_m"
                FROM "{tbl_node}"
                ;
            """.format(tbl_node=gt_node.name, gc_node=gt_node.gc, x=x, y=y, epsg=i_epsg, uid=s_uid_nodes
                       , epsg_nodes=gt_node.epsg)
            d_closest_point = dict(self.execute(sql).fetchone())

            if s_coordtype_nodes == 'geographic':  # get distance with ellipsoidal
                sql = u"""
                    SELECT ST_Distance(ST_Transform(MakePoint({x}, {y}, {epsg}), {epsg_nodes}), "{gc_node}", 1)
                    FROM "{tbl_node}" WHERE "{uid}" == {id};
                """.format(
                    tbl_node=gt_node.name, gc_node=gt_node.gc, x=x, y=y, epsg=i_epsg, uid=s_uid_nodes
                    , id=d_closest_point['id'], epsg_nodes=gt_node.epsg
                )
                d_closest_point['dist_m'] = self.execute(sql).fetchone()[0]

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
        sql = u"""
            SELECT "fid" AS id, "distance" AS dist_m
            FROM "KNN"
            WHERE "f_table_name" = '{tbl_node}'
            AND "f_geometry_column" = '{gc_node}'
            AND "ref_geometry" = MakePoint({x}, {y}, {epsg})
            AND "max_items" = 1
            ;
        """.format(tbl_node=gt_node.name, gc_node=gt_node.gc, x=x, y=y, epsg=i_epsg_lonlat)
        d_closest_point = dict(self.execute(sql).fetchone())

        print('closest point id:{}, dist_m:{}'.format(d_closest_point['id'], d_closest_point['dist_m']))
        return d_closest_point

    # 登録されているジオメトリカラムの情報をもとに、テーブル名からgtblを得る
    def get_gtbl_from_metadata(self, s_tbl, s_gc=None):
        if s_gc is None:
            sql = u"""SELECT * FROM "geometry_columns" WHERE "f_table_name" IS '{}';""".format(s_tbl)
        else:
            sql = u"""
                SELECT * FROM "geometry_columns" WHERE "f_table_name" IS '{}' AND "f_geometry_column" IS '{}';
            """.format(s_tbl, s_gc)
        ll_fetch = self.execute(sql).fetchall()
        d_type = {1: 'pt', 2: 'ln', 3: 'pg', 4: 'mpt', 5: 'mln', 6: 'mpg'}  # M、Z次元には未対応（dimension列見てない）
        if len(ll_fetch) == 1:
            l_fetch = ll_fetch[0]
            gt = GTBL(s_tbl, l_fetch['srid'], d_type[l_fetch['geometry_type']], l_fetch['f_geometry_column'])
        elif len(ll_fetch) == 0:
            print('no table and/or geometry column was found. please check them.')
            return 0
        else:
            print('geotable "{}" has multiple geometry column. please identify the geometry column.'.format(s_tbl))
            return 0

        return gt

    # GTBLならそれを、違うなら作成してGTBLを返す
    def ch_and_get_gtbl(self, gt_or_tbl, s_gc, i_epsg, s_geomtype):
        if isinstance(gt_or_tbl, GTBL):
            gt = gt_or_tbl
        elif isinstance(gt_or_tbl, types.StringTypes) and i_epsg is not None and s_geomtype is not None:
            if s_geomtype in (
                'pt', 'ln', 'pg', 'mpt', 'mln', 'mpg', 'mptm', 'mlnm', 'mpgm', 'ptz' 'ptzm', 'mptz', 'mptzm'
            ):
                s_type_abr = s_geomtype
            else:
                s_type_abr = self.get_type_abr(s_geomtype)
            gt = GTBL(gt_or_tbl, i_epsg, s_type_abr, s_gc)
        else:
            print('given parameter is something wrong')
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
                print('if you give s_tbl instead of gtbl object, parameter "epsg" must not be None.')
                return 0
            if s_type_abr is None:
                print('if you give s_tbl instead of gtbl object, parameter "s_type_abr" must not be None.')
                return 0
            s_tbl = gt_or_tbl

        if s_type_abr in ('pt', 'ln', 'pg', 'mpt', 'mln', 'mpg', 'mptm', 'mlnm', 'mpgm', 'ptz' 'ptzm', 'mptz', 'mptzm'):
            s_type_abr = s_type_abr
        else:
            s_type_abr = self.get_type_abr(s_type_abr)

        gt = GTBL(s_tbl, epsg, s_type_abr, gc)
        return gt

    # レイヤ名を返す
    @staticmethod
    def get_tblname(gt_or_tbl):
        if isinstance(gt_or_tbl, GTBL):
            s_tbl = gt_or_tbl.name
        elif isinstance(gt_or_tbl, types.StringTypes):
            s_tbl = gt_or_tbl
        else:
            print('given parameter is something wrong')
            return 0  # 処理終了

        return s_tbl

    # レイヤ名とジオメトリカラム名を返す
    @staticmethod
    def get_tblname_and_gc(gt_or_tbl, s_gc=None):
        if isinstance(gt_or_tbl, GTBL) and s_gc is None:
            s_tbl = gt_or_tbl.name
            s_gc = gt_or_tbl.gc
        elif isinstance(gt_or_tbl, types.StringTypes) and s_gc is not None:
            s_tbl = gt_or_tbl
        else:
            print('given parameter is something wrong')
            print('given gt_or_tbl is {}'.format(gt_or_tbl))
            print('given s_gc is {}'.format(s_gc))
            return 0  # 処理終了

        return s_tbl, s_gc

    # ジオメトリタイプから略形表記を取得する
    @staticmethod
    def get_type_abr(s_geomtype):
        # 与えられたパラメータからgtblを定義する
        type_dict = {
            'pt': 'POINT', 'ln': 'LINESTRING', 'pg': 'POLYGON',
            'mpt': 'MULTIPOINT', 'mln': 'MULTILINESTRING', 'mpg': 'MULTIPOLYGON',
            'mptm': 'MULTIPOINT M', 'mlnm': 'MULTILINESTRING M', 'mpgm': 'MULTIPOLYGON M',
            'ptz': 'POINT Z', 'ptzm': 'POINT ZM', 'mptz': 'MULTIPOINT Z', 'mptzm': 'MULTIPOINT ZM'
        }
        # typeからtype_abrを呼ぶために辞書を反転する
        type_dict_inv = {v: k for k, v in type_dict.items()}  # 辞書内包表記で反転できるそうな。
        s_type_abr = type_dict_inv[s_geomtype]

        return s_type_abr

    # 適当な一時テーブル名を作成
    @staticmethod
    def get_temptblname(s_prefix='geotemp'):
        # 適当な一時テーブル名を作成 http://qiita.com/FGtatsuro/items/92bca91ed665449ab047
        import string
        import random
        s_temptbl = ''.join([random.choice(string.ascii_letters) for i in range(10)])
        s_temptbl = '{}_{}'.format(s_prefix, s_temptbl)

        return s_temptbl


class GTBL(object):
    def __init__(self, name, epsg, type_abr, gc=None):
        self.name = name
        self.epsg = epsg
        self.type_abr = type_abr

        if gc is None:
            gc = 'geom_{}_{}'.format(type_abr, epsg)
        self.gc = gc

        type_dict = {
            'pt': 'POINT', 'ln': 'LINESTRING', 'pg': 'POLYGON',
            'mpt': 'MULTIPOINT', 'mln': 'MULTILINESTRING', 'mpg': 'MULTIPOLYGON',
            'mptm': 'MULTIPOINT M', 'mlnm': 'MULTILINESTRING M', 'mpgm': 'MULTIPOLYGON M',
            'ptz': 'POINT Z', 'ptzm': 'POINT ZM', 'mptz': 'MULTIPOINT Z', 'mptzm': 'MULTIPOINT ZM'
        }
        self.type = type_dict[type_abr]

