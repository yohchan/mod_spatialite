# mod_spatialite

SpatiaLiteをPython27で利用しやすく？するためのモジュール。あ、Windowsでしか動作確認してません。。
mod_spatialite.pyと同じディレクトリに`mod_spatialite`という名称のディレクトリを作成し、mod_spatialite関連のファイルを入れておく。

c.f.
* mod_spatialite入手先：https://www.gaia-gis.it/gaia-sins/
* v4.3.0a win-x64: http://www.gaia-gis.it/gaia-sins/windows-bin-amd64/mod_spatialite-4.3.0a-win-amd64.7z
* v5.0.0beta win-x64: http://www.gaia-gis.it/gaia-sins/windows-bin-NEXTGEN-amd64/mod_spatialite-NG-win-amd64.7z


つかいかた：
```python
from mod_spatialite import SpatiaLiteConnection  # SpatiaLiteConnectionクラス（sqlite3.Connectionクラスを継承）

con = SpatiaLiteConnection('hoge.sqlite')  # ':memory:'も利用可。

version = con.get_spatialite_version()
print(version)  # SpatiaLiteのバージョンが得られる。

version = con.execute('SELECT spatialite_version();').fetchone()[0]
print(version)  # sqlite3のconnectionとして普通にクエリも流せる。
```

ジオメトリカラムの情報とテーブルをセットにしたGTBLクラスでジオメトリ情報を管理できる。
```python
con.create_table('tbl_hoge', ll_coldef=[['col_1', 'INTEGER'], ['col_2', 'TEXT']])  # テーブルを生成するメソッド
gt = con.add_geomcol('tbl_hoge', i_epsg=4612, s_geomtype='POINT')  # ジオメトリカラムを生成するメソッド

# gtはGTBLクラスのオブジェクト
gt.name  # テーブル名 => tbl_hoge
gt.gc  # ジオメトリカラム名 => geom_pt_4612 （gt生成時に指定しない場合は、自動的にカラム名称作成）
gt.epsg  # ジオメトリカラムのEPSG => 4612
gt.type  # ジオメトリタイプ => POINT
gt.type_abr  # ジオメトリタイプの略称（オレオレタイプ） => pt （pt, ln, pg, mpt, mln...）

con.discard_geomcol(gt)  # ジオメトリカラムの登録解除メソッド
con.recov_geomcol(gt)  # ジオメトリカラムの登録メソッド
con.rename_geomcol(gt, s_gc_dst='geom_renamed')  # ジオメトリカラムをリネームするメソッド
con.rename_geotable('tbl_hoge', 'tbl_fuga')  # ジオテーブルをリネームするメソッド
con.drop_geotable(gt)  # ジオテーブルを削除するメソッド
```

なんか他にもいろいろメソッド作ってあるけど、そんな感じで、、、 （説明不足）
