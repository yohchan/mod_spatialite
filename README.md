# mod_spatialite

SpatiaLite を Python の `sqlite3` から扱いやすくするためのモジュール群。

- `mod_spatialite`（パッケージ）: `SpatiaLiteConnection`（`sqlite3.Connection` を継承し、SpatiaLite 拡張の読み込みと初期化を行う）と、テーブル名・ジオメトリカラム・EPSG・ジオメトリタイプを組にして扱う `GTBL`。シェープファイル・zip の取り込み、ジオメトリカラムの登録・改名、インデックス、ATTACH などのメソッドを持つ。
- `mod_spatialite.standardmesh`: JIS 標準地域メッシュ（1次〜3次、1/2・1/4 細分）のコード計算、WKT 生成、メッシュコード列から区画・代表点を作る SQL 式の生成。旧名 `mod_standardmesh` でも import できる（互換用）。

状態: 2026-11-13 に保守を再開し、設計の見直しを進めている。未完了の事項・既知の問題点・API や挙動の変更の記録は [OPEN_ITEMS.md](OPEN_ITEMS.md) にある。

## 動作環境

| 項目 | 内容 |
|---|---|
| Python | 3.10 以上（`pyproject.toml` の `requires-python`） |
| Python の依存 | なし（標準ライブラリのみ）。`shp2spatialite_ogr` 等の一部メソッドは GDAL/OGR（`osgeo`）を関数内で import する |
| OS の依存（Linux/WSL） | SpatiaLite 拡張 `mod_spatialite`。Debian/Ubuntu では `libsqlite3-mod-spatialite`（`sudo apt install libsqlite3-mod-spatialite`） |
| 確認済みの組合せ | WSL2（Ubuntu）、Python 3.12.13、SQLite 3.50.4、SpatiaLite 5.1.0、PROJ 9.7.1、GEOS 3.14.1（2026-11-13） |
| パッケージ管理 | [uv](https://docs.astral.sh/uv/)。lock の生成・`requirements.txt` の書き出しに使った版は uv 0.11.14 |

### Windows

Windows では、OS に SpatiaLite が無いため、SpatiaLite の Windows 用バイナリ（DLL 群と `proj.db`）を手元に置いて使う。現行のコードは `sys.platform == "win32"` のときだけ次の処理をする。

- `SpatiaLiteConnection(..., p_mod_spatialite=<DLL のフォルダ>)` を指定すると、そのフォルダを `PATH` に追加して拡張を読む（指定しない場合の既定のパスは旧配置を指しており、見直し予定）。
- SpatiaLite 5 以上では、モジュールと同じフォルダの `proj.db` を `PROJ_SetDatabasePath` に渡す。

DLL 群と `proj.db` は SpatiaLite 側の配布物のため、このリポジトリには含めず、再配布もしない（手元に置いたものは `.gitignore` で除外）。Windows で使う場合は、利用者が SpatiaLite の配布元から別途入手する。入手先: <https://www.gaia-gis.it/gaia-sins/>。
Linux/WSL・Windows のどちらでも配布できる形への見直しは OPEN_ITEMS.md の「設計の見直し」にある。

## 使い方

### 他のプロジェクトから使う

このリポジトリを手元に clone し、利用側の `pyproject.toml` から editable の path 依存として参照する（`sys.path` への追加や `exec` による読み込みはしない）。

```toml
[project]
dependencies = ["mod-spatialite"]

[tool.uv.sources]
mod-spatialite = { path = "../mod_spatialite", editable = true }
```

```python
from mod_spatialite import SpatiaLiteConnection, GTBL
from mod_spatialite import standardmesh  # 旧名の import mod_standardmesh も可
```

`mod_spatialite._core` など先頭が `_` のモジュールは内部用のため、直接 import しない。リポジトリ直下を `sys.path` に足して読む方式は使えない（本体は `src/` の下にある）。

API や挙動を変えたときは、OPEN_ITEMS.md の「変更の記録」に旧→新と利用側での直し方を書く。利用側はそれを見て追随する。

### 接続

```python
from mod_spatialite import SpatiaLiteConnection

con = SpatiaLiteConnection("example.sqlite")  # ":memory:" も可

print(con.get_spatialite_version())  # SpatiaLite の版
print(
    con.execute("SELECT spatialite_version();").fetchone()[0]
)  # sqlite3 の接続として SQL も流せる
```

### GTBL（テーブルとジオメトリカラムの組）

```python
con.create_table("tbl_example", ll_coldef=[["col_1", "INTEGER"], ["col_2", "TEXT"]])
gt = con.add_geomcol("tbl_example", i_epsg=4612, s_geomtype="POINT")  # GTBL を返す

gt.name  # テーブル名 => tbl_example
gt.gc  # ジオメトリカラム名 => geom_pt_4612（指定しなければ自動で命名）
gt.epsg  # => 4612
gt.type  # => POINT
gt.type_abr  # ジオメトリタイプの略称 => pt（pt, ln, pg, mpt, mln, ...）

con.discard_geomcol(gt)  # ジオメトリカラムの登録解除
con.recov_geomcol(gt)  # ジオメトリカラムの登録
gt = con.rename_geomcol(gt, s_gc_dst="geom_renamed")  # ジオメトリカラムの改名
con.drop_geotable(gt)  # ジオテーブルの削除
```

### 標準地域メッシュ

```python
from mod_spatialite import standardmesh as ms

s_code = ms.get_mesh_index(141.35, 43.06, 3)  # 経度・緯度から 3 次メッシュコード
ms.detect_meshlevel(s_code)  # => 3
s_expr = ms.get_meshcode_sql_mbr(
    "meshcode", 3, 4612
)  # メッシュコード列から区画を作る SQL 式
```

## 開発

```bash
uv sync                                                          # .venv を作る
uv run --locked --with 'pytest==9.2.0' python -m pytest          # テスト（tests/）
uvx ruff@0.17.5 check .                                          # Lint（設定は pyproject.toml。既存コードの一括修正は未実施）
uv export --format requirements.txt -o requirements.txt          # uv.lock を更新したら書き出す（手編集しない）
```

- 開発用の道具（pytest・ruff）は `pyproject.toml` の `[dependency-groups]` に書かず、上のように版を指定して実行する。editable の path 依存では、uv が参照先の dependency-groups を利用側の `uv.lock` に写すため、ここを変えるたびに利用側の `uv run --locked` が止まるため。
- `uv.lock` と `requirements.txt` はコミットする。`requirements.txt` は uv が使えないときに pip で環境を再現するための退避用。
- `pyproject.toml` の `[project]`（dependencies・requires-python 等）を変えると、利用側の `uv.lock` も取り直しが要る。変えるときは OPEN_ITEMS.md の「変更の記録」に書く。
- コントリビューターは 1 名のため、`master` に直接コミット・push する。

### リポジトリの構成

| パス | 役割 |
|---|---|
| `src/mod_spatialite/` | 本体のパッケージ（`__init__.py` で公開する名前を定める。`_core.py` に接続クラスと GTBL、`standardmesh.py` に標準地域メッシュ） |
| `src/mod_standardmesh.py` | 旧名で import するための互換用モジュール |
| `tests/` | pytest のテスト |
| `tools/backup_local_only_files.py` | git が持っていない手元のファイル（未追跡・除外分）を、整理の前にデータ置き場へ SHA-256 の目録付きで退避する |
| `OPEN_ITEMS.md` | 未完了・持ち越し事項と変更の記録 |
| `pyproject.toml`、`uv.lock`、`requirements.txt` | 依存と設定 |

データや DB を持たないライブラリのため、データ置き場の受け皿（`data/`・`output/` など）・DB の登録・スナップショットの索引は置かない。

## データ・作業記録の所在

このリポジトリはデータを持たない。保守作業の記録と、整理前に退避した git 外のファイルは、開発者の手元の `~/data/mod_spatialite/`（リポジトリの外。別ドライブへの symlink）に置く。バックアップは外付け HDD への手作業の複製で、頻度は不定期。

| パス | 内容 |
|---|---|
| `backup/<ラベル>/` | `tools/backup_local_only_files.py` による退避コピーと `manifest.tsv`（SHA-256） |
| `revival/` | 2026-11 の保守再開の作業記録（棚卸し・利用側の一覧・段階の状態） |

## 経緯

- 2019〜2024: Windows 上で開発（旧配置は別の場所にあった旧メインリポジトリ）。
- 2026-07: WSL への移行に伴い、Linux/WSL で動くように Windows 固有の処理を `win32` の分岐に限定し、`pyproject.toml`（editable install 用）とスモークテストを追加。開発の場所をこのリポジトリ（WSL 側）に移した。
- 2026-11-13: 保守を再開。git 外のファイルを退避したうえで、再開前の状態にタグ `pre-revival-20261113`（7251b24）を付けた。
