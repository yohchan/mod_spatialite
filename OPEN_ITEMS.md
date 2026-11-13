# OPEN_ITEMS

未完了・持ち越し事項の一覧。1 件 1 行で追記し、完了した行は消さずに先頭に `[done YYYY-MM-DD]` を付ける。
API や挙動を変えた場合の記録（旧→新・利用側での直し方）は末尾の「変更の記録」に置く。

## 未完了

### 既知の問題点（2026-11-13、2022_MSW からの照会の検証で判明）
- [ ] 2026-11-13 attach_db() が URI（`file:...?mode=ro`）を受け付けない（生 SQL の ATTACH は uri=True 接続なら可）
- [ ] 2026-11-13 コンストラクタは主接続のパスにファイルが無いと空ファイルを作り、毎回 InitSpatialMetaData を実行する。URI を主接続に渡すと open() で落ちる
- [ ] 2026-11-13 GTBL 系のヘルパー（get_gtbl_from_metadata・get_primary_key・ch_exists_table 等）が main しか見ず、ATTACH 先の表を扱えない
- [done 2026-11-13] zipshps2spatialite が zip と同じディレクトリに一時展開する（読み取り専用の置き場で PermissionError）→ 97b5332 で既定をシステムの一時ディレクトリへ
- [ ] 2026-11-13 zipshps2spatialite / shp2spatialite の append モードで、ジオメトリ列名が Geometry のまま残る
- [ ] 2026-11-13 get_nearest_neighbour は経緯度に平面の ST_Distance を使い、インデックスなしの全組合せになる。KNN 分岐は SpatiaLite 5.1.0 で未検証
- [ ] 2026-11-13 create_geometry_view は表名に大文字があると失敗する（views_geometry_columns の小文字制約）。overwrite=True は同名オブジェクトを種類を問わず削除する
- [ ] 2026-11-13 create_table の flg_temp が効かない（小文字の "create table" を置換しているが、列定義ありのときの SQL は大文字）
- [ ] 2026-11-13 disconnect() が既定で VACUUM と upgrade_db を実行する（大きい DB では重い）
- [ ] 2026-11-13 import 時・接続時の print と、SPATIALITE_SECURITY 環境変数の設定（import・接続時の副作用）
- [ ] 2026-11-13 Windows 用の絶対パス（C:/Dropbox/.../mod_spatialite/mod_spatialite）が win32 分岐にハードコードされている（mod_spatialite.py の initialize）
- [ ] 2026-11-13 generate_stdmesh は Python で 1 区画ずつ WKT を作って INSERT する（全国規模には不向き）。get_mesh_index の境界上の精度問題と 1/10・1/20 細分は未実装
- [ ] 2026-11-13 測地線距離の専用メソッドが無い（ST_Distance(a, b, 1) と MakeLine を生 SQL で書けば足りる）
- [ ] 2026-11-13 zipshps2spatialite のポリゴン取り込みが遅い（全国 471,024 行で 211.8 秒、dbf だけなら 66.7 秒。2022_MSW で測定）。1 本ずつ一時表に ImportSHP してから CloneTable で追記する方式が原因と見ている。優先度低

### 設計の見直し
- [ ] 2026-11-13 マルチプラットフォーム（Linux/WSL・Windows）で配布できる形にする。Windows 用 DLL 群（mod_spatialite/）と同梱 proj.db を手元に置いて PATH に足す現行方式（git 管理外）を、配布できる方式に置き換える
- [ ] 2026-11-13 smoke_test_windows.py（未追跡。Windows/OSGeo4W 用の動作確認）を、上記の方式に合わせて tests/ に取り込む（Windows のときだけ実行）
- [ ] 2026-11-13 mod_spatialite_old/（旧世代の Windows 用 DLL。コードからの参照なし）の退役
- [ ] 2026-11-13 requires-python の引き上げ（現行 >=3.10）と、任意依存 osgeo（GDAL/OGR。shp2spatialite_ogr 等で関数内 import）の宣言。editable で参照する利用側の依存解決に響くため、変更の記録とあわせて行う
- [ ] 2026-11-13 パッケージ構成（平置きの py-modules mod_spatialite・mod_standardmesh）の見直し。利用側の import と editable 参照に響くため、変更の記録とあわせて行う

### 利用側
- [ ] 2026-11-13 旧配置（Dropbox 側の旧メインリポジトリ。4bd68fe で停止し、7251b24 以降の修正を含まない）を sys.path 注入・exec で読んでいる利用側は、各リポジトリ側でこのリポジトリの参照へ切り替える（このリポジトリでは行わない）

## 変更の記録

### 2026-11-13 commit 97b5332（2022_MSW の依頼による）
- 追加（既存の呼び出しへの影響なし）
  - mod_standardmesh.get_meshcode_sql_extent / get_meshcode_sql_mbr / get_meshcode_sql_point: メッシュコードの列から区画（BuildMbr）・中心点（MakePoint）の SQL 式を返す。レベル 1, 2, 3, "half", "quarter"。SRID は引数
  - SpatiaLiteConnection.zipdbfs2spatialite: zip 中の dbf の属性だけをジオメトリなしの表に追記（既定 CP932、dbf_name 列付き）
- 挙動の変更
  - SpatiaLiteConnection.zipshps2spatialite: 引数 p_dir_tmp を追加。展開先の既定を「zip と同じディレクトリ」→「システムの一時ディレクトリ」に変更。zip を名前順に処理し、zip のパスのリストも受け付ける。失敗しても一時ディレクトリを消す
  - 利用側の直し方: 不要（引数は後ろに追加しただけ）。zip の隣に展開されることを前提にしたコードは無い想定

### 2026-11-13 commit 7251b24
- get_stdmeshcode2wkt / generate_stdmesh が全コードで ValueError になっていた不具合を修正（回帰テスト tests/test_standardmesh.py を追加）。利用側の直し方: 不要
