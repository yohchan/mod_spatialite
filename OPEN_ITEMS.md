# OPEN_ITEMS

未完了・持ち越し事項の一覧。1 件 1 行で追記し、完了した行は消さずに先頭に `[done YYYY-MM-DD]` を付ける。
API や挙動を変えた場合の記録（旧→新・利用側での直し方）は末尾の「変更の記録」に置く。

## 未完了

### 既知の問題点（2026-11-13、利用側からの照会の検証で判明）
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
- [ ] 2026-11-13 Windows 用の DLL フォルダの既定値が、旧開発環境の絶対パスのまま win32 分岐にハードコードされている（src/mod_spatialite/_core.py の initialize）
- [ ] 2026-11-13 generate_stdmesh は Python で 1 区画ずつ WKT を作って INSERT する（全国規模には不向き）。get_mesh_index の境界上の精度問題と 1/10・1/20 細分は未実装
- [ ] 2026-11-13 測地線距離の専用メソッドが無い（ST_Distance(a, b, 1) と MakeLine を生 SQL で書けば足りる）
- [done 2026-11-13] 2026-11-13 get_tblname / get_tblname_and_gc は staticmethod なのに、引数が不正なときの分岐で self を参照する（その分岐に入ると NameError。ruff F821）
- [ ] 2026-11-13 `.format` に使われない引数がある（ruff F523、src/mod_spatialite/_core.py の clone_table・create_cloned_table）。意図した値が SQL・メッセージに入っていない可能性を確認する
- [done 2026-11-13] ruff による一括整形（1c90525。.git-blame-ignore-revs に登録、Markdown は対象外）
- [ ] 2026-11-13 ruff の既存の指摘（行長 E501 ほか）の修正は未実施。各段階で触る箇所から順に直す
- [ ] 2026-11-13 新しく作る DB のページサイズが既定の 4096 のまま。列の多い表（例: 200 列を超え 1 行約 2KB の表）では 1 ページに 1 行しか入らず、ほぼ 2 倍の大きさになる（合成データで page_size 65536 にすると約半分を確認）。新規作成時に page_size を指定できる引数（InitSpatialMetaData より前に PRAGMA page_size）か、既存 DB のページサイズを変える補助（PRAGMA page_size → VACUUM）を設ける。優先度中
- [ ] 2026-11-13 zipshps2spatialite のポリゴン取り込みが遅い（数十万行規模のポリゴンで、dbf の属性だけを取り込む場合の約 3 倍の時間がかかる）。1 本ずつ一時表に ImportSHP してから CloneTable で追記する方式が原因と見ている。優先度低

### 設計の見直し
- [ ] 2026-11-13 マルチプラットフォーム（Linux/WSL・Windows）で配布できる形にする。SpatiaLite の DLL 群・proj.db は SpatiaLite 側の配布物のため、このリポジトリには含めず再配布もしない（利用者が別途入手する）。現行の「モジュールと同じ場所に置いた DLL・proj.db を PATH に足す」方式を、利用者が入手した SpatiaLite の場所を指定・検出できる方式に改める
- [ ] 2026-11-13 smoke_test_windows.py（未追跡。Windows/OSGeo4W 用の動作確認）を、上記の方式に合わせて tests/ に取り込む（Windows のときだけ実行）
- [ ] 2026-11-13 mod_spatialite_old/（旧世代の Windows 用 DLL。コードからの参照なし）の退役
- [ ] 2026-11-13 requires-python の引き上げ（現行 >=3.10）と、任意依存 osgeo（GDAL/OGR。shp2spatialite_ogr 等で関数内 import）の宣言。editable で参照する利用側の依存解決に響くため、変更の記録とあわせて行う
- [done 2026-11-13] パッケージ構成（平置きの py-modules mod_spatialite・mod_standardmesh）の見直し → src/mod_spatialite/ パッケージへ配置換え（変更の記録を参照）
- [ ] 2026-11-13 互換用モジュール mod_standardmesh の扱い。利用側が `from mod_spatialite import standardmesh` に移った後、DeprecationWarning を出し、さらに後で削除する
- [ ] 2026-11-13 src/mod_spatialite/_core.py（約 2,400 行）の分割（接続クラス・GTBL 等）。中身の見直しと一緒に行う。分割しても公開名（mod_spatialite から import する名前）は変えない

### リポジトリの運用
- [done 2026-11-13] 開発依存グループ（pytest・ruff。4c58810）の追加が、editable で参照する利用側の `uv lock --check`／`uv run --locked` に影響しないかを確認する → 影響した（利用側の lock がずれて止まる）。開発依存グループを取り下げて解消（変更の記録を参照）
- [ ] 2026-11-13 手元の git 設定が core.autocrlf=true のため、新しいファイルが作業ツリーでは CRLF で書き出される。.gitattributes（現行は `* text=auto` と本体 1 ファイルの eol=lf）で全テキストの改行を LF に固定するかを決める
- [ ] 2026-11-13 .gitignore がルートの `/mod_spatialite/`（Windows 用 DLL のフォルダ）を除外している。配置換えで同じ名前のパッケージフォルダを作ると丸ごと無視されるため、配置換えの前に DLL の置き場所と除外の規則を見直す
- [done 2026-11-13] 手元の作業記録・退避の置き場のバックアップの方式・頻度を README に書く（外付け HDD への手作業の複製、不定期）

### 利用側
- [ ] 2026-11-13 旧配置（以前の開発場所に残る旧メインリポジトリの写し。4bd68fe で停止し、7251b24 以降の修正を含まない）を sys.path 注入・exec で読んでいる利用側は、各利用側でこのリポジトリの editable 参照へ切り替える（このリポジトリでは行わない）

## 変更の記録

### 2026-11-13 配置換え: src/mod_spatialite/ パッケージ化（挙動の変更なし）
- 旧→新
  - `mod_spatialite.py`（リポジトリ直下のモジュール）→ `src/mod_spatialite/_core.py`（パッケージ `mod_spatialite` の内部モジュール）。公開名 `SpatiaLiteConnection`・`GTBL` は `src/mod_spatialite/__init__.py` から公開
  - `mod_standardmesh.py` → `src/mod_spatialite/standardmesh.py`。旧名 `mod_standardmesh` は互換用モジュール `src/mod_standardmesh.py` が同じ関数をそのまま再公開する（警告なし）
  - 依存・requires-python は変更なし（利用側の uv.lock の取り直しは不要）
  - Windows で同梱 proj.db を探す場所は、従来どおりリポジトリ直下（移動前の「モジュールの隣」と同じ場所）
- 利用側の直し方
  - import の書き方は変更不要（`from mod_spatialite import SpatiaLiteConnection, GTBL`・`import mod_standardmesh` とも従来どおり動く）。新しく書くコードでは `from mod_spatialite import standardmesh` を推奨
  - uv で editable 参照している場合: `uv run` を通せば自動で新しい配置を読む。**`.venv` を直接使う Jupyter カーネル・IDE は、`uv sync`（または `uv run` を 1 回）のあと再起動する**。しないと editable の登録が旧配置（リポジトリ直下の mod_spatialite.py）を指したままで、`import mod_spatialite` が失敗するか、直下の同名フォルダを読んで `cannot import name 'SpatiaLiteConnection'` になる
  - リポジトリ直下を `sys.path` に足して読む方式は使えなくなった。editable 参照に切り替える
  - `mod_spatialite._core` 等の内部モジュールを直接参照しない（クラスの `__module__` は `mod_spatialite._core` になった）

### 2026-11-13 引数が不正なときのメッセージ表示の不具合
- get_method_name を staticmethod にし、呼び出し元のメソッド名を返すよう修正（以前は引数を取らない普通のメソッドで、self.get_method_name() は TypeError、返す名前も get_method_name 自身だった）。これを使う get_extent・rename_geomcol・ch_and_get_gtbl の不正引数の分岐が、エラーで止まらずメッセージを出すようになった
- staticmethod の get_tblname / get_tblname_and_gc は self を参照していた（NameError）。関数名を直接表示するよう修正
- 挙動はいずれも意図どおり（メッセージを表示して 0 または None を返す）に戻っただけ。利用側の直し方: 不要

### 2026-11-13 開発依存グループの追加（4c58810・718c3ec）と取り下げ
- 4c58810 で pyproject.toml に `[dependency-groups] dev`（pytest・ruff）を追加した。editable の path 依存では uv が参照先の dependency-groups を利用側の uv.lock（mod-spatialite の `[package.metadata.requires-dev]`）に写すため、利用側の `uv lock --check` が不整合となり、`uv run --locked`／`uv sync --locked` が止まった（利用側 2 リポジトリで確認）。解決されるパッケージは増えない
- 取り下げのコミットで `[dependency-groups]` を削除し、uv.lock を 4c58810 より前と同じ内容（mod-spatialite 自身のみ）に戻した
- 利用側の直し方: 不要（取り下げにより、4c58810 より前に取った利用側の lock のまま整合する）。4c58810〜718c3ec の間に利用側で `uv lock` を取り直していた場合は、もう一度 `uv lock` を取り直す

### 2026-11-13 commit 97b5332（利用側の依頼による）
- 追加（既存の呼び出しへの影響なし）
  - mod_standardmesh.get_meshcode_sql_extent / get_meshcode_sql_mbr / get_meshcode_sql_point: メッシュコードの列から区画（BuildMbr）・中心点（MakePoint）の SQL 式を返す。レベル 1, 2, 3, "half", "quarter"。SRID は引数
  - SpatiaLiteConnection.zipdbfs2spatialite: zip 中の dbf の属性だけをジオメトリなしの表に追記（既定 CP932、dbf_name 列付き）
- 挙動の変更
  - SpatiaLiteConnection.zipshps2spatialite: 引数 p_dir_tmp を追加。展開先の既定を「zip と同じディレクトリ」→「システムの一時ディレクトリ」に変更。zip を名前順に処理し、zip のパスのリストも受け付ける。失敗しても一時ディレクトリを消す
  - 利用側の直し方: 不要（引数は後ろに追加しただけ）。zip の隣に展開されることを前提にしたコードは無い想定

### 2026-11-13 commit 7251b24
- get_stdmeshcode2wkt / generate_stdmesh が全コードで ValueError になっていた不具合を修正（回帰テスト tests/test_standardmesh.py を追加）。利用側の直し方: 不要
