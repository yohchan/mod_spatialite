# OPEN_ITEMS

未完了・持ち越し事項の一覧。1 件 1 行で追記し、完了した行は消さずに先頭に `[done YYYY-MM-DD]` を付ける。
API や挙動を変えた場合の記録（旧→新・利用側での直し方）は末尾の「変更の記録」に置く。

## 未完了

### 既知の問題点（2026-11-13、利用側からの照会の検証で判明）
- [done 2026-11-14] attach_db() が URI（`file:...?mode=ro`）を受け付けない（生 SQL の ATTACH は uri=True 接続なら可）
- [done 2026-11-14] コンストラクタは主接続のパスにファイルが無いと空ファイルを作り、毎回 InitSpatialMetaData を実行する。URI を主接続に渡すと open() で落ちる
- [done 2026-11-14] GTBL 系のヘルパー（get_gtbl_from_metadata・get_primary_key・ch_exists_table 等）が main しか見ず、ATTACH 先の表を扱えない
- [done 2026-11-13] zipshps2spatialite が zip と同じディレクトリに一時展開する（読み取り専用の置き場で PermissionError）→ 97b5332 で既定をシステムの一時ディレクトリへ
- [done 2026-11-14] zipshps2spatialite / shp2spatialite の append モードで、ジオメトリ列名が Geometry のまま残る → 追記しないモードと同じ規則で命名（変更の記録を参照）
- [done 2026-11-14] ch_exists_table・get_tbllist は名前を LIKE で照合するため、表名の _ と % がワイルドカードとして働く → ch_exists_table に flg_exact を追加（get_tbllist は LIKE のまま）
- [done 2026-11-14] get_nearest_neighbour は経緯度に平面の ST_Distance を使い、インデックスなしの全組合せになる。KNN 分岐は SpatiaLite 5.1.0 で未検証 → get_nearest を追加し、get_nearest_neighbour もそれを使う（KNN は 5.x に無く常に失敗していた）
- [done 2026-11-14] create_geometry_view は表名に大文字があると失敗する（views_geometry_columns の小文字制約）。overwrite=True は同名オブジェクトを種類を問わず削除する → 小文字で登録、削除はビューとその登録行に限定
- [done 2026-11-14] create_table の flg_temp が効かない（小文字の "create table" を置換しているが、列定義ありのときの SQL は大文字）
- [done 2026-11-14] disconnect() が既定で VACUUM と upgrade_db を実行する（大きい DB では重い）
- [done 2026-11-14] import 時・接続時の print と、SPATIALITE_SECURITY 環境変数の設定（import・接続時の副作用）
- [ ] 2026-11-13 Windows 用の DLL フォルダの既定値が、旧開発環境の絶対パスのまま win32 分岐にハードコードされている（src/mod_spatialite/_core.py の initialize）
- [done 2026-11-14] generate_stdmesh は Python で 1 区画ずつ WKT を作って INSERT する（全国規模には不向き）。1/10 細分は未実装 → SQL で一括生成、tenth を追加（北海道を覆う矩形の 1/2 メッシュ 134 万区画が約 4 秒）
- [done 2026-11-14] get_mesh_index は境界上の点で浮動小数の誤差により隣のメッシュを返すことがある → 格子番号（許容値つき）から作るよう修正（変更の記録を参照）
- [ ] 2026-11-14 1/20 細分（twentieth）は get_stdmeshcode2wkt・generate_stdmesh とも未対応（定義の確認が要る）。10 桁のコードは quarter と tenth を区別できない
- [done 2026-11-14] 測地線距離の専用メソッドが無い → 作らない（`ST_Distance(a, b, 1)` で足りる）。README に記載
- [done 2026-11-13] 2026-11-13 get_tblname / get_tblname_and_gc は staticmethod なのに、引数が不正なときの分岐で self を参照する（その分岐に入ると NameError。ruff F821）
- [done 2026-11-14] `.format` に使われない引数がある（ruff F523、clone_table・create_cloned_table）→ 警告文に表名を入れる形に修正（SQL への影響はなかった）
- [done 2026-11-13] ruff による一括整形（1c90525。.git-blame-ignore-revs に登録、Markdown は対象外）
- [ ] 2026-11-13 ruff の既存の指摘（行長 E501 ほか）の修正は未実施。各段階で触る箇所から順に直す
- [done 2026-11-14] 新しく作る DB のページサイズが既定の 4096 のまま。列の多い表（例: 200 列を超え 1 行約 2KB の表）では 1 ページに 1 行しか入らず、ほぼ 2 倍の大きさになる（合成データで page_size 65536 にすると約半分を確認）。新規作成時に page_size を指定できる引数（InitSpatialMetaData より前に PRAGMA page_size）か、既存 DB のページサイズを変える補助（PRAGMA page_size → VACUUM）を設ける。優先度中
- [ ] 2026-11-13 zipshps2spatialite のポリゴン取り込みが遅い（数十万行規模のポリゴンで、dbf の属性だけを取り込む場合の約 3 倍の時間がかかる）。1 本ずつ一時表に ImportSHP してから CloneTable で追記する方式が原因と見ている。優先度低

### 設計の見直し
- [ ] 2026-11-14 k 近傍（近い順に k 件。例 k=3〜5）を別の表（src の ID・dst の ID・順位・距離）に書くメソッド。経緯度では KNN2 の候補の順位が測地線の距離の順と一致しないことがあるため、k より多めの候補を取って測地線で並べ直す。規模の目安は src 数十万 × dst 約千。利用側の要望あり（急ぎではない）
- [ ] 2026-11-14 get_nearest は src を 1 行ずつ問い合わせる（数十万行では数分の見込み）。規模が大きい場合の高速化
- [done 2026-11-14] トランザクションを扱うコンテキストマネージャ（例: `with con.transaction():` で BEGIN し、正常終了で COMMIT、例外で ROLLBACK）を加えるか。接続は自動コミットのため 1 文ごとにコミットされ、大量の書き込みが遅い（数万行で数秒が十数分になる環境がある）。既存の begin() は BEGIN を出すだけで ROLLBACK しない。採否は未定
- [ ] 2026-11-14 print から置き換えた logging のメッセージの文面（英語・日本語の混在、誤字）の整理
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

### 2026-11-14 get_mesh_index の境界上の点
- 旧: 区画の境界上の点が、浮動小数の誤差で南西隣の区画のコードになることがあった（例: 経度 141.35 は 3 次メッシュの境界上だが、西隣の 64414277 を返していた）。新: 境界上の点は北東側の区画（例: 64414278）。境界から外れた点の値は変わらない
  - 利用側の直し方: 不要。ただし、境界上の点（区画の幅で割り切れる経緯度）について以前のコードを保存・照合している場合は、値が変わることがある

### 2026-11-14 generate_stdmesh の SQL 化
- 旧: 1 区画ずつ Python でコード・WKT を作って INSERT。範囲の南西端から 1 区画ずつ進めるため、範囲の幅が区画の整数倍でないと北端・東端を含むメッシュが抜けることがあった。境界上の点は浮動小数の誤差で南西隣のメッシュになることがあった
- 新: 格子番号から SQL でまとめて作る。範囲（境界を含む）と重なるメッシュをすべて作る（北端・東端がメッシュ境界に一致する場合はその北側・東側も含む）。境界上の点は北東側のメッシュに入る。mesh_level に "tenth"（1/10 細分）を追加。表の形・戻り値は不変
  - 利用側の直し方: 不要（同じ範囲で作り直すと、北端・東端の行・列が増えることがある）

### 2026-11-14 最近傍: get_nearest の追加と get_nearest_neighbour の切り替え
- 追加: `get_nearest(gt_src, gt_dst, s_col_id, s_col_dist=None, s_dst_id_col=None, flg_geodesic=None, flg_create_index=True)`。gt_src の各行の最近傍の ID（既定は gt_dst の主キー）と距離を書く。経緯度の SRID では測地線のメートル、投影座標では CRS の単位（flg_geodesic で明示も可）。空間インデックスで絞り、総当たりはしない
- get_nearest_neighbour: 旧 = 全組合せの総当たりで、経緯度でも度のまま平面距離で比べていた。use_virtual_knn=True は常に失敗（SpatiaLite 5 に仮想表 KNN が無い）。新 = get_nearest で gt_dst の ROWID を書く。**経緯度の SRID では測地線距離で比べるため、以前と結果（選ばれる最近傍）が変わる行がありうる**。use_virtual_knn は無視して警告。ジオメトリが NULL の行には NULL を書く（旧は更新しなかった）
  - 利用側の直し方: 呼び出しは変更不要。経緯度のデータで以前の結果を再計算すると値が変わりうる。以前と同じ結果（度のまま平面距離）が要る場合は `get_nearest(gt_src, gt_dst, s_col, s_dst_id_col="ROWID", flg_geodesic=False)` を使う

### 2026-11-14 create_geometry_view・ch_exists_table の完全一致
- create_geometry_view: 旧 = 表名・ビュー名に大文字があると登録に失敗。overwrite=True は同名のオブジェクトを種類を問わず削除（同名の表も消えた）。新 = views_geometry_columns に小文字で登録。overwrite=True で消すのは同名のビューとその登録行だけ（同名の表があればエラーで止まる）
  - 利用側の直し方: 不要（同名の表をビューで置き換える使い方をしていた場合は、表を明示的に消してから呼ぶ）
- ch_exists_table: 追加 = 引数 `flg_exact`（True で名前を完全一致〔大小区別なし〕で照合）。既定は従来どおり LIKE
  - 利用側の直し方: 不要（表名に _ を含む表の存在を確かめるときは `flg_exact=True` を推奨）

### 2026-11-14 シェープの追記取り込みのジオメトリ列名・cast_to_multi
- shp2spatialite（flg_append=True）・zipshps2spatialite: 旧 = 追記先のジオメトリ列名が "geometry"（返す GTBL の gc は "Geometry"）。flg_multi=True では 2 本目の追記で IntegrityError になり取り込めなかった。新 = 追記しない場合と同じ規則で、`s_gc_dst`、未指定なら `geom_<型>_<EPSG>`（flg_multi なら MULTI の型。例 `geom_mpg_4612`）。追記先の表が既にある場合は、その表の列名に合わせる
  - 利用側の直し方: 取り込んだ表のジオメトリ列を "Geometry"／"geometry" と直接書いている箇所は、返された GTBL の `gt.gc` を使う（または `s_gc_dst` で列名を指定する）。以前の版で作った表に追記し続ける場合は変更不要
- cast_to_multi: 旧 = 失敗していた（内部で GTBL を作れず AttributeError）。新 = MULTI 型に変換して登録し直し、変換後の GTBL を返す。shp2spatialite（flg_append=False, flg_multi=True）も動くようになった
  - 利用側の直し方: 不要

### 2026-11-14 transaction()・ATTACH 先の指定・flg_temp
- 追加: `SpatiaLiteConnection.transaction()`（`with con.transaction():` で BEGIN、正常終了で COMMIT、例外で ROLLBACK。BEGIN 済みのときは sqlite3.OperationalError）
  - 利用側の直し方: 不要（大量の書き込みをこれで囲むと速くなる）
- attach_db: 追加 = 引数 `flg_readonly`（パスから読み取り専用の URI を作って ATTACH）、URI 文字列（"file:...?mode=ro" 等）の受け付け。どちらも uri=True で開いた接続のときだけ有効で、URI を解釈しない接続に渡すと ValueError。パスで指定した場合の挙動は従来どおり
  - 利用側の直し方: 不要（読み取り専用で ATTACH したい場合は、接続を `SpatiaLiteConnection(p, uri=True)` で開き `attach_db(p_other, "o", flg_readonly=True)`）
- ATTACH 先の指定: 追加 = 引数 `s_schema`（get_tbllist・ch_exists_table・get_columnsdef・ch_exists_col・get_gtbl_from_metadata・get_primary_key）と `flg_include_views`（get_tbllist・ch_exists_table）。既定値では従来どおり
  - 利用側の直し方: 不要
- get_gtbl_from_metadata: 旧 = 表名・列名を大小区別して照合（大文字を含む名前では見つからない）。新 = 大小を区別せずに照合
  - 利用側の直し方: 不要
- create_table(flg_temp=True): 旧 = 列定義を渡すと永続表として作られていた（不具合）。新 = 列定義の有無にかかわらず一時表（接続を閉じると消える）
  - 利用側の直し方: flg_temp=True で作った表を永続表として使っていた場合は、flg_temp を外す

### 2026-11-14 段階 3: 副作用と接続の作法（挙動の変更あり）
- 出力: 旧 = import 時・接続時・各メソッドで print。新 = logging（ロガーは `mod_spatialite` 配下）。経過・成功は INFO（既定では表示されない）、引数の誤り・処理を飛ばして戻る分岐・エラーは WARNING（既定で stderr に出る）。import 時は何も出さない
  - 利用側の直し方: 経過を見たい場合は `logging.basicConfig(level=logging.INFO)` 等を設定する。print の出力を前提にした処理があれば logging に合わせる
- SPATIALITE_SECURITY: 旧 = 接続のたびに relaxed に上書き。新 = 未設定のときだけ relaxed（既定は従来どおり）
  - 利用側の直し方: 不要
- ファイルの作成と URI: 旧 = ファイルが無ければ自前で空ファイルを作る（URI を渡すと失敗）。新 = 作らない（sqlite3 自身が作る）。`SpatiaLiteConnection("file:a.sqlite?mode=ro", uri=True)` で読み取り専用で開ける（ファイルが無ければ作らずに sqlite3.OperationalError）。普通のパスに uri=True を付けた形も従来どおり新規作成される
  - 利用側の直し方: 不要
- メタデータの初期化: 旧 = 接続のたびに InitSpatialMetaData(1)。新 = 主接続（main）に spatial_ref_sys が無いときだけ実行（既存 DB で毎回出ていた "already exists" のエラー表示がなくなる）
  - 利用側の直し方: 不要
- ページサイズ: 追加 = コンストラクタの `i_page_size`（新しい DB のときだけ有効）、メソッド `set_page_size(i_page_size)`（既存 DB。VACUUM で作り直す）
  - 利用側の直し方: 不要（列の多い表を持つ DB は、新規作成時に `i_page_size=65536` 等を指定すると小さくなる）
- disconnect(): 旧 = 既定で VACUUM と upgrade_db を実行。新 = 既定は閉じるだけ
  - 利用側の直し方: 閉じるときに VACUUM・upgrade_db をしたい場合は `disconnect(flg_vacuum=True, flg_upgrade=True)` と書く
- 自動コミット（isolation_level=None）であることと、大量の書き込みは明示の BEGIN/COMMIT/ROLLBACK で囲むこと、`with con:` は BEGIN しないことを、SpatiaLiteConnection の docstring と README に明記（挙動は変更なし）

### 2026-11-13 配置換え: src/mod_spatialite/ パッケージ化（挙動の変更なし）
- 旧→新
  - `mod_spatialite.py`（リポジトリ直下のモジュール）→ `src/mod_spatialite/_core.py`（パッケージ `mod_spatialite` の内部モジュール）。公開名 `SpatiaLiteConnection`・`GTBL` は `src/mod_spatialite/__init__.py` から公開
  - `mod_standardmesh.py` → `src/mod_spatialite/standardmesh.py`。旧名 `mod_standardmesh` は互換用モジュール `src/mod_standardmesh.py` が同じ関数をそのまま再公開する（警告なし）
  - 依存・requires-python は変更なし（利用側の uv.lock の取り直しは不要）
  - Windows で同梱 proj.db を探す場所は、従来どおりリポジトリ直下（移動前の「モジュールの隣」と同じ場所）
- 利用側の直し方
  - import の書き方は変更不要（`from mod_spatialite import SpatiaLiteConnection, GTBL`・`import mod_standardmesh` とも従来どおり動く）。新しく書くコードでは `from mod_spatialite import standardmesh` を推奨
  - uv で editable 参照している場合: `uv run` を通せば自動で新しい配置を読む。**`.venv` を直接使う Jupyter カーネル・IDE は、`uv sync`（または `uv run` を 1 回）のあと再起動する**。しないと editable の登録が旧配置（リポジトリ直下の mod_spatialite.py）を指したままで、`import mod_spatialite` が `ModuleNotFoundError: No module named 'mod_spatialite'` で失敗する（利用側 2 リポジトリで確認）
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
