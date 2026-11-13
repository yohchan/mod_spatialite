"""SpatiaLite を Python の sqlite3 から扱いやすくするためのモジュール群。

公開するもの:
- SpatiaLiteConnection: sqlite3.Connection を継承し、SpatiaLite 拡張の読み込みと初期化を行う
- GTBL: テーブル名・ジオメトリカラム・EPSG・ジオメトリタイプの組
- standardmesh: JIS 標準地域メッシュのユーティリティ（サブモジュール）

_core 等の先頭が _ のモジュールは内部用。利用側は mod_spatialite から直接 import する。
"""

from ._core import GTBL, SpatiaLiteConnection

__all__ = ["GTBL", "SpatiaLiteConnection"]
