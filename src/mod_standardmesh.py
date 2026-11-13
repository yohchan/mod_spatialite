"""互換用モジュール: 旧名 mod_standardmesh で mod_spatialite.standardmesh を読めるようにする。

新しいコードでは `from mod_spatialite import standardmesh` を使う。
"""

from mod_spatialite.standardmesh import *  # noqa: F401,F403
from mod_spatialite.standardmesh import (  # noqa: F401  （* では拾われない名前も明示する）
    detect_meshlevel,
    get_mesh_index,
    get_meshcode_sql_extent,
    get_meshcode_sql_mbr,
    get_meshcode_sql_point,
    get_meshlist_from_extent,
    get_stdmeshcode2wkt,
    get_unitsize,
    split_meshcode,
)
