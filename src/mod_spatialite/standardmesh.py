#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
2017/04/02
基準地域メッシュの取り扱いに関するモジュール
spatialiteと独立で扱えるものはこちらに入れ込む。conが必要なものについてはmod_spatialiteへ。
"""

import logging

logger = logging.getLogger(__name__)


# ある位置のメッシュコードを返す
def get_mesh_index(
    longitude: int or float, latitude: int or float, mesh_level: int or str = 3
) -> str:
    # todo: lonlat値が境界上の場合、計算精度の関係で誤った値になる場合がある。
    import math

    if mesh_level in (
        1,
        2,
        3,
        "5x",
        "2x",
        "half",
        "quarter",
        "eighth",
        "tenth",
    ):  # 1st mesh base
        x_tmp = longitude - 100
        y_tmp = latitude * 1.5

        n_part = 1.0  # 分割数の設定
        x1d = math.floor(x_tmp)
        y1d = math.floor(y_tmp)
        meshcode = f"{int(y1d)}{int(x1d)}"

        x_tmp = x_tmp - x1d / n_part  # / 1.0
        y_tmp = y_tmp - y1d / n_part
        if mesh_level != 1:  # 2nd mesh base
            n_part = n_part * 8
            x2d = math.floor(x_tmp * n_part)
            y2d = math.floor(y_tmp * n_part)
            meshcode = f"{meshcode}{int(y2d)}{int(x2d)}"

            x_tmp = x_tmp - x2d / n_part  # / 8.0
            y_tmp = y_tmp - y2d / n_part
            if mesh_level in (
                3,
                "half",
                "quarter",
                "eighth",
                "tenth",
                "twentieth",
            ):  # 3rd mesh base
                n_part = n_part * 10
                x3d = math.floor(x_tmp * n_part)
                y3d = math.floor(y_tmp * n_part)
                meshcode = f"{meshcode}{int(y3d)}{int(x3d)}"

                x_tmp = x_tmp - x3d / n_part  # / 80.0
                y_tmp = y_tmp - y3d / n_part

                if mesh_level not in (3, "tenth"):  # ext 3rd mesh base
                    n_part = n_part * 2
                    x4d = math.floor(x_tmp * n_part)
                    y4d = math.floor(y_tmp * n_part)
                    _4d = x4d + y4d * 2 + 1
                    meshcode = f"{meshcode}{int(_4d)}"

                    x_tmp = x_tmp - x4d / n_part  # / 160.0
                    y_tmp = y_tmp - y4d / n_part
                    if mesh_level != "half":  # half mesh base
                        n_part = n_part * 2
                        x5d = math.floor(x_tmp * n_part)
                        y5d = math.floor(y_tmp * n_part)
                        _5d = x5d + y5d * 2 + 1
                        meshcode = f"{meshcode}{int(_5d)}"

                        x_tmp = x_tmp - x5d / n_part  # / 320.0
                        y_tmp = y_tmp - y5d / n_part
                        if mesh_level != "quarter":  # quarter mesh base = eighth mesh
                            n_part = n_part * 2
                            x6d = math.floor(x_tmp * n_part)
                            y6d = math.floor(y_tmp * n_part)
                            _6d = x6d + y6d * 2 + 1
                            meshcode = f"{meshcode}{int(_6d)}"

                elif mesh_level in ("tenth", "twentieth"):  # 10th mesh base
                    n_part = (
                        n_part * 10
                    )  # change division number to 10 for 1/10 division
                    x4d = math.floor(x_tmp * n_part)
                    y4d = math.floor(y_tmp * n_part)
                    meshcode = f"{meshcode}{int(y4d)}{int(x4d)}"  # append y and x direction code

                    x_tmp = x_tmp - x4d / n_part  # / 800.0
                    y_tmp = y_tmp - y4d / n_part
                    if mesh_level == "twentieth":  # 20th mesh
                        n_part = n_part * 2
                        x5d = math.floor(x_tmp * n_part)
                        y5d = math.floor(y_tmp * n_part)
                        meshcode = f"{meshcode}{int(y5d)}{int(x5d)}"  # append y and x direction code

            else:  # ext 2nd mesh base
                if mesh_level == "5x":  # 5x mesh
                    n_part = n_part * 2
                    x3d = math.floor(x_tmp * n_part)
                    y3d = math.floor(y_tmp * n_part)
                    _3d = x3d + y3d * 2 + 1
                    meshcode = f"{meshcode}{int(_3d)}"

                elif mesh_level == "2x":  # 2x mesh
                    n_part = n_part * 5
                    x3d = math.floor(x_tmp * n_part) * 2
                    y3d = math.floor(y_tmp * n_part) * 2
                    meshcode = f"{meshcode}{int(y3d)}{int(x3d)}5"

        return meshcode

    else:
        logger.warning(f"invalid mesh_level: {mesh_level}")
        return "invalid mesh_level"


# メッシュレベルに基づいて経度と緯度のメッシュ単位サイズを取得する
def get_unitsize(mesh_level: int or str) -> tuple:
    """
    This function calculates the unit size (longitude and latitude) based on the provided mesh level.
    It supports the following mesh levels: 1, 2, 3, '5x', '2x', 'half', 'quarter', 'eighth', 'tenth'.
    If an invalid mesh level is provided, the function prints an error message and returns an empty tuple.

    Args:
        mesh_level (int or str): The desired mesh level, could be a string or an integer.
        1: 1次メッシュ

        --- 統合地域メッシュ ---
        2: 2次メッシュ（10倍地域メッシュ）
        '5x': 5倍地域メッシュ
        '2x': 2倍地域メッシュ

        3: 3次メッシュ

        --- 分割地域メッシュ ---
        'half': 1/2地域メッシュ
        'quarter': 1/4地域メッシュ
        'eighth': 1/8地域メッシュ
        'tenth': 1/10細分メッシュ

    Returns:
        tuple: If a valid mesh level is provided, it returns a tuple containing the unit longitude and
               unit latitude sizes.
               If an invalid mesh level is provided, it returns an empty tuple and prints an error message.
    """

    # 東西南北端の座標
    if mesh_level in (
        1,
        2,
        3,
        "5x",
        "2x",
        "half",
        "quarter",
        "eighth",
        "tenth",
    ):  # 1st mesh base
        unitlon = 1.0
        unitlat = 1.0 / 1.5
        if mesh_level != 1:  # 2nd mesh base
            unitlon = unitlon / 8
            unitlat = unitlat / 8
            if mesh_level in (
                3,
                "half",
                "quarter",
                "eighth",
                "tenth",
                "twentieth",
            ):  # 3rd mesh base 分割地域メッシュ
                unitlon = unitlon / 10
                unitlat = unitlat / 10
                if mesh_level in ("half", "quarter", "eighth"):  # ext 3rd mesh base
                    unitlon = unitlon / 2
                    unitlat = unitlat / 2
                    if mesh_level != "half":  # half mesh base
                        unitlon = unitlon / 2
                        unitlat = unitlat / 2
                        if mesh_level != "quarter":  # quarter mesh base = eighth mesh
                            unitlon = unitlon / 2
                            unitlat = unitlat / 2
                elif mesh_level in ("tenth", "twentieth"):  # 細分メッシュ
                    unitlon = unitlon / 10
                    unitlat = unitlat / 10
                    if mesh_level == "twentieth":  # 20th mesh
                        unitlon = unitlon / 2
                        unitlat = unitlat / 2
            else:  # ext 2nd mesh base 統合地域メッシュ
                if mesh_level == "5x":  # 5x mesh
                    unitlon = unitlon / 2
                    unitlat = unitlat / 2
                elif mesh_level == "2x":  # 2x mesh
                    unitlon = unitlon / 5
                    unitlat = unitlat / 5

        return unitlon, unitlat

    else:
        logger.warning(f"invalid mesh_level: {mesh_level}")
        return tuple()


# 基準地域メッシュのレベル判別
def detect_meshlevel(meshcode: str) -> int or str:
    # メッシュレベルの自動判別
    if len(meshcode) == 4:
        mesh_level = 1
    elif len(meshcode) == 6:
        mesh_level = 2
    elif len(meshcode) == 8:
        mesh_level = 3
    elif len(meshcode) == 7:
        mesh_level = "5x"
    elif len(meshcode) == 9:
        if meshcode[-1] == "5":
            mesh_level = "2x"
        elif meshcode[-1] in ("1", "2", "3", "4"):
            mesh_level = "half"
        else:
            mesh_level = 0
    elif len(meshcode) == 10:
        mesh_level = "quarter or tenth"
    elif len(meshcode) == 11:
        mesh_level = "eighth"
    elif len(meshcode) == 12:
        mesh_level = "twentieth"
    else:
        mesh_level = 0

    if mesh_level == 0:
        logger.warning(f"invalid meshcode moge: {meshcode}")

    return mesh_level


# メッシュコードの分割
def split_meshcode(meshcode: str) -> tuple:
    # メッシュタイプの選別
    mesh_level = detect_meshlevel(meshcode)

    # 値の初期化
    x1d = None
    y1d = None
    x2d = None
    y2d = None
    x3d = None
    y3d = None
    _3d = None
    _4d = None
    _5d = None
    _6d = None
    _7d = None

    # コードの分割
    if mesh_level != 0:  # 1st mesh base
        y1d = int(meshcode[0:2])
        x1d = int(meshcode[2:4])
        if mesh_level != 1:  # 2nd mesh base
            y2d = int(meshcode[4])
            x2d = int(meshcode[5])
            if mesh_level not in (2, "5x", "2x"):  # 3rd mesh base
                y3d = int(meshcode[6])
                x3d = int(meshcode[7])
                if mesh_level != 3:  # ext 3rd mesh base
                    _4d = int(meshcode[8])
                    if mesh_level != "half":  # half mesh base
                        _5d = int(meshcode[9])
                        if mesh_level != "quarter or tenth":
                            # quarter or tenth mesh base
                            _6d = int(meshcode[10])
                            if mesh_level != "eighth":  # twentieth mesh
                                _7d = int(meshcode[11])
            else:  # ext 2nd mesh base
                if mesh_level == "5x":  # 5x mesh
                    _3d = int(meshcode[6])
                elif mesh_level == "2x":  # 2x mesh
                    _3d = int(meshcode[6])
                    _4d = int(meshcode[7])

    return mesh_level, x1d, x2d, x3d, y1d, y2d, y3d, _3d, _4d, _5d, _6d, _7d


# メッシュコードからメッシュのWKTと端点座標を返す。
def get_stdmeshcode2wkt(meshcode: str) -> tuple:
    # todo: 1/10, 1/20細分メッシュの実装未実施 -> 1/10細分は1/4メッシュと同じコード長さなので、自動検出でextentの識別ができない
    """
    メッシュコードからメッシュのWKTと端点座標を返す。
    2017/07/28 メッシュレベルの自動判別を追加
    ref: http://www.gikosha.co.jp/fig_blog/mesh.html
    """

    # コード分割（split_meshcode は 1/20 細分用の _7d を含む12要素を返す）
    (mesh_level, x1d, x2d, x3d, y1d, y2d, y3d, _3d, _4d, _5d, _6d, _7d) = (
        split_meshcode(meshcode)
    )

    # 10桁コードは1/4地域メッシュと1/10細分メッシュを桁数で区別できない。
    # 1/10細分は未実装のため、1/4地域メッシュとして扱う。
    if mesh_level == "quarter or tenth":
        mesh_level = "quarter"

    # 1/20細分（12桁）は未実装
    if mesh_level == "twentieth":
        logger.warning(f"twentieth mesh is not supported: {meshcode}")
        return tuple()

    # 東西南北端の座標
    if mesh_level != 0:  # 1st mesh base
        unitlon = 1.0
        unitlat = 1.0 / 1.5
        minlon = x1d + 100.0
        minlat = y1d / 1.5
        if mesh_level != 1:  # 2nd mesh base
            unitlon = unitlon / 8
            unitlat = unitlat / 8
            minlon = minlon + x2d / 8.0
            minlat = minlat + y2d / 1.5 / 8
            if mesh_level not in (2, "5x", "2x"):  # 3rd mesh base
                unitlon = unitlon / 10
                unitlat = unitlat / 10
                minlon = minlon + x3d / 8.0 / 10
                minlat = minlat + y3d / 1.5 / 8 / 10
                if mesh_level != 3:  # ext 3rd mesh base
                    unitlon = unitlon / 2
                    unitlat = unitlat / 2
                    if _4d in (2, 4):
                        minlon = minlon + unitlon
                    if _4d in (3, 4):
                        minlat = minlat + unitlat
                    if mesh_level != "half":  # half mesh base
                        unitlon = unitlon / 2
                        unitlat = unitlat / 2
                        if _5d in (2, 4):
                            minlon = minlon + unitlon
                        if _5d in (3, 4):
                            minlat = minlat + unitlat
                        if mesh_level != "quarter":  # quarter mesh base = eighth mesh
                            unitlon = unitlon / 2
                            unitlat = unitlat / 2
                            if _6d in (2, 4):
                                minlon = minlon + unitlon
                            if _6d in (3, 4):
                                minlat = minlat + unitlat
            else:  # ext 2nd mesh base
                if mesh_level == "5x":  # 5x mesh
                    unitlon = unitlon / 2
                    unitlat = unitlat / 2
                    if _3d in (2, 4):
                        minlon = minlon + unitlon
                    if _3d in (3, 4):
                        minlat = minlat + unitlat
                elif mesh_level == "2x":  # 2x mesh
                    unitlon = unitlon / 5
                    unitlat = unitlat / 5
                    minlon = minlon + unitlon * _4d / 2
                    minlat = minlat + unitlat * _3d / 2
        maxlon = minlon + unitlon
        maxlat = minlat + unitlat

        # wkt
        wkt = f"POLYGON(({minlon} {minlat}, {minlon} {maxlat}, {maxlon} {maxlat}, {maxlon} {minlat}, {minlon} {minlat}))"
        return wkt, (minlon, minlat), (maxlon, maxlat)

    else:
        logger.warning(f"invalid meshcode hoge: {meshcode}")
        return tuple()


# メッシュコードのリストを取得
def get_meshlist_from_extent(
    lon_min: float, lat_min: float, lon_max: float, lat_max: float, mesh_level: int = 3
) -> list:
    # メッシュの幅を取得
    unitlon, unitlat = get_unitsize(mesh_level)

    # 緯度と経度でループを順に回して、メッシュリストを生成
    mesh_list = []
    lat = lat_min
    while lat <= lat_max:
        lon = lon_min
        while lon <= lon_max:
            # 該当する座標のメッシュコードを取得してリストに追加
            meshcode = get_mesh_index(lon, lat, mesh_level)
            mesh_list.append(meshcode)
            lon += unitlon
        lat += unitlat

    return mesh_list


# メッシュコードから区画・代表点を作る SQL 式
# Python で1区画ずつ WKT を作らず、SQL の中で計算させる（CREATE TABLE AS やビューに埋め込む用途）。
# 区画の定義は get_stdmeshcode2wkt と同じ（JIS X 0410）。
# 1次: 緯度 2/3 度×経度 1 度、2次: 1/8、3次: 1/10、1/2・1/4: 2分割ごとに 1=南西 2=南東 3=北西 4=北東
D_SQL_MESH_LEVELS: dict[int | str, int] = {
    1: 4,
    2: 6,
    3: 8,
    "half": 9,
    "quarter": 10,
}  # レベル: 桁数


def get_meshcode_sql_extent(
    s_code_expr: str, mesh_level: int | str
) -> tuple[str, str, str, str]:
    """
    メッシュコードの列名（または SQL 式）から、区画の南西端・北東端の経緯度を計算する SQL 式を返す。

    Args:
        s_code_expr: メッシュコードの列名または SQL 式（例: '"mesh_code"', 'm."MESH_ID"'）。
            INTEGER 列でも TEXT 列でもよい（内部で TEXT に CAST する）。
        mesh_level: 1, 2, 3, "half"（1/2地域メッシュ・9桁）, "quarter"（1/4地域メッシュ・10桁）のいずれか。
            桁数がレベルと合わない行の結果は保証しない（呼び出し側で length() 等により絞ること）。

    Returns:
        (lon_min, lat_min, lon_max, lat_max) の SQL 式の tuple
    """
    if mesh_level not in D_SQL_MESH_LEVELS:
        raise ValueError(
            f"mesh_level must be one of {list(D_SQL_MESH_LEVELS)}: {mesh_level!r}"
        )

    s_c = f"CAST(({s_code_expr}) AS TEXT)"

    def _digit(i_pos: int, i_len: int = 1) -> str:
        return f"CAST(substr({s_c}, {i_pos}, {i_len}) AS INTEGER)"

    # 1次メッシュ
    l_lon: list[str] = [f"(100 + {_digit(3, 2)})"]
    l_lat: list[str] = [f"({_digit(1, 2)} / 1.5)"]
    unitlon, unitlat = "1.0", "(1.0 / 1.5)"

    if mesh_level != 1:  # 2次メッシュ
        l_lon.append(f"({_digit(6)} / 8.0)")
        l_lat.append(f"({_digit(5)} / 12.0)")
        unitlon, unitlat = "(1.0 / 8)", "(1.0 / 12)"
    if mesh_level not in (1, 2):  # 3次メッシュ
        l_lon.append(f"({_digit(8)} / 80.0)")
        l_lat.append(f"({_digit(7)} / 120.0)")
        unitlon, unitlat = "(1.0 / 80)", "(1.0 / 120)"
    if mesh_level in (
        "half",
        "quarter",
    ):  # 1/2地域メッシュ（9桁目: 1=南西 2=南東 3=北西 4=北東）
        l_lon.append(f"((({_digit(9)} - 1) % 2) / 160.0)")
        l_lat.append(
            f"((({_digit(9)} - 1) / 2) / 240.0)"
        )  # INTEGER 同士の除算は切り捨て
        unitlon, unitlat = "(1.0 / 160)", "(1.0 / 240)"
    if mesh_level == "quarter":  # 1/4地域メッシュ（10桁目）
        l_lon.append(f"((({_digit(10)} - 1) % 2) / 320.0)")
        l_lat.append(f"((({_digit(10)} - 1) / 2) / 480.0)")
        unitlon, unitlat = "(1.0 / 320)", "(1.0 / 480)"

    s_lon_min = "(" + " + ".join(l_lon) + ")"
    s_lat_min = "(" + " + ".join(l_lat) + ")"
    return (
        s_lon_min,
        s_lat_min,
        f"({s_lon_min} + {unitlon})",
        f"({s_lat_min} + {unitlat})",
    )


def get_meshcode_sql_mbr(s_code_expr: str, mesh_level: int | str, i_epsg: int) -> str:
    """
    メッシュコードから区画のポリゴン（BuildMbr）を作る SQL 式を返す。

    例: f'CREATE TABLE t AS SELECT mesh_code, {get_meshcode_sql_mbr("mesh_code", "half", 4612)} AS pg_4612 FROM src'
    （CREATE TABLE AS で作った列はジオメトリ列として登録されないため、必要なら RecoverGeometryColumn を実行する）
    """
    s_lon_min, s_lat_min, s_lon_max, s_lat_max = get_meshcode_sql_extent(
        s_code_expr, mesh_level
    )
    return (
        f"BuildMbr({s_lon_min}, {s_lat_min}, {s_lon_max}, {s_lat_max}, {int(i_epsg)})"
    )


def get_meshcode_sql_point(s_code_expr: str, mesh_level: int | str, i_epsg: int) -> str:
    """
    メッシュコードから区画の中心点（MakePoint）を作る SQL 式を返す。
    代表点は区画の中心（経緯度の中点）とする。
    """
    s_lon_min, s_lat_min, s_lon_max, s_lat_max = get_meshcode_sql_extent(
        s_code_expr, mesh_level
    )
    return (
        f"MakePoint(({s_lon_min} + {s_lon_max}) / 2.0, "
        f"({s_lat_min} + {s_lat_max}) / 2.0, {int(i_epsg)})"
    )
