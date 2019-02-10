#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
2017/04/02
基準地域メッシュの取り扱いに関するモジュール
spatialiteと独立で扱えるものはこちらに入れ込む。conが必要なものについてはmod_spatialiteへ。
"""


# ある位置のメッシュコードを返す
def get_mesh_index(longitude, latitude, mesh_level=3):
    import math

    if mesh_level in (1, 2, 3, '5x', '2x', 'half', 'quarter', 'eighth'):  # 1st mesh base
        x_tmp = longitude - 100
        y_tmp = latitude * 1.5

        n_part = 1.0  # 分割数の設定
        x1d = math.floor(x_tmp)
        y1d = math.floor(y_tmp)
        meshcode = '{}{}'.format(int(y1d), int(x1d))

        x_tmp = x_tmp - x1d / n_part  # / 1.0
        y_tmp = y_tmp - y1d / n_part
        if mesh_level != 1:  # 2nd mesh base
            n_part = n_part * 8
            x2d = math.floor(x_tmp * n_part)
            y2d = math.floor(y_tmp * n_part)
            meshcode = '{}{}{}'.format(meshcode, int(y2d), int(x2d))

            x_tmp = x_tmp - x2d / n_part  # / 8.0
            y_tmp = y_tmp - y2d / n_part
            if mesh_level not in (2, '5x', '2x'):  # 3rd mesh base
                n_part = n_part * 10
                x3d = math.floor(x_tmp * n_part)
                y3d = math.floor(y_tmp * n_part)
                meshcode = '{}{}{}'.format(meshcode, int(y3d), int(x3d))

                x_tmp = x_tmp - x3d / n_part  # / 80.0
                y_tmp = y_tmp - y3d / n_part
                if mesh_level != 3:  # ext 3rd mesh base
                    n_part = n_part * 2
                    x4d = math.floor(x_tmp * n_part)
                    y4d = math.floor(y_tmp * n_part)
                    _4d = x4d + y4d * 2 + 1
                    meshcode = '{}{}'.format(meshcode, int(_4d))

                    x_tmp = x_tmp - x4d / n_part  # / 160.0
                    y_tmp = y_tmp - y4d / n_part
                    if mesh_level != 'half':  # half mesh base
                        n_part = n_part * 2
                        x5d = math.floor(x_tmp * n_part)
                        y5d = math.floor(y_tmp * n_part)
                        _5d = x5d + y5d * 2 + 1
                        meshcode = '{}{}'.format(meshcode, int(_5d))

                        x_tmp = x_tmp - x5d / n_part  # / 320.0
                        y_tmp = y_tmp - y5d / n_part
                        if mesh_level != 'quarter':  # quarter mesh base = eighth mesh
                            n_part = n_part * 2
                            x6d = math.floor(x_tmp * n_part)
                            y6d = math.floor(y_tmp * n_part)
                            _6d = x6d + y6d * 2 + 1
                            meshcode = '{}{}'.format(meshcode, int(_6d))

            else:  # ext 2nd mesh base
                if mesh_level == '5x':  # 5x mesh
                    n_part = n_part * 2
                    x3d = math.floor(x_tmp * n_part)
                    y3d = math.floor(y_tmp * n_part)
                    _3d = x3d + y3d * 2 + 1
                    meshcode = '{}{}'.format(meshcode, int(_3d))

                elif mesh_level == '2x':  # 2x mesh
                    n_part = n_part * 5
                    x3d = math.floor(x_tmp * n_part) * 2
                    y3d = math.floor(y_tmp * n_part) * 2
                    meshcode = '{}{}{}5'.format(meshcode, int(y3d), int(x3d))

        return meshcode

    else:
        print('invalid mesh_level: {}'.format(mesh_level))
        return 0


def get_unitsize(mesh_level):
    # 東西南北端の座標
    if mesh_level in (1, 2, 3, '5x', '2x', 'half', 'quarter', 'eighth'):  # 1st mesh base
        unitlon = 1.0
        unitlat = 1.0 / 1.5
        if mesh_level != 1:  # 2nd mesh base
            unitlon = unitlon / 8
            unitlat = unitlat / 8
            if mesh_level not in (2, '5x', '2x'):  # 3rd mesh base
                unitlon = unitlon / 10
                unitlat = unitlat / 10
                if mesh_level != 3:  # ext 3rd mesh base
                    unitlon = unitlon / 2
                    unitlat = unitlat / 2
                    if mesh_level != 'half':  # half mesh base
                        unitlon = unitlon / 2
                        unitlat = unitlat / 2
                        if mesh_level != 'quarter':  # quarter mesh base = eighth mesh
                            unitlon = unitlon / 2
                            unitlat = unitlat / 2
            else:  # ext 2nd mesh base
                if mesh_level == '5x':  # 5x mesh
                    unitlon = unitlon / 2
                    unitlat = unitlat / 2
                elif mesh_level == '2x':  # 2x mesh
                    unitlon = unitlon / 5
                    unitlat = unitlat / 5

        return unitlon, unitlat

    else:
        print('invalid mesh_level: {}'.format(mesh_level))
        return 0


# 基準地域メッシュのレベル判別
def detect_meshlevel(meshcode):
    # メッシュレベルの自動判別
    if len(meshcode) == 4:
        mesh_level = 1
    elif len(meshcode) == 6:
        mesh_level = 2
    elif len(meshcode) == 8:
        mesh_level = 3
    elif len(meshcode) == 7:
        mesh_level = '5x'
    elif len(meshcode) == 9:
        if meshcode[-1] == '5':
            mesh_level = '2x'
        elif meshcode[-1] in ('1', '2', '3', '4'):
            mesh_level = 'half'
        else:
            mesh_level = 0
    elif len(meshcode) == 10:
        mesh_level = 'quarter'
    elif len(meshcode) == 11:
        mesh_level = 'eighth'
    else:
        mesh_level = 0

    if mesh_level == 0:
        print('invalid meshcode moge: {}'.format(meshcode))

    return mesh_level


# メッシュコードの分割
def split_meshcode(meshcode):
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

    # コードの分割
    if mesh_level != 0:  # 1st mesh base
        y1d = int(meshcode[0:2])
        x1d = int(meshcode[2:4])
        if mesh_level != 1:  # 2nd mesh base
            y2d = int(meshcode[4])
            x2d = int(meshcode[5])
            if mesh_level not in (2, '5x', '2x'):  # 3rd mesh base
                y3d = int(meshcode[6])
                x3d = int(meshcode[7])
                if mesh_level != 3:  # ext 3rd mesh base
                    _4d = int(meshcode[8])
                    if mesh_level != 'half':  # half mesh base
                        _5d = int(meshcode[9])
                        if mesh_level != 'quarter':  # quarter mesh base
                            _6d = int(meshcode[10])
            else:  # ext 2nd mesh base
                if mesh_level == '5x':  # 5x mesh
                    _3d = int(meshcode[6])
                elif mesh_level == '2x':  # 2x mesh
                    _3d = int(meshcode[6])
                    _4d = int(meshcode[7])

    return mesh_level, x1d, x2d, x3d, y1d, y2d, y3d, _3d, _4d, _5d, _6d


def get_stdmeshcode2wkt(meshcode):
    """
    メッシュコードからメッシュのWKTと端点座標を返す。
    2017/07/28 メッシュレベルの自動判別を追加
    ref: http://www.gikosha.co.jp/fig_blog/mesh.html
    """

    # コード分割
    (mesh_level, x1d, x2d, x3d, y1d, y2d, y3d, _3d, _4d, _5d, _6d) = split_meshcode(meshcode)

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
            if mesh_level not in (2, '5x', '2x'):  # 3rd mesh base
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
                    if mesh_level != 'half':  # half mesh base
                        unitlon = unitlon / 2
                        unitlat = unitlat / 2
                        if _5d in (2, 4):
                            minlon = minlon + unitlon
                        if _5d in (3, 4):
                            minlat = minlat + unitlat
                        if mesh_level != 'quarter':  # quarter mesh base = eighth mesh
                            unitlon = unitlon / 2
                            unitlat = unitlat / 2
                            if _6d in (2, 4):
                                minlon = minlon + unitlon
                            if _6d in (3, 4):
                                minlat = minlat + unitlat
            else:  # ext 2nd mesh base
                if mesh_level == '5x':  # 5x mesh
                    unitlon = unitlon / 2
                    unitlat = unitlat / 2
                    if _3d in (2, 4):
                        minlon = minlon + unitlon
                    if _3d in (3, 4):
                        minlat = minlat + unitlat
                elif mesh_level == '2x':  # 2x mesh
                    unitlon = unitlon / 5
                    unitlat = unitlat / 5
                    minlon = minlon + unitlon * _4d / 2
                    minlat = minlat + unitlat * _3d / 2
        maxlon = minlon + unitlon
        maxlat = minlat + unitlat

        # wkt
        wkt = 'POLYGON(({0} {1}, {0} {3}, {2} {3}, {2} {1}, {0} {1}))'.format(minlon, minlat, maxlon, maxlat)
        return wkt, (minlon, minlat), (maxlon, maxlat)

    else:
        print('invalid meshcode hoge: {}'.format(meshcode))
        return 0
