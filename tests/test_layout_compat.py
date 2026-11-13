"""配置換え（src/mod_spatialite/ パッケージ化）後も、従来の import の書き方が使えることを確かめる。"""


def test_public_names_from_package() -> None:
    import mod_spatialite
    from mod_spatialite import GTBL, SpatiaLiteConnection

    assert mod_spatialite.SpatiaLiteConnection is SpatiaLiteConnection
    assert mod_spatialite.GTBL is GTBL
    assert set(mod_spatialite.__all__) == {"GTBL", "SpatiaLiteConnection"}


def test_legacy_mod_standardmesh_reexports_same_objects() -> None:
    import mod_standardmesh
    from mod_spatialite import standardmesh

    l_public = [s for s in dir(standardmesh) if not s.startswith("_") and callable(getattr(standardmesh, s))]
    assert l_public
    for s_name in l_public:
        assert getattr(mod_standardmesh, s_name) is getattr(standardmesh, s_name), s_name


def test_legacy_from_import() -> None:
    from mod_standardmesh import get_unitsize

    from mod_spatialite.standardmesh import get_unitsize as get_unitsize_new

    assert get_unitsize is get_unitsize_new
