# WSL 疎通スモークテスト（Phase 1）
# 実行: .venv/bin/python tests/test_smoke.py （pytest でも実行可）
# 複製 DB（rebuild/_data/pyobjects.sqlite3）が存在する場合のみ実 DB 接続も検証する。
from pathlib import Path

# rebuild/_data は git 管理外（CLAUDE.md 第2章）。リポジトリ外のためテストからは相対で参照する。
P_REPLICA_DB: Path = Path(__file__).resolve().parents[2] / "_data" / "pyobjects.sqlite3"


def test_import() -> None:
    import mod_spatialite
    import mod_standardmesh

    assert hasattr(mod_spatialite, "SpatiaLiteConnection")
    assert hasattr(mod_spatialite, "GTBL")


def test_memory_connection() -> None:
    from mod_spatialite import SpatiaLiteConnection

    con = SpatiaLiteConnection(":memory:")
    s_ver: str = con.get_spatialite_version()
    assert s_ver, "spatialite_version() returned empty"

    s_wkt: str = con.execute(
        "SELECT ST_AsText(MakePoint(141.35, 43.06, 4326));"
    ).fetchone()[0]
    assert s_wkt == "POINT(141.35 43.06)"
    con.close()


def test_replica_db_connection() -> None:
    from mod_spatialite import SpatiaLiteConnection

    if not P_REPLICA_DB.is_file():
        print(f"skip: replica db not found: {P_REPLICA_DB}")
        return

    con = SpatiaLiteConnection(str(P_REPLICA_DB))
    l_tables: list[str] = [
        row["name"]
        for row in con.execute("SELECT name FROM sqlite_master WHERE type='table';")
    ]
    assert "pyobjects" in l_tables, f"pyobjects table not found in {P_REPLICA_DB}"

    s_wkt: str = con.execute(
        "SELECT ST_AsText(ST_Transform(MakePoint(141.35, 43.06, 4326), 3100));"
    ).fetchone()[0]
    assert s_wkt.startswith("POINT("), f"ST_Transform failed: {s_wkt}"
    con.close()


def test_standardmesh() -> None:
    import mod_standardmesh

    l_funcs: list[str] = [s for s in dir(mod_standardmesh) if not s.startswith("_")]
    assert l_funcs, "mod_standardmesh has no public functions"


def test_invalid_arg_messages() -> None:
    # 不正な引数の分岐が NameError／TypeError にならず、メッセージを出して 0 を返すこと
    from mod_spatialite import SpatiaLiteConnection

    assert SpatiaLiteConnection.get_tblname(123) == 0
    assert SpatiaLiteConnection.get_tblname_and_gc(123) == 0
    assert SpatiaLiteConnection.get_tblname("tbl_x") == "tbl_x"
    assert SpatiaLiteConnection.get_tblname_and_gc("tbl_x", "pt_4612") == ("tbl_x", "pt_4612")

    # get_method_name は呼び出し元のメソッド名を返す（インスタンスからも呼べる）
    con = SpatiaLiteConnection(":memory:")

    def _caller() -> str:
        return con.get_method_name()

    assert _caller() == "_caller"
    con.close()


if __name__ == "__main__":
    test_import()
    test_memory_connection()
    test_replica_db_connection()
    test_standardmesh()
    test_invalid_arg_messages()
    print("all smoke tests passed.")
