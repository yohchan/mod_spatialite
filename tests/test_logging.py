"""print をやめて logging で出すことの確認（import 時に何も出さない／失敗の分岐は WARNING）。"""

import logging
import subprocess
import sys


def test_import_prints_nothing() -> None:
    o_res = subprocess.run(
        [sys.executable, "-c", "import mod_spatialite, mod_standardmesh"],
        capture_output=True,
        text=True,
        check=True,
    )
    assert o_res.stdout == ""
    assert o_res.stderr == ""


def test_invalid_argument_logs_warning(caplog) -> None:
    from mod_spatialite import standardmesh

    with caplog.at_level(logging.WARNING, logger="mod_spatialite"):
        standardmesh.get_unitsize("no_such_level")
    assert any(
        r.levelno == logging.WARNING and r.name.startswith("mod_spatialite")
        for r in caplog.records
    )


def test_connection_messages_are_info_not_stdout(capsys, caplog) -> None:
    from mod_spatialite import SpatiaLiteConnection

    with caplog.at_level(logging.INFO, logger="mod_spatialite"):
        con = SpatiaLiteConnection(":memory:")
        con.close()
    assert capsys.readouterr().out == ""
    assert any(r.levelno == logging.INFO for r in caplog.records)
