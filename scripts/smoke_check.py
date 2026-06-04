#!/usr/bin/env python3
"""Lightweight release smoke check.

This script avoids SLURM/SSH collection. It verifies imports, configuration
sanity, and the basic login/authentication flow.
"""
import os
import py_compile
import sys
import tempfile
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


def compile_sources():
    for rel in ("app.py", "collector.py", "config.py", "start.py", "stop.py"):
        py_compile.compile(str(ROOT / rel), doraise=True)


def configure_test_env(tmpdir: str):
    os.environ["DASHBOARD_PASSWORD"] = "smoke-test-password"
    os.environ["DASHBOARD_PORT"] = "9000"
    os.environ["DASHBOARD_HOST"] = "127.0.0.1"
    os.environ["DASHBOARD_FILE_BROWSER_ROOT"] = tmpdir


def check_config_defaults():
    import config

    assert config.ACCESS_PASSWORD == "smoke-test-password"
    assert config.PORT == 9000
    assert config.HOST == "127.0.0.1"
    assert config.DEFAULT_USER_SETTINGS["historyTrackUsers"] == ""
    assert config.DEFAULT_USER_SETTINGS["clusterUsername"] == ""
    assert config.DEFAULT_USER_SETTINGS["bookmarks"] == []
    config.validate_runtime_config()


def check_auth_flow():
    from fastapi.testclient import TestClient
    import app as dashboard_app

    dashboard_app.collector.set_paused(True)
    with TestClient(dashboard_app.app) as client:
        login = client.get("/login")
        assert login.status_code == 200

        unauthorized = client.get("/api/settings")
        assert unauthorized.status_code == 401

        bad = client.post("/login", data={"password": "wrong"})
        assert bad.status_code == 401

        good = client.post(
            "/login",
            data={"password": "smoke-test-password"},
            follow_redirects=False,
        )
        assert good.status_code == 303


def main():
    compile_sources()
    with tempfile.TemporaryDirectory() as tmpdir:
        configure_test_env(tmpdir)
        check_config_defaults()
        check_auth_flow()
    print("Smoke check passed.")


if __name__ == "__main__":
    main()
