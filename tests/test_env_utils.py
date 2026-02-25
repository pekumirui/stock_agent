"""env_utils.py のユニットテスト"""
import os
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "scripts"))

from env_utils import load_env


class TestLoadEnv:
    def test_loads_variables(self, tmp_path, monkeypatch):
        """基本的な環境変数読み込み"""
        env_file = tmp_path / ".env"
        env_file.write_text("FOO=bar\nBAZ=qux\n")
        monkeypatch.delenv("FOO", raising=False)
        monkeypatch.delenv("BAZ", raising=False)

        load_env(tmp_path)

        assert os.environ["FOO"] == "bar"
        assert os.environ["BAZ"] == "qux"

    def test_does_not_overwrite_existing(self, tmp_path, monkeypatch):
        """既存の環境変数を上書きしない"""
        env_file = tmp_path / ".env"
        env_file.write_text("MY_VAR=new_value\n")
        monkeypatch.setenv("MY_VAR", "existing_value")

        load_env(tmp_path)

        assert os.environ["MY_VAR"] == "existing_value"

    def test_missing_env_file(self, tmp_path):
        """存在しない.envファイルではエラーにならない"""
        load_env(tmp_path)  # no .env file

    def test_ignores_comments_and_blank_lines(self, tmp_path, monkeypatch):
        """コメント行と空行を無視する"""
        env_file = tmp_path / ".env"
        env_file.write_text("# comment\n\nKEY1=val1\n  # another comment\n")
        monkeypatch.delenv("KEY1", raising=False)

        load_env(tmp_path)

        assert os.environ["KEY1"] == "val1"

    def test_strips_quotes(self, tmp_path, monkeypatch):
        """値のクォートを除去する"""
        env_file = tmp_path / ".env"
        env_file.write_text('DOUBLE="hello"\nSINGLE=\'world\'\n')
        monkeypatch.delenv("DOUBLE", raising=False)
        monkeypatch.delenv("SINGLE", raising=False)

        load_env(tmp_path)

        assert os.environ["DOUBLE"] == "hello"
        assert os.environ["SINGLE"] == "world"

    def test_default_base_dir(self, monkeypatch):
        """引数なしで呼び出してもエラーにならない"""
        load_env()  # uses default base_dir
