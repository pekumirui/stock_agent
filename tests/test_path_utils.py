"""
path_utils.py のテスト
"""
import tempfile
from pathlib import Path

import pytest

from path_utils import find_edinet_temp_dir


def test_find_edinet_temp_dir_with_edinet_prefix():
    """edinet_プレフィックスがある場合、そのディレクトリを返すこと。"""
    path = Path(tempfile.gettempdir()) / "edinet_abc123" / "XBRL" / "PublicDoc" / "sample.htm"
    result = find_edinet_temp_dir([path])
    assert result.name == "edinet_abc123"


def test_find_edinet_temp_dir_without_edinet_prefix():
    """edinet_プレフィックスがない場合、/tmp直下手前まで遡ること。"""
    base = Path(tempfile.gettempdir()) / "work" / "nested"
    path = base / "PublicDoc" / "sample.htm"
    result = find_edinet_temp_dir([path])
    assert result == Path(tempfile.gettempdir()) / "work"


def test_find_edinet_temp_dir_empty_input():
    """空リストは ValueError を返すこと。"""
    with pytest.raises(ValueError):
        find_edinet_temp_dir([])
