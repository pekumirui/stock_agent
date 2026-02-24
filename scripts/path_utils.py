"""
パス操作の共通ユーティリティ
"""
import tempfile
from pathlib import Path


def find_edinet_temp_dir(extracted_paths: list[Path]) -> Path:
    """extract_edinet_zip() が生成した一時ディレクトリのルートを特定する。"""
    if not extracted_paths:
        raise ValueError("extracted_paths must not be empty")

    first_path = extracted_paths[0]
    temp_dir = first_path

    while temp_dir.parent != temp_dir and not str(temp_dir.name).startswith("edinet_"):
        temp_dir = temp_dir.parent

    # edinet_ プレフィックスが見つからない場合は /tmp 直下まで遡る
    if not str(temp_dir.name).startswith("edinet_"):
        temp_dir = first_path
        while temp_dir.parent != temp_dir and temp_dir.parent != Path(tempfile.gettempdir()):
            temp_dir = temp_dir.parent

    return temp_dir
