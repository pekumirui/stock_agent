"""環境変数読み込みユーティリティ"""
import os
from pathlib import Path

_DEFAULT_BASE_DIR = Path(__file__).parent.parent


def load_env(base_dir: Path = None) -> None:
    """プロジェクトルートの.envファイルから環境変数を読み込む。

    既存の環境変数は上書きしない（os.environ.setdefault を使用）。

    Args:
        base_dir: プロジェクトルートのパス。省略時は scripts/ の親ディレクトリ。
    """
    if base_dir is None:
        base_dir = _DEFAULT_BASE_DIR
    env_path = base_dir / ".env"
    if not env_path.exists():
        return
    with open(env_path, encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            if '=' in line:
                key, _, value = line.partition('=')
                key = key.strip()
                value = value.strip().strip('"').strip("'")
                os.environ.setdefault(key, value)
