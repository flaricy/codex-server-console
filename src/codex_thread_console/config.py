from __future__ import annotations

import os
import secrets
from dataclasses import dataclass
from pathlib import Path


def default_data_dir() -> Path:
    return Path(__file__).resolve().parents[2] / ".data"


def _secure_dir(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True, mode=0o700)
    path.chmod(0o700)
    return path


@dataclass(slots=True)
class Settings:
    workspace_root: Path
    data_dir: Path
    host: str = "127.0.0.1"
    port: int = 8765
    session_token: str = ""
    codex_bin: Path | None = None

    @classmethod
    def from_env(cls) -> "Settings":
        workspace = Path(
            os.environ.get("CODEX_CONSOLE_WORKSPACE_ROOT", os.getcwd())
        ).expanduser().resolve()
        data = Path(
            os.environ.get(
                "CODEX_CONSOLE_DATA_DIR",
                default_data_dir(),
            )
        ).expanduser().resolve()
        codex_bin_raw = os.environ.get("CODEX_CONSOLE_CODEX_BIN")
        return cls(
            workspace_root=workspace,
            data_dir=_secure_dir(data),
            host=os.environ.get("CODEX_CONSOLE_HOST", "127.0.0.1"),
            port=int(os.environ.get("CODEX_CONSOLE_PORT", "8765")),
            session_token=secrets.token_urlsafe(32),
            codex_bin=(
                Path(codex_bin_raw).expanduser().resolve()
                if codex_bin_raw
                else None
            ),
        )

    @property
    def database_path(self) -> Path:
        return self.data_dir / "queue.sqlite3"

    @property
    def token_path(self) -> Path:
        return self.data_dir / "session-token"

    @property
    def lock_path(self) -> Path:
        return self.data_dir / "server.lock"

    @property
    def origin(self) -> str:
        return f"http://{self.host}:{self.port}"

    def write_token_file(self) -> None:
        self.token_path.write_text(self.session_token, encoding="utf-8")
        self.token_path.chmod(0o600)

    def validate_cwd(self, raw: str | Path) -> Path:
        path = Path(raw).expanduser().resolve(strict=True)
        if not path.is_dir() or not path.is_relative_to(self.workspace_root):
            raise ValueError(
                f"working directory must be inside {self.workspace_root}"
            )
        return path
