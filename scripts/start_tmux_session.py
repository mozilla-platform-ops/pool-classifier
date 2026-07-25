#!/usr/bin/env python3
"""start_tmux_session.py — class to launch multi-window tmux sessions.

Specify number of windows, working directory, and per-window commands.
- Exits if session already exists, to avoid accidentally stomping on an active session.
- If no session exists, creates it and populates all windows with the specified commands.

To attach to the session, run `tmux attach-session -t <session_name>`.
"""

from __future__ import annotations

import subprocess
import sys
from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class PaneSpec:
    """Specification for a single tmux window."""

    name: str
    commands: list[str] = field(default_factory=list)


class TmuxLauncher:
    """Create and populate a tmux session with multiple windows.

    Args:
        session_name: Name for the tmux session.
        num_panes: Total number of windows to create.
        root: Working directory for the session.
        default_shell: Shell to use in each window (default: zsh).
        panes: Per-window specs (name + commands). Extras beyond num_panes
               are ignored; windows without a spec get a generic shell window.
    """

    def __init__(
        self,
        session_name: str,
        num_panes: int,
        root: str | Path,
        *,
        default_shell: str = "zsh",
        panes: list[PaneSpec] | None = None,
    ) -> None:
        self.session_name = session_name
        self.num_panes = num_panes
        self.root = Path(root).expanduser()
        self.default_shell = default_shell
        self.panes = panes or []

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _run(
        self, *args: str, capture_output: bool = False
    ) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            list(args),
            check=True,
            capture_output=capture_output,
            text=True,
        )

    def _send(self, target: str, text: str, *, enter: bool = True) -> None:
        keys = [text, "C-m"] if enter else [text]
        self._run("tmux", "send-keys", "-t", target, *keys)

    def _session_exists(self) -> bool:
        result = subprocess.run(
            ["tmux", "list-sessions"],  # noqa: S607
            capture_output=True,
            text=True,
            check=False,
        )
        return self.session_name in result.stdout

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def launch(self) -> None:
        """Create the session and populate all windows. Exits if session exists."""
        if self._session_exists():
            print(
                f"tmux session '{self.session_name}' already exists. Exiting.",
                file=sys.stderr,
            )
            sys.exit(1)

        print(f"* No tmux session '{self.session_name}' detected. Starting...")

        # Capture stable tmux window IDs so user base-index settings do not matter.
        result = self._run(
            "tmux",
            "new-session",
            "-d",
            "-s",
            self.session_name,
            "-c",
            str(self.root),
            "-P",
            "-F",
            "#{window_id}",
            capture_output=True,
        )
        window_targets = [result.stdout.strip()]

        # Create remaining windows.
        for _ in range(1, self.num_panes):
            result = self._run(
                "tmux",
                "new-window",
                "-t",
                self.session_name,
                "-c",
                str(self.root),
                "-P",
                "-F",
                "#{window_id}",
                capture_output=True,
            )
            window_targets.append(result.stdout.strip())

        # Configure each window.
        for i, target in enumerate(window_targets):
            spec = (
                self.panes[i]
                if i < len(self.panes)
                else PaneSpec(name=self.default_shell)
            )
            self._run(
                "tmux",
                "rename-window",
                "-t",
                target,
                spec.name,
            )
            for cmd in spec.commands:
                self._send(target, cmd)

        # Return focus to the first window so the user lands there on attach.
        self._run("tmux", "select-window", "-t", window_targets[0])

        print(
            f"\nTo attach to the tmux session, run:\n   tmux attach-session -t {self.session_name}"
        )


# ---------------------------------------------------------------------------
# Script-specific configuration
# ---------------------------------------------------------------------------


def main() -> None:
    launcher = TmuxLauncher(
        session_name="pool-classifier",
        num_panes=3,
        root=Path(__file__).resolve().parent.parent,
        default_shell="zsh",
        panes=[
            PaneSpec(name="pool-classifier", commands=["./pc_start.sh --debug"]),
            PaneSpec(name="fetch-data", commands=["watchp -n 15 ./pc_fetch_data.sh"]),
            PaneSpec(name="shell"),
        ],
    )
    launcher.launch()


if __name__ == "__main__":
    main()
