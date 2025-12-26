from __future__ import annotations

import os
from pathlib import Path
from ansible.plugins.callback import CallbackBase


class CallbackModule(CallbackBase):
    CALLBACK_VERSION = 2.0
    CALLBACK_TYPE = 'notification'
    CALLBACK_NAME = 'last_failed'
    CALLBACK_NEEDS_WHITELIST = False

    def __init__(self):
        super().__init__()
        # Write to installer logs dir
        self._out_dir = Path(__file__).resolve().parents[1] / "logs"
        self._out_file = self._out_dir / "last_failed_task.txt"
        try:
            os.makedirs(self._out_dir, exist_ok=True)
        except Exception:
            pass

    def _write_last_failed(self, result):
        try:
            task_name = result._task.get_name()  # noqa
            task_path = getattr(result._task, "get_path", lambda: None)()  # noqa
            lineno = getattr(result._task, "get_lineno", lambda: None)()  # noqa
            role_name = None
            if task_path and "/roles/" in task_path:
                try:
                    role_segment = task_path.split("/roles/")[1]
                    role_name = role_segment.split("/")[0]
                except Exception:
                    role_name = None
            host = getattr(result, "_host", None)
            host_name = getattr(host, "name", "unknown") if host else "unknown"
            line = f"{task_name}\n# host: {host_name}\n"
            if task_path:
                line += f"# file: {task_path}\n"
            if lineno:
                line += f"# line: {lineno}\n"
            if role_name:
                line += f"# role: {role_name}\n"
            with open(self._out_file, "w", encoding="utf-8") as f:
                f.write(line)
        except Exception:
            # Best effort only; never break the run
            pass

    def v2_runner_on_failed(self, result, ignore_errors=False):
        self._write_last_failed(result)

    def v2_runner_on_unreachable(self, result):
        self._write_last_failed(result)


