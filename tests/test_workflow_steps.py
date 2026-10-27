"""The workflow's step runner: criticality, and subprocess output reaching the log."""

import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

import run_daily_workflow as wf


class RunStepTests(unittest.TestCase):
    def boom(self):
        raise RuntimeError("down")

    def test_a_critical_failure_stops_the_workflow(self):
        with self.assertLogs(wf.logger, "ERROR"), self.assertRaises(wf.StepFailed):
            wf.run_step("critical", self.boom)

    def test_a_non_critical_failure_returns_the_default(self):
        with self.assertLogs(wf.logger, "WARNING") as logs:
            result = wf.run_step("optional", self.boom, critical=False, default={})
        self.assertEqual(result, {})
        self.assertIn("down", "\n".join(logs.output))

    def test_a_step_returns_its_result(self):
        with self.assertLogs(wf.logger, "INFO"):
            self.assertEqual(wf.run_step("ok", lambda: 42), 42)


class RunScriptTests(unittest.TestCase):
    def script(self, body):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        path = Path(tmp.name) / "step.py"
        path.write_text(body, encoding="utf-8")
        return str(path)

    def test_stdout_and_stderr_reach_the_log(self):
        path = self.script("import sys\nprint('to stdout')\nprint('to stderr', file=sys.stderr)\n")
        with self.assertLogs(wf.logger, "INFO") as logs:
            wf.run_script(path)
        text = "\n".join(logs.output)
        self.assertIn("to stdout", text)
        self.assertIn("to stderr", text)

    def test_a_failing_script_raises_with_its_traceback_logged(self):
        path = self.script("raise SystemExit('fatal: no SPY')\n")
        with self.assertLogs(wf.logger, "INFO") as logs, \
             self.assertRaises(subprocess.CalledProcessError):
            wf.run_script(path)
        self.assertIn("fatal: no SPY", "\n".join(logs.output))

    def test_scripts_run_with_this_interpreter(self):
        path = self.script("import sys\nprint(sys.executable)\n")
        with self.assertLogs(wf.logger, "INFO") as logs:
            wf.run_script(path)
        self.assertIn(sys.executable, "\n".join(logs.output))


if __name__ == "__main__":
    unittest.main()
