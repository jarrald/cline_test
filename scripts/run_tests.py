"""Standard test runner: unittest + JUnit XML + markdown summary.

Usage:
    python -m scripts.run_tests --label unit --start-dir tests/unit
    python -m scripts.run_tests --label all --start-dir tests

Exit code is non-zero when anything fails, so CI can gate on it.
"""

from __future__ import annotations

import argparse
import os
import sys
import time
import unittest
import xml.etree.ElementTree as ET

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


class JUnitResult(unittest.TextTestResult):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.records: list[dict] = []
        self._started: dict[str, float] = {}

    def startTest(self, test: unittest.TestCase) -> None:
        self._started[test.id()] = time.monotonic()
        super().startTest(test)

    def _record(self, test: unittest.TestCase, outcome: str, message: str = "") -> None:
        duration = time.monotonic() - self._started.get(test.id(), time.monotonic())
        self.records.append(
            {
                "id": test.id(),
                "class": type(test).__module__ + "." + type(test).__qualname__,
                "name": getattr(test, "_testMethodName", test.id()),
                "time": round(duration, 4),
                "outcome": outcome,
                "message": message,
            }
        )

    def addSuccess(self, test: unittest.TestCase) -> None:
        super().addSuccess(test)
        self._record(test, "passed")

    def addFailure(self, test: unittest.TestCase, err) -> None:
        super().addFailure(test, err)
        self._record(test, "failed", self._exc_info_to_string(err, test))

    def addError(self, test: unittest.TestCase, err) -> None:
        super().addError(test, err)
        self._record(test, "error", self._exc_info_to_string(err, test))

    def addSkip(self, test: unittest.TestCase, reason: str) -> None:
        super().addSkip(test, reason)
        self._record(test, "skipped", reason)

    def addExpectedFailure(self, test: unittest.TestCase, err) -> None:
        super().addExpectedFailure(test, err)
        self._record(test, "expected_failure", self._exc_info_to_string(err, test))

    def addUnexpectedSuccess(self, test: unittest.TestCase) -> None:
        super().addUnexpectedSuccess(test)
        self._record(test, "unexpected_success")


def write_junit(path: str, records: list[dict], suite_name: str) -> None:
    suites = ET.Element("testsuites", name=suite_name)
    grouped: dict[str, list[dict]] = {}
    for record in records:
        grouped.setdefault(record["class"], []).append(record)
    totals = {"tests": 0, "failures": 0, "errors": 0, "skipped": 0, "time": 0.0}
    for classname, cases in grouped.items():
        suite = ET.SubElement(
            suites,
            "testsuite",
            name=classname,
            tests=str(len(cases)),
            failures=str(sum(1 for c in cases if c["outcome"] in {"failed", "unexpected_success"})),
            errors=str(sum(1 for c in cases if c["outcome"] == "error")),
            skipped=str(sum(1 for c in cases if c["outcome"] == "skipped")),
            time=f"{sum(c['time'] for c in cases):.3f}",
        )
        totals["tests"] += len(cases)
        totals["failures"] += int(suite.get("failures"))
        totals["errors"] += int(suite.get("errors"))
        totals["skipped"] += int(suite.get("skipped"))
        totals["time"] += sum(c["time"] for c in cases)
        for case in cases:
            element = ET.SubElement(suite, "testcase", classname=classname, name=case["name"], time=f"{case['time']:.3f}")
            if case["outcome"] in {"failed", "unexpected_success"}:
                ET.SubElement(element, "failure", message=case["outcome"]).text = case["message"][-4000:]
            elif case["outcome"] == "error":
                ET.SubElement(element, "error", message="error").text = case["message"][-4000:]
            elif case["outcome"] == "skipped":
                ET.SubElement(element, "skipped", message=case["message"])
    suites.set("tests", str(totals["tests"]))
    suites.set("failures", str(totals["failures"]))
    suites.set("errors", str(totals["errors"]))
    suites.set("skipped", str(totals["skipped"]))
    suites.set("time", f"{totals['time']:.3f}")
    os.makedirs(os.path.dirname(path), exist_ok=True)
    ET.ElementTree(suites).write(path, encoding="utf-8", xml_declaration=True)


def write_summary(path: str, records: list[dict], label: str, duration: float) -> None:
    passed = sum(1 for r in records if r["outcome"] == "passed")
    failed = sum(1 for r in records if r["outcome"] in {"failed", "error", "unexpected_success"})
    skipped = sum(1 for r in records if r["outcome"] == "skipped")
    lines = [
        f"# Test run - {label}",
        "",
        f"* duration: **{duration:.1f}s**",
        f"* tests: **{len(records)}** (passed {passed}, failed {failed}, skipped {skipped})",
        "",
        "| Test | Outcome | Time |",
        "|---|---|---|",
    ]
    for record in records:
        lines.append(f"| `{record['class'].split('.')[-1]}.{record['name']}` | {record['outcome']} | {record['time'] * 1000:.0f} ms |")
    if failed:
        lines += ["", "## Failures", ""]
        for record in records:
            if record["outcome"] in {"failed", "error", "unexpected_success"}:
                lines += [f"### {record['name']}", "", "```", record["message"][-3000:], "```", ""]
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as handle:
        handle.write("\n".join(lines) + "\n")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="run test suites with JUnit output")
    parser.add_argument("--start-dir", action="append", default=["tests"])
    parser.add_argument("--pattern", default="test_*.py")
    parser.add_argument("--label", default="all")
    parser.add_argument("--reports-dir", default=os.path.join(REPO_ROOT, "runtime", "test-reports"))
    parser.add_argument("--verbosity", type=int, default=2)
    args = parser.parse_args(argv)

    loader = unittest.TestLoader()
    suite = unittest.TestSuite()
    for start_dir in args.start_dir:
        suite.addTests(loader.discover(start_dir=start_dir, pattern=args.pattern, top_level_dir=REPO_ROOT))

    started = time.monotonic()
    runner = unittest.TextTestRunner(stream=sys.stdout, verbosity=args.verbosity, resultclass=JUnitResult)
    result = runner.run(suite)
    duration = time.monotonic() - started
    records = getattr(result, "records", [])
    write_junit(os.path.join(args.reports_dir, f"{args.label}-junit.xml"), records, args.label)
    write_summary(os.path.join(args.reports_dir, f"{args.label}-summary.md"), records, args.label, duration)
    print(f"\n{args.label}: {len(records)} tests in {duration:.1f}s -> {args.reports_dir}")
    return 0 if result.wasSuccessful() else 1


if __name__ == "__main__":
    raise SystemExit(main())
