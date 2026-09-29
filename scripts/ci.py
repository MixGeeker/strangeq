#!/usr/bin/env python3
"""统一执行 Go 检查，保存完整事件和明确的失败／跳过清单。"""
import argparse
import json
import os
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
MODULE = ROOT / "src/amqp-go"


def check():
    paths = subprocess.check_output(["git", "ls-files", "*.go"], cwd=MODULE, text=True).splitlines()
    paths = [p for p in paths if not p.startswith(("examples/", "testdata/"))]
    changed = []
    for start in range(0, len(paths), 50):
        changed += subprocess.check_output(["gofmt", "-l", *paths[start:start + 50]], cwd=MODULE, text=True).splitlines()
    # Windows checkout 可采用 CRLF；仅换行差异不作为 Go 格式错误。
    changed = [path for path in changed if subprocess.check_output(["gofmt", path], cwd=MODULE)
               != (MODULE / path).read_bytes().replace(b"\r\n", b"\n")]
    if changed:
        raise RuntimeError("gofmt required:\n" + "\n".join(changed))
    for command in (["go", "mod", "verify"], ["go", "vet", "./..."]):
        subprocess.run(command, cwd=MODULE, check=True)


def tests(args):
    directory = Path(args.output).resolve()
    directory.mkdir(parents=True, exist_ok=True)
    options = {
        "unit": ["-short", "./..."],
        "race": ["-race", "-short", "-coverprofile=" + str(directory / "coverage.out"), "-covermode=atomic", "./..."],
        "full": ["./..."],
        "conformance": ["-tags=conformance", "-run=TestConformance", ".", "./server"],
        "upgrade": ["-run=TestUpgradeAndColdRestore", "./integration/upgrade"],
        "migration": ["-run=TestImportMigrationFixture", "./integration/upgrade"],
    }
    if args.suite == "upgrade":
        for name in ("STRANGEQ_BASELINE_BINARY", "STRANGEQ_CANDIDATE_BINARY", "STRANGEQ_UPGRADE_REPORT"):
            if not os.environ.get(name):
                raise ValueError("upgrade test input missing: " + name)
    if args.suite == "migration":
        for name in ("STRANGEQ_MIGRATION_SOURCE", "STRANGEQ_CANDIDATE_BINARY", "STRANGEQ_MIGRATION_REPORT"):
            if not os.environ.get(name):
                raise ValueError("migration test input missing: " + name)
    env = dict(os.environ, SKIP_RABBITMQ_TESTS="1", STRANGEQ_REQUIRE_UPGRADE="1" if args.suite == "upgrade" else "0",
               STRANGEQ_REQUIRE_MIGRATION="1" if args.suite == "migration" else "0")
    command = ["go", "test", "-json", "-count=1", "-timeout=15m", *options[args.suite]]
    process = subprocess.Popen(command, cwd=MODULE, env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, encoding="utf-8", errors="replace")
    result = {"suite": args.suite, "command": command, "passed": 0, "failed": [], "skipped": [], "packages": 0}
    messages = {}
    with (directory / (args.suite + ".jsonl")).open("w", encoding="utf-8") as output:
        for line in process.stdout:
            output.write(line)
            try:
                event = json.loads(line)
            except ValueError:
                print(line.rstrip())
                continue
            identity = event.get("Package", "") + "/" + event.get("Test", "")
            if event.get("Action") == "output":
                messages.setdefault(identity, []).append(event.get("Output", ""))
                messages[identity] = messages[identity][-12:]
            if event.get("Action") in ("skip", "fail"):
                target = "skipped" if event["Action"] == "skip" else "failed"
                result[target].append({"test": identity, "output": "".join(messages.get(identity, []))})
                print(event["Action"] + ": " + identity)
            if event.get("Action") == "pass":
                if "Test" in event:
                    result["passed"] += 1
                else:
                    result["packages"] += 1
                    print("pass: " + event.get("Package", ""), flush=True)
    result["exitCode"] = process.wait()
    if args.suite in ("upgrade", "migration") and (result["passed"] == 0 or result["skipped"]):
        result["exitCode"] = result["exitCode"] or 1
    (directory / (args.suite + "-summary.json")).write_text(json.dumps(result, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    if os.environ.get("GITHUB_STEP_SUMMARY"):
        with open(os.environ["GITHUB_STEP_SUMMARY"], "a", encoding="utf-8") as output:
            output.write(f"### {args.suite}\n\nPassed: {result['passed']}; skipped: {len(result['skipped'])}; exit: {result['exitCode']}\n\n")
            for item in result["failed"] + result["skipped"]:
                output.write("- `" + item["test"].replace("`", "") + "`\n")
    return result["exitCode"]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("suite", choices=["check", "unit", "race", "full", "conformance", "upgrade", "migration"])
    parser.add_argument("--output", default="dist/test-results")
    args = parser.parse_args()
    if args.suite == "check":
        check()
        return 0
    return tests(args)


if __name__ == "__main__":
    sys.exit(main())
