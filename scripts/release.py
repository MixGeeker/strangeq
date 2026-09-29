#!/usr/bin/env python3
"""StrangeQ 的冻结构建与归档校验；发布工作流复用同一实现。"""
import argparse
import gzip
import hashlib
import io
import json
import os
from pathlib import Path, PurePosixPath
import re
import subprocess
import tarfile
import uuid
import zipfile

ROOT = Path(__file__).resolve().parents[1]
MODULE = ROOT / "src/amqp-go"
VERSION = re.compile(r"(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)(?:-[0-9A-Za-z.-]+)?(?:\+[0-9A-Za-z.-]+)?\Z")


def run(*args, cwd=ROOT, env=None):
    return subprocess.check_output(args, cwd=cwd, env=env, text=True, encoding="utf-8").strip()


def digest(data):
    return hashlib.sha256(data).hexdigest()


def contract():
    value = json.loads((ROOT / "ci/contract.json").read_text(encoding="utf-8"))
    if value["schemaVersion"] != 1 or not re.fullmatch(r"[a-f0-9]{40}", value["upgradeBaseline"]["commit"]):
        raise ValueError("invalid build contract")
    return value


def version(value):
    value = value.removeprefix("v")
    if not VERSION.fullmatch(value):
        raise ValueError("invalid version")
    main, _, build = value.partition("+")
    _, separator, prerelease = main.partition("-")
    if (separator and any(not part or (part.isdigit() and len(part) > 1 and part.startswith("0")) for part in prerelease.split("."))
            or "+" in value and any(not part for part in build.split("."))):
        raise ValueError("invalid version")
    return value


def environment(target):
    if target not in contract()["targets"]:
        raise ValueError("unsupported target")
    goos, goarch = target.split("-")
    env = dict(os.environ, GOOS=goos, GOARCH=goarch, CGO_ENABLED="0", GOENV="off",
               GOTOOLCHAIN="local", GOWORK="off", GOFLAGS="", GOEXPERIMENT="")
    actual = run("go", "env", "GOVERSION", env=env)
    if actual != "go" + contract()["goVersion"]:
        raise ValueError("Go toolchain does not match ci/contract.json: " + actual)
    return env


def modules(env):
    stream = run("go", "list", "-m", "-json", "all", cwd=MODULE, env=env)
    decoder, result = json.JSONDecoder(), []
    while stream.strip():
        item, end = decoder.raw_decode(stream.lstrip())
        stream = stream.lstrip()[end:]
        if not item.get("Main"):
            if item.get("Replace"):
                raise ValueError("release dependencies cannot use replace")
            result.append(item)
    return result


def notices(dependencies):
    parts = []
    for dep in dependencies:
        directory = Path(dep["Dir"])
        files = sorted(p for p in directory.iterdir() if p.is_file() and
                       p.name.lower().startswith(("license", "copying", "notice")))
        if not files:
            raise ValueError("dependency license missing: " + dep["Path"])
        parts.append("\n=== " + dep["Path"] + " " + dep["Version"] + " ===\n")
        for path in files:
            parts.append(path.name + "\n" + path.read_text(encoding="utf-8", errors="replace") + "\n")
    return "".join(parts).encode()


def archive_files(path):
    if path.suffix == ".zip":
        with zipfile.ZipFile(path) as archive:
            entries = archive.infolist()
            if any(i.is_dir() or ((i.external_attr >> 16) & 0o170000) == 0o120000 for i in entries):
                raise ValueError("unsupported ZIP member")
            if len(entries) > 128 or sum(i.file_size for i in entries) > 512 * 1024 * 1024:
                raise ValueError("archive exceeds limits")
            names = [i.orig_filename for i in entries]
            contents = [archive.read(i) for i in entries]
    else:
        with tarfile.open(path, "r:gz") as archive:
            entries = archive.getmembers()
            if len(entries) > 128 or any(not i.isfile() for i in entries) or sum(i.size for i in entries) > 512 * 1024 * 1024:
                raise ValueError("unsupported TAR members")
            names = [i.name for i in entries]
            contents = [archive.extractfile(i).read() for i in entries]
    if len(names) != len(set(n.casefold() for n in names)):
        raise ValueError("duplicate archive names")
    roots = set()
    files = {}
    for name, content in zip(names, contents):
        parts = PurePosixPath(name).parts
        if len(parts) != 2 or "\\" in name or ":" in name or any(p in (".", "..", "") for p in parts) or name.startswith("/"):
            raise ValueError("unsafe archive name")
        roots.add(parts[0])
        files[parts[1]] = content
    if len(roots) != 1:
        raise ValueError("invalid archive root")
    return roots.pop(), files


def verify(path, expected_version, commit, extract=None, allow_dirty=False):
    path = Path(path)
    checksum = path.with_name(path.name + ".sha256").read_text(encoding="utf-8").strip()
    if checksum != digest(path.read_bytes()) + "  " + path.name:
        raise ValueError("archive checksum mismatch")
    root, files = archive_files(path)
    metadata = json.loads(files["build-metadata.json"])
    if (metadata["schemaVersion"] != 1 or metadata["version"] != version(expected_version)
            or metadata["commit"] != commit or metadata["goVersion"] != contract()["goVersion"]
            or metadata["target"] not in contract()["targets"]
            or (metadata["dirty"] and not allow_dirty)):
        raise ValueError("archive identity mismatch")
    expected_root = f"strangeq-{metadata['version']}-{metadata['target']}"
    suffix = ".zip" if metadata["target"].startswith("windows-") else ".tar.gz"
    if root != expected_root or path.name != root + suffix:
        raise ValueError("archive filename mismatch")
    content_hashes = {name: digest(data) for name, data in files.items() if name != "build-metadata.json"}
    if content_hashes != metadata["files"]:
        raise ValueError("archive content mismatch")
    binary = "amqp-server.exe" if metadata["target"].startswith("windows-") else "amqp-server"
    required = {binary, "LICENSE", "THIRD_PARTY_NOTICES.txt", "config.sample.yaml", "README.md", "AUTHORIZATION.md", "TLS.md"}
    if not required.issubset(files):
        raise ValueError("archive content incomplete")
    if extract:
        destination = Path(extract).resolve()
        destination.mkdir(parents=True, exist_ok=True)
        if any(destination.iterdir()):
            raise ValueError("extraction requires an empty directory")
        for name, data in files.items():
            target = destination / name
            with target.open("xb") as output:
                output.write(data)
            if name == binary:
                target.chmod(0o755)
    return metadata


def build(args):
    selected = version(args.version)
    commit = run("git", "rev-parse", "HEAD")
    dirty = bool(run("git", "status", "--porcelain", "--untracked-files=no"))
    if dirty and not args.allow_dirty:
        raise ValueError("release build requires clean tracked files")
    env = environment(args.target)
    run("go", "mod", "download", cwd=MODULE, env=env)
    run("go", "mod", "verify", cwd=MODULE, env=env)
    output = Path(args.output).resolve()
    output.mkdir(parents=True, exist_ok=True)
    stage = output / (".stage-" + str(uuid.uuid4()))
    stage.mkdir()
    binary = "amqp-server.exe" if args.target.startswith("windows-") else "amqp-server"
    run("go", "build", "-mod=readonly", "-trimpath", "-buildvcs=false",
        "-ldflags=-s -w -buildid= -X main.version=" + selected,
        "-o", str(stage / binary), "./cmd/amqp-server", cwd=MODULE, env=env)
    linked = json.loads(run("go", "version", "-m", "-json", str(stage / binary), env=env))
    paths = {item["Path"] for item in linked["Deps"]}
    dependencies = [item for item in modules(env) if item["Path"] in paths]
    if len(dependencies) != len(paths):
        raise ValueError("linked dependency inventory incomplete")
    for item in dependencies:
        if not item.get("Dir"):
            downloaded = json.loads(run("go", "mod", "download", "-json", item["Path"] + "@" + item["Version"], cwd=MODULE, env=env))
            item["Dir"] = downloaded["Dir"]
    files = {binary: (stage / binary).read_bytes(),
             "LICENSE": (ROOT / "LICENSE").read_bytes(),
             "config.sample.yaml": (MODULE / "config.sample.yaml").read_bytes(),
             "THIRD_PARTY_NOTICES.txt": ("=== Go " + contract()["goVersion"] + " ===\n").encode()
             + (Path(run("go", "env", "GOROOT", env=env)) / "LICENSE").read_bytes() + notices(dependencies)}
    for source, name in [("docs/WINDOWS.md" if args.target.startswith("windows-") else "docs/RUNNING.md", "README.md"),
                         ("docs/AUTHORIZATION.md", "AUTHORIZATION.md"), ("docs/TLS.md", "TLS.md")]:
        files[name] = (ROOT / source).read_bytes()
    metadata = {"schemaVersion": 1, "version": selected, "commit": commit, "dirty": dirty,
                "goVersion": contract()["goVersion"], "target": args.target,
                "dependencies": [{k: dep[k] for k in ("Path", "Version", "Sum") if k in dep} for dep in dependencies],
                "files": {name: digest(data) for name, data in files.items()}}
    files["build-metadata.json"] = (json.dumps(metadata, ensure_ascii=False, indent=2) + "\n").encode()
    name = f"strangeq-{selected}-{args.target}"
    path = output / (name + (".zip" if args.target.startswith("windows-") else ".tar.gz"))
    if path.exists() or path.with_name(path.name + ".sha256").exists():
        raise ValueError("existing artifacts cannot be overwritten")
    if args.target.startswith("windows-"):
        with zipfile.ZipFile(path, "x", compression=zipfile.ZIP_DEFLATED, compresslevel=9) as archive:
            for filename, data in sorted(files.items()):
                info = zipfile.ZipInfo(name + "/" + filename, (1980, 1, 1, 0, 0, 0))
                info.compress_type = zipfile.ZIP_DEFLATED
                info.external_attr = 0o100644 << 16
                archive.writestr(info, data)
    else:
        with path.open("xb") as raw, gzip.GzipFile(fileobj=raw, mode="wb", filename="", mtime=0) as compressed:
            with tarfile.open(fileobj=compressed, mode="w") as archive:
                for filename, data in sorted(files.items()):
                    info = tarfile.TarInfo(name + "/" + filename)
                    info.size = len(data)
                    info.mode = 0o755 if filename == binary else 0o644
                    archive.addfile(info, io.BytesIO(data))
    path.with_name(path.name + ".sha256").write_text(digest(path.read_bytes()) + "  " + path.name + "\n", encoding="utf-8")
    verify(path, selected, commit, allow_dirty=args.allow_dirty)
    print(path)


def baseline(args):
    source = Path(args.source).resolve()
    expected = contract()["upgradeBaseline"]
    if run("git", "rev-parse", "HEAD", cwd=source) != expected["commit"]:
        raise ValueError("upgrade baseline commit mismatch")
    if run("git", "status", "--porcelain", "--untracked-files=no", cwd=source):
        raise ValueError("upgrade baseline is dirty")
    output = Path(args.output).resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    if output.exists():
        raise ValueError("baseline output already exists")
    run("go", "build", "-mod=readonly", "-trimpath", "-buildvcs=false",
        "-ldflags=-s -w -buildid= -X main.version=" + expected["version"],
        "-o", str(output), "./cmd/amqp-server", cwd=source / "src/amqp-go", env=environment(args.target))


def manifest(args):
    root = Path(args.directory)
    expected = set(contract()["targets"])
    found = set()
    files = {}
    for path in sorted(root.iterdir()):
        if path.name.endswith((".zip", ".tar.gz")):
            metadata = verify(path, args.version, args.commit)
            if metadata["target"] in found:
                raise ValueError("duplicate release target")
            found.add(metadata["target"])
        if path.is_file() and path.name != "release-manifest.json":
            files[path.name] = {"sha256": digest(path.read_bytes()), "size": path.stat().st_size}
    if found != expected:
        raise ValueError("release target set incomplete")
    summaries = {f"tests-{suite}-{suite}-summary.json": suite for suite in ("unit", "race", "conformance")}
    summaries.update({f"tests-{suite}-{platform}-amd64-{suite}-summary.json": suite
                      for suite in ("upgrade", "migration") for platform in ("windows", "linux")})
    for name, suite in summaries.items():
        summary = json.loads((root / name).read_text(encoding="utf-8"))
        if (summary["suite"] != suite or summary["exitCode"] != 0 or summary["passed"] <= 0
                or summary["failed"] or suite in ("upgrade", "migration") and summary["skipped"]):
            raise ValueError("required test suite did not pass: " + name)
    for platform in ("windows", "linux"):
        report = json.loads((root / f"upgrade-{platform}-amd64.json").read_text(encoding="utf-8"))
        archive = next(p for p in root.iterdir() if p.name.endswith(f"-{platform}-amd64" + (".zip" if platform == "windows" else ".tar.gz")))
        _, payload = archive_files(archive)
        executable = payload["amqp-server.exe" if platform == "windows" else "amqp-server"]
        if (report["schemaVersion"] != 1 or report["platform"] != platform + "-amd64"
                or report["status"] != "passed" or report["candidateSha256"] != digest(executable)
                or any(report.get(field) is not True for field in ("freshInstallation", "unsupportedStorageRefused", "failedUpgradeColdRestore", "directoryMigration"))
                or report["verifiedOriginalMessages"] != 640 or report["verifiedNewMessagesAfterCrash"] != 16):
            raise ValueError("upgrade evidence does not match release binary")
        migration = json.loads((root / f"migration-{platform}-amd64.json").read_text(encoding="utf-8"))
        if (migration["schemaVersion"] != 1 or migration["platform"] != platform + "-amd64"
                or migration["status"] != "passed" or migration["candidateSha256"] != digest(executable)
                or migration["baselineSha256"] != report["baselineSha256"]
                or migration.get("samePlatformRestore") is not True or migration.get("separateMachineTested") is not True
                or migration["sourceRunId"] != str(args.run_id) or not migration["sourceJob"] or not migration["targetJob"]
                or migration["sourceJob"] == migration["targetJob"]
                or migration["verifiedOriginalMessages"] != 640 or migration["verifiedNewMessagesAfterRestart"] != 16):
            raise ValueError("migration evidence does not match release binary and run")
    value = {"schemaVersion": 1, "version": version(args.version), "commit": args.commit,
             "buildRunId": args.run_id, "files": files}
    destination = root / "release-manifest.json"
    with destination.open("x", encoding="utf-8") as output:
        json.dump(value, output, indent=2)
        output.write("\n")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    item = commands.add_parser("build")
    item.add_argument("--version", required=True)
    item.add_argument("--target", required=True)
    item.add_argument("--output", required=True)
    item.add_argument("--allow-dirty", action="store_true")
    item.set_defaults(function=build)
    item = commands.add_parser("baseline")
    for field in ("source", "target", "output"):
        item.add_argument("--" + field, required=True)
    item.set_defaults(function=baseline)
    item = commands.add_parser("verify")
    for field in ("archive", "version", "commit"):
        item.add_argument("--" + field, required=True)
    item.add_argument("--extract")
    item.add_argument("--allow-dirty", action="store_true")
    item.set_defaults(function=lambda a: verify(a.archive, a.version, a.commit, a.extract, a.allow_dirty))
    item = commands.add_parser("manifest")
    for field in ("directory", "version", "commit", "run-id"):
        item.add_argument("--" + field, required=True)
    item.set_defaults(function=manifest)
    args = parser.parse_args()
    args.function(args)


if __name__ == "__main__":
    main()
