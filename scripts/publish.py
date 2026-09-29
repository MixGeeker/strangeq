#!/usr/bin/env python3
"""创建完整草稿，或校验并晋级其原始资产；不移动 tag、不覆盖资产。"""
import argparse
import json
import os
from pathlib import Path
import re
import subprocess
from urllib.parse import quote

import release


def gh(*args):
    return subprocess.check_output(["gh", *args], text=True, encoding="utf-8").strip()


def api(repository, path):
    return json.loads(gh("api", f"repos/{repository}/{path}"))


def tag_commit(repository, tag):
    obj = api(repository, "git/ref/tags/" + quote(tag, safe=""))["object"]
    for _ in range(8):
        if obj["type"] == "commit":
            return obj["sha"]
        if obj["type"] != "tag":
            raise ValueError("tag does not point to a commit")
        obj = api(repository, "git/tags/" + obj["sha"])["object"]
    raise ValueError("tag nesting exceeds limit")


def verify_directory(directory, tag, commit):
    manifest = json.loads((directory / "release-manifest.json").read_text(encoding="utf-8"))
    if manifest["schemaVersion"] != 1 or manifest["version"] != release.version(tag) or manifest["commit"] != commit:
        raise ValueError("release manifest identity mismatch")
    if not re.fullmatch(r"[1-9][0-9]*", str(manifest["buildRunId"])):
        raise ValueError("invalid build run")
    expected = set(manifest["files"]) | {"release-manifest.json"}
    actual = {p.name for p in directory.iterdir()}
    if actual != expected:
        raise ValueError("release asset set mismatch")
    for name, metadata in manifest["files"].items():
        if Path(name).name != name or "/" in name or "\\" in name or ":" in name or name in (".", ".."):
            raise ValueError("invalid release asset name")
        path = directory / name
        if not path.is_file() or path.is_symlink() or path.stat().st_size != metadata["size"] or release.digest(path.read_bytes()) != metadata["sha256"]:
            raise ValueError("release asset changed: " + name)
    return manifest


def draft(args):
    directory = Path(args.directory).resolve()
    commit = tag_commit(args.repository, args.tag)
    manifest = verify_directory(directory, args.tag, commit)
    if commit != os.environ.get("GITHUB_SHA") or str(manifest["buildRunId"]) != os.environ.get("GITHUB_RUN_ID"):
        raise ValueError("draft is not from this exact tag build")
    if os.environ.get("GITHUB_REF") != "refs/tags/" + args.tag:
        raise ValueError("draft requires a tag workflow")
    # 使用完整资产集一次创建草稿；API 在同 tag 已有 Release 时拒绝，不作 clobber。
    notes = directory.parent / "release-notes.md"
    notes.write_text(
        "StrangeQ " + manifest["version"] + "\n\n"
        + "源码提交：`" + commit + "`。\n\n"
        + "Windows/Linux amd64 最终归档已验证首次安装、固定基线升级、进程强杀恢复、失败冷备回退、目录迁移和同平台换机恢复。整机重启、掉电与服务注册尚未覆盖。\n\n"
        + "附件包含逐平台制品、摘要、升级与迁移报告及测试失败／跳过摘要。请结合已知限制审核后通过 Publish verified release 工作流发布。\n",
        encoding="utf-8")
    subprocess.run(["gh", "release", "create", args.tag, "--repo", args.repository,
                    "--verify-tag", "--draft", "--title", "StrangeQ " + manifest["version"],
                    "--notes-file", str(notes), *[str(directory / name) for name in sorted({*manifest["files"], "release-manifest.json"})]], check=True)


def publish(args):
    directory = Path(args.directory).resolve()
    directory.mkdir(parents=True, exist_ok=True)
    if any(directory.iterdir()):
        raise ValueError("publish download directory must be empty")
    current = api(args.repository, "releases/tags/" + quote(args.tag, safe=""))
    if not current["draft"]:
        raise ValueError("release is already published")
    commit = tag_commit(args.repository, args.tag)
    subprocess.run(["gh", "release", "download", args.tag, "--repo", args.repository, "--dir", str(directory)], check=True)
    # 先验证清单自身的来源，再信任清单内的 buildRunId 和资产摘要。
    subprocess.run(["gh", "attestation", "verify", str(directory / "release-manifest.json"),
                    "--repo", args.repository,
                    "--signer-workflow", args.repository + "/.github/workflows/release.yml",
                    "--source-ref", "refs/tags/" + args.tag, "--source-digest", commit,
                    "--deny-self-hosted-runners"], check=True)
    manifest = verify_directory(directory, args.tag, commit)
    build = api(args.repository, "actions/runs/" + str(manifest["buildRunId"]))
    if (build["conclusion"] != "success" or build["status"] != "completed" or build["head_sha"] != commit
            or build["path"].split("@")[0] != ".github/workflows/release.yml" or build["event"] != "push"):
        raise ValueError("release build has not completed successfully for this commit")
    # 读取最新身份，拒绝准备期间有人替换草稿或移动 tag。
    latest = api(args.repository, "releases/tags/" + quote(args.tag, safe=""))
    identity = lambda value: (value["id"], value["draft"], sorted((a["id"], a["name"], a["size"], a["updated_at"]) for a in value["assets"]))
    if identity(current) != identity(latest) or tag_commit(args.repository, args.tag) != commit:
        raise ValueError("release changed during verification")
    prerelease = "-" in manifest["version"].split("+")[0]
    subprocess.run(["gh", "release", "edit", args.tag, "--repo", args.repository,
                    "--draft=false", "--prerelease=" + str(prerelease).lower(),
                    "--latest=" + str(not prerelease).lower()], check=True)
    final = api(args.repository, "releases/tags/" + quote(args.tag, safe=""))
    if final["draft"] or final["id"] != current["id"]:
        raise ValueError("release publication was not confirmed")
    print(final["html_url"])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("operation", choices=["draft", "publish"])
    parser.add_argument("--tag", required=True)
    parser.add_argument("--repository", required=True)
    parser.add_argument("--directory", required=True)
    args = parser.parse_args()
    if args.tag != "v" + release.version(args.tag):
        raise ValueError("release tag must start with v")
    if not re.fullmatch(r"[A-Za-z0-9_-]+/[A-Za-z0-9_.-]+", args.repository):
        raise ValueError("invalid repository")
    {"draft": draft, "publish": publish}[args.operation](args)


if __name__ == "__main__":
    main()
