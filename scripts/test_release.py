import io
import json
from pathlib import Path
import tempfile
import tarfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch
import zipfile

import publish
import release


class ReleaseTests(unittest.TestCase):
    def fixture(self, directory, dirty=False, target="windows-amd64"):
        root = "strangeq-1.2.3-" + target
        binary = "amqp-server.exe" if target.startswith("windows-") else "amqp-server"
        files = {name: name.encode() for name in (binary, "LICENSE", "THIRD_PARTY_NOTICES.txt", "config.sample.yaml", "README.md", "AUTHORIZATION.md", "TLS.md")}
        metadata = {"schemaVersion": 1, "version": "1.2.3", "commit": "a" * 40,
                    "target": target, "goVersion": release.contract()["goVersion"], "dirty": dirty,
                    "files": {name: release.digest(data) for name, data in files.items()}}
        files["build-metadata.json"] = json.dumps(metadata).encode()
        path = directory / (root + (".zip" if target.startswith("windows-") else ".tar.gz"))
        if target.startswith("windows-"):
            with zipfile.ZipFile(path, "w") as archive:
                for name, data in files.items():
                    archive.writestr(root + "/" + name, data)
        else:
            with tarfile.open(path, "w:gz") as archive:
                for name, data in files.items():
                    member = tarfile.TarInfo(root + "/" + name)
                    member.size = len(data)
                    archive.addfile(member, io.BytesIO(data))
        path.with_name(path.name + ".sha256").write_text(release.digest(path.read_bytes()) + "  " + path.name + "\n")
        return path

    def test_archive_identity_and_empty_extraction(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            archive = self.fixture(root)
            metadata = release.verify(archive, "v1.2.3", "a" * 40, root / "unpacked")
            self.assertEqual(metadata["target"], "windows-amd64")
            with self.assertRaisesRegex(ValueError, "empty"):
                release.verify(archive, "1.2.3", "a" * 40, root / "unpacked")
            with self.assertRaisesRegex(ValueError, "identity"):
                release.verify(archive, "1.2.4", "a" * 40)
            with self.assertRaisesRegex(ValueError, "identity"):
                release.verify(archive, "1.2.3", "b" * 40)

    def test_tampering_is_rejected(self):
        with tempfile.TemporaryDirectory() as temporary:
            archive = self.fixture(Path(temporary))
            archive.write_bytes(archive.read_bytes() + b"modified")
            with self.assertRaisesRegex(ValueError, "checksum"):
                release.verify(archive, "1.2.3", "a" * 40)

    def test_content_hash_is_checked_after_outer_hash(self):
        with tempfile.TemporaryDirectory() as temporary:
            archive = self.fixture(Path(temporary))
            root, files = release.archive_files(archive)
            files["amqp-server.exe"] = b"modified"
            with zipfile.ZipFile(archive, "w") as output:
                for name, data in files.items():
                    output.writestr(root + "/" + name, data)
            archive.with_name(archive.name + ".sha256").write_text(release.digest(archive.read_bytes()) + "  " + archive.name)
            with self.assertRaisesRegex(ValueError, "content"):
                release.verify(archive, "1.2.3", "a" * 40)

    def test_traversal_and_windows_collisions_are_rejected(self):
        with tempfile.TemporaryDirectory() as temporary:
            archive = Path(temporary) / "test.zip"
            for names in (["../escape"], ["root/a", "root/A"], ["root/a:b"], ["root\\escape"],
                          ["root/a", "root/./a"], ["root/a."], ["root/CON"], ["root/NUL.txt"]):
                with self.subTest(names=names):
                    with zipfile.ZipFile(archive, "w") as output:
                        for name in names:
                            output.writestr(name, b"test")
                    if names == ["root\\escape"]:
                        archive.write_bytes(archive.read_bytes().replace(b"root/escape", b"root\\escape"))
                    with self.assertRaises(ValueError):
                        release.archive_files(archive)

    def test_dirty_archive_is_not_release_eligible(self):
        with tempfile.TemporaryDirectory() as temporary:
            archive = self.fixture(Path(temporary), dirty=True)
            with self.assertRaisesRegex(ValueError, "identity"):
                release.verify(archive, "1.2.3", "a" * 40)
            release.verify(archive, "1.2.3", "a" * 40, allow_dirty=True)

    def test_version_cannot_inject_paths_or_commands(self):
        for value in ("v1.2.3/../../x", "v1.2.3;exit", "--help", "1.2", "1.2.3\n", "1.2.3+..", "01.2.3", "1.2.3-01", "1.2.3-rc..1"):
            with self.subTest(value=value), self.assertRaises(ValueError):
                release.version(value)
        self.assertEqual(release.version("v1.2.3-rc.1+build.2"), "1.2.3-rc.1+build.2")
        self.assertEqual(release.version("v1.2.3-rc--next+build-01"), "1.2.3-rc--next+build-01")

    def test_publish_requires_exact_asset_set_and_hashes(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            payload = b"verified"
            (root / "artifact.zip").write_bytes(payload)
            manifest = {"schemaVersion": 1, "version": "1.2.3", "commit": "a" * 40, "buildRunId": "123",
                        "files": {"artifact.zip": {"size": len(payload), "sha256": release.digest(payload)}}}
            (root / "release-manifest.json").write_text(json.dumps(manifest))
            publish.verify_directory(root, "v1.2.3", "a" * 40)
            (root / "unexpected").write_text("test")
            with self.assertRaisesRegex(ValueError, "set mismatch"):
                publish.verify_directory(root, "v1.2.3", "a" * 40)

    def test_tag_resolution_handles_annotated_tags(self):
        with patch.object(publish, "api", side_effect=[{"object": {"type": "tag", "sha": "b" * 40}}, {"object": {"type": "commit", "sha": "a" * 40}}]):
            self.assertEqual(publish.tag_commit("owner/repo", "v1.2.3"), "a" * 40)
        with patch.object(publish, "api", return_value={"object": {"type": "tree", "sha": "a" * 40}}):
            with self.assertRaises(ValueError):
                publish.tag_commit("owner/repo", "v1.2.3")

    def test_publish_gates_before_publication(self):
        current = {"id": 17, "draft": True, "assets": [{"id": 7, "name": "artifact.zip", "size": 3, "updated_at": "original"}]}
        build = {"conclusion": "success", "status": "completed", "head_sha": "a" * 40,
                 "path": ".github/workflows/release.yml", "event": "push"}
        manifest = {"version": "1.2.3-rc.1", "buildRunId": "123"}
        for scenario in ("valid", "failed-build", "wrong-commit", "changed-assets", "changed-tag", "bad-attestation", "bad-hash"):
            with self.subTest(scenario=scenario), tempfile.TemporaryDirectory() as temporary:
                run = dict(build)
                latest = dict(current)
                commits = ["a" * 40, "a" * 40]
                if scenario == "failed-build":
                    run["conclusion"] = "failure"
                if scenario == "wrong-commit":
                    run["head_sha"] = "b" * 40
                if scenario == "changed-assets":
                    latest["assets"] = [{**current["assets"][0], "updated_at": "replaced"}]
                if scenario == "changed-tag":
                    commits[1] = "b" * 40
                final = {**current, "draft": False, "html_url": "https://example.invalid/release"}
                commands = []

                def execute(command, **kwargs):
                    commands.append(command)
                    if scenario == "bad-attestation" and command[:3] == ["gh", "attestation", "verify"]:
                        raise ValueError("attestation rejected")

                args = SimpleNamespace(directory=temporary, tag="v1.2.3-rc.1", repository="owner/repo")
                with patch.object(publish, "api", side_effect=[current, run, latest, final]), \
                        patch.object(publish, "tag_commit", side_effect=commits), \
                        patch.object(publish, "verify_directory", return_value=manifest,
                                     side_effect=ValueError("hash mismatch") if scenario == "bad-hash" else None), \
                        patch.object(publish.subprocess, "run", side_effect=execute):
                    if scenario == "valid":
                        publish.publish(args)
                        self.assertEqual([c[1:3] for c in commands], [["release", "download"], ["attestation", "verify"], ["release", "edit"]])
                        self.assertIn("--prerelease=true", commands[-1])
                        self.assertIn("--latest=false", commands[-1])
                    else:
                        with self.assertRaises(ValueError):
                            publish.publish(args)
                        self.assertFalse(any(c[:3] == ["gh", "release", "edit"] for c in commands))

    def test_release_manifest_requires_native_evidence_for_exact_binary(self):
        for scenario in ("valid", "wrong-binary", "same-machine", "wrong-run", "failed-suite", "skipped-upgrade", "missing-platform"):
            with self.subTest(scenario=scenario), tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                for target in release.contract()["targets"]:
                    if scenario == "missing-platform" and target == "darwin-arm64":
                        continue
                    self.fixture(root, target=target)
                for platform in ("windows", "linux"):
                    binary = "amqp-server.exe" if platform == "windows" else "amqp-server"
                    upgrade = {"schemaVersion": 1, "platform": platform + "-amd64", "status": "passed",
                               "candidateSha256": release.digest(binary.encode()), "baselineSha256": "b" * 64,
                               "freshInstallation": True, "unsupportedStorageRefused": True, "failedUpgradeColdRestore": True,
                               "directoryMigration": True, "verifiedOriginalMessages": 640, "verifiedNewMessagesAfterCrash": 16}
                    migration = {"schemaVersion": 1, "platform": platform + "-amd64", "status": "passed",
                                 "candidateSha256": upgrade["candidateSha256"], "baselineSha256": upgrade["baselineSha256"],
                                 "samePlatformRestore": True, "separateMachineTested": True, "sourceRunId": "123",
                                 "sourceJob": "upgrade", "targetJob": "migration", "verifiedOriginalMessages": 640,
                                 "verifiedNewMessagesAfterRestart": 16}
                    if scenario == "wrong-binary":
                        upgrade["candidateSha256"] = "c" * 64
                    if scenario == "same-machine":
                        migration["separateMachineTested"] = False
                    if scenario == "wrong-run":
                        migration["sourceRunId"] = "456"
                    for kind, report in (("upgrade", upgrade), ("migration", migration)):
                        (root / f"{kind}-{platform}-amd64.json").write_text(json.dumps(report))
                for suite in ("unit", "race", "conformance", "upgrade", "migration"):
                    for platform in (("windows", "linux") if suite in ("upgrade", "migration") else (None,)):
                        name = f"tests-{suite}-" + (platform + "-amd64-" if platform else "") + suite + "-summary.json"
                        summary = {"suite": suite, "exitCode": 0, "passed": 1, "failed": [], "skipped": []}
                        if scenario == "failed-suite":
                            summary["exitCode"] = 1
                        if scenario == "skipped-upgrade" and suite == "upgrade":
                            summary["skipped"] = ["required test"]
                        (root / name).write_text(json.dumps(summary))
                args = SimpleNamespace(directory=temporary, version="1.2.3", commit="a" * 40, run_id="123")
                if scenario == "valid":
                    release.manifest(args)
                    publish.verify_directory(root, "v1.2.3", "a" * 40)
                else:
                    with self.assertRaises(ValueError):
                        release.manifest(args)
                    self.assertFalse((root / "release-manifest.json").exists())


if __name__ == "__main__":
    unittest.main()
