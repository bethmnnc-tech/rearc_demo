"""
Unit tests for the pure-Python parts of ingestion/bls_ingest.py -- the logic
with actual branching to get wrong (which links count as files, which files
need re-pulling). No Spark session or network access required.

Run with: pytest tests/ -v
"""

import bls_ingest as b


class TestIsDataFileLink:
    def test_accepts_plain_filenames(self):
        assert b._is_data_file_link("pr.series") is True
        assert b._is_data_file_link("pr.data.0.Current") is True

    def test_rejects_parent_dir(self):
        assert b._is_data_file_link("../") is False
        assert b._is_data_file_link("..") is False

    def test_rejects_sort_query_links(self):
        assert b._is_data_file_link("?C=N;O=D") is False
        assert b._is_data_file_link("?C=M;O=A") is False

    def test_rejects_subdirectories(self):
        assert b._is_data_file_link("subdir/") is False

    def test_rejects_absolute_and_anchor_links(self):
        assert b._is_data_file_link("/pub/time.series/pr/") is False
        assert b._is_data_file_link("#top") is False
        assert b._is_data_file_link("mailto:someone@bls.gov") is False

    def test_rejects_empty(self):
        assert b._is_data_file_link("") is False


class TestFetchBlsIndexParsing:
    """Exercises the href-extraction regex against a synthetic Apache-style
    directory listing, without making a real network call."""

    SAMPLE_HTML = """
    <html><body>
    <a href="/pub/time.series/pr/">Parent Directory</a>
    <a href="?C=N;O=D">Name</a>
    <a href="pr.class">pr.class</a>              06-Aug-2026 08:30   102
    <a href="pr.data.0.Current">pr.data.0.Current</a>   06-Aug-2026 08:30   1.5M
    <a href="pr.series">pr.series</a>             06-Aug-2026 08:30   15K
    </body></html>
    """

    def test_extracts_only_real_files(self):
        names = [h for h in b._HREF_RE.findall(self.SAMPLE_HTML) if b._is_data_file_link(h)]
        assert names == ["pr.class", "pr.data.0.Current", "pr.series"]


class TestFilesToPull:
    def test_new_file_is_pulled(self):
        remote = [b.RemoteFile("pr.series", 100, "Mon")]
        manifest = {}
        result = b.files_to_pull(remote, manifest)
        assert [f.name for f in result] == ["pr.series"]

    def test_unchanged_file_is_skipped(self):
        remote = [b.RemoteFile("pr.series", 100, "Mon")]
        manifest = {"pr.series": b.RemoteFile("pr.series", 100, "Mon")}
        assert b.files_to_pull(remote, manifest) == []

    def test_changed_size_triggers_repull(self):
        remote = [b.RemoteFile("pr.data.0.Current", 200, "Mon")]
        manifest = {"pr.data.0.Current": b.RemoteFile("pr.data.0.Current", 100, "Mon")}
        result = b.files_to_pull(remote, manifest)
        assert [f.name for f in result] == ["pr.data.0.Current"]

    def test_changed_last_modified_triggers_repull(self):
        remote = [b.RemoteFile("pr.data.0.Current", 100, "Tue")]
        manifest = {"pr.data.0.Current": b.RemoteFile("pr.data.0.Current", 100, "Mon")}
        result = b.files_to_pull(remote, manifest)
        assert [f.name for f in result] == ["pr.data.0.Current"]

    def test_removed_remote_file_is_simply_not_in_result(self):
        # A file that disappeared from BLS's listing shouldn't error -- it's
        # just absent from `remote`, so it can't appear in the pull list.
        # (We keep the manifest row; we don't delete already-landed raw data.)
        remote = []
        manifest = {"pr.footnote": b.RemoteFile("pr.footnote", 40, "Mon")}
        assert b.files_to_pull(remote, manifest) == []

    def test_mixed_batch(self):
        remote = [
            b.RemoteFile("unchanged", 10, "Mon"),
            b.RemoteFile("changed", 99, "Mon"),
            b.RemoteFile("new", 5, "Mon"),
        ]
        manifest = {
            "unchanged": b.RemoteFile("unchanged", 10, "Mon"),
            "changed": b.RemoteFile("changed", 10, "Mon"),
        }
        result = {f.name for f in b.files_to_pull(remote, manifest)}
        assert result == {"changed", "new"}
