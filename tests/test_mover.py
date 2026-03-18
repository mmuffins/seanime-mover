import io
import logging
import tempfile
import time
import unittest
from contextlib import redirect_stdout
from pathlib import Path

from mover import READY_AGE_SECONDS, clean_queue, scan_once


class TestMover(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.root = Path(self.temp_dir.name)
        self.source = self.root / "source"
        self.dest = self.root / "dest"
        self.source.mkdir()
        self.dest.mkdir()
        self.log_stream = io.StringIO()
        self.logger = logging.getLogger(f"test_mover_{id(self)}")
        self.logger.handlers.clear()
        self.logger.setLevel(logging.INFO)
        self.logger.propagate = False
        handler = logging.StreamHandler(self.log_stream)
        handler.setFormatter(logging.Formatter("%(levelname)s %(message)s"))
        self.logger.addHandler(handler)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def _write_file(self, relative_path: str, content: bytes, age_seconds: int = READY_AGE_SECONDS + 5) -> Path:
        path = self.source / relative_path
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(content)
        file_time = time.time() - age_seconds
        path.touch()
        path_time = (file_time, file_time)
        path.parent.touch()
        import os

        os.utime(path, path_time)
        return path

    def test_moves_nested_file_and_creates_placeholder(self) -> None:
        source_path = self._write_file("a/b/example.txt", b"payload")

        stats = scan_once(self.source, self.dest, self.logger, now=time.time())

        self.assertEqual(stats.moved, 1)
        self.assertTrue((self.dest / "example.txt").exists())
        self.assertEqual((self.dest / "example.txt").read_bytes(), b"payload")
        self.assertTrue(source_path.exists())
        self.assertEqual(source_path.stat().st_size, 0)

    def test_flattens_source_structure(self) -> None:
        self._write_file("one/two/three/data.bin", b"abc")

        scan_once(self.source, self.dest, self.logger, now=time.time())

        self.assertTrue((self.dest / "data.bin").exists())
        self.assertFalse((self.dest / "one").exists())

    def test_skips_zero_byte_files(self) -> None:
        placeholder = self._write_file("nested/placeholder.txt", b"")

        stats = scan_once(self.source, self.dest, self.logger, now=time.time())

        self.assertEqual(stats.skipped_zero_byte, 1)
        self.assertFalse((self.dest / "placeholder.txt").exists())
        self.assertEqual(placeholder.stat().st_size, 0)

    def test_skips_tmp_directories(self) -> None:
        tmp_file = self._write_file("a/.tmp123/deeper/file.txt", b"payload")

        stats = scan_once(self.source, self.dest, self.logger, now=time.time())

        self.assertEqual(stats.skipped_tmp_dirs, 1)
        self.assertTrue(tmp_file.exists())
        self.assertFalse((self.dest / "file.txt").exists())

    def test_skips_recent_files(self) -> None:
        recent_file = self._write_file("recent/file.txt", b"payload", age_seconds=5)

        stats = scan_once(self.source, self.dest, self.logger, now=time.time())

        self.assertEqual(stats.skipped_too_recent, 1)
        self.assertTrue(recent_file.exists())
        self.assertFalse((self.dest / "file.txt").exists())

    def test_collision_logs_error_and_leaves_source_untouched(self) -> None:
        source_path = self._write_file("a/b/shared.txt", b"source-data")
        (self.dest / "shared.txt").write_bytes(b"dest-data")

        stats = scan_once(self.source, self.dest, self.logger, now=time.time())

        self.assertEqual(stats.collisions, 1)
        self.assertEqual(source_path.read_bytes(), b"source-data")
        self.assertEqual((self.dest / "shared.txt").read_bytes(), b"dest-data")
        self.assertIn("Destination collision", self.log_stream.getvalue())

    def test_placeholder_is_not_moved_again(self) -> None:
        source_path = self._write_file("a/b/repeat.txt", b"payload")

        first_stats = scan_once(self.source, self.dest, self.logger, now=time.time())
        second_stats = scan_once(self.source, self.dest, self.logger, now=time.time() + READY_AGE_SECONDS + 5)

        self.assertEqual(first_stats.moved, 1)
        self.assertEqual(second_stats.moved, 0)
        self.assertEqual(second_stats.skipped_zero_byte, 1)
        self.assertEqual(source_path.stat().st_size, 0)
        self.assertEqual((self.dest / "repeat.txt").read_bytes(), b"payload")

    def test_clean_queue_prints_marker(self) -> None:
        stdout = io.StringIO()

        with redirect_stdout(stdout):
            clean_queue()

        self.assertEqual(stdout.getvalue(), "clean_queue\n")


if __name__ == "__main__":
    unittest.main()
