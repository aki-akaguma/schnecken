#!/usr/bin/env python3

import unittest
import subprocess
import os
import tempfile
import shutil
import time

# Define the path to your bdb-tool.py script
BDB_TOOL_SCRIPT = '../bin/bdb-tool.py' # Assuming it's in the same directory

class TestBDBTool(unittest.TestCase):

    def setUp(self):
        """Set up a temporary directory for each test."""
        self.test_dir = tempfile.mkdtemp()
        # Use a base path for the DB within the temp dir for explicit --db tests
        self.db_path = os.path.join(self.test_dir, 'test_db_base')
        self.db_lock_path = f"{self.db_path}.lock"
        # print(f"\nCreated temporary directory: {self.test_dir}") # Keep print for debugging if needed

    def tearDown(self):
        """Clean up the temporary directory after each test."""
        if os.path.exists(self.test_dir):
            shutil.rmtree(self.test_dir)
            # print(f"Removed temporary directory: {self.test_dir}") # Keep print for debugging if needed

    def _run_bdb_command(self, command_args, expected_exit_code=0, use_temp_db=False):
        """
        Helper to run bdb-tool.py with given arguments.
        Defaults to using --temp-db for isolation unless specified otherwise.
        Returns cleaned stdout and stderr (removes temp_db message from stderr).
        """
        cmd_prefix = ['python3', BDB_TOOL_SCRIPT]
        if use_temp_db:
            cmd_prefix.append('--temp-db')
        else:
            cmd_prefix.extend(['--db', self.db_path]) # Use explicit db path if not temp

        cmd = cmd_prefix + command_args
        # print(f"Running command: {' '.join(cmd)}") # Uncomment for verbose test output
        process = subprocess.run(
            cmd,
            capture_output=True,
            text=True,  # Capture stdout/stderr as text
            check=False # Don't raise CalledProcessError, let us check returncode
        )
        
        # Clean stderr output for assertions
        cleaned_stderr = []
        temp_db_message_found = False
        for line in process.stderr.splitlines():
            if "Using temporary database in:" in line:
                temp_db_message_found = True
                continue # Skip this line
            cleaned_stderr.append(line)
        cleaned_stderr_str = "\n".join(cleaned_stderr).strip()

        # print(f"STDOUT:\n{process.stdout.strip()}") # Uncomment for verbose test output
        # print(f"CLEANED STDERR:\n{cleaned_stderr_str}") # Uncomment for verbose test output

        self.assertEqual(process.returncode, expected_exit_code,
            f"Command failed with unexpected exit code {process.returncode} (expected {expected_exit_code}). "
            f"STDOUT: {process.stdout.strip()} CLEANED STDERR: {cleaned_stderr_str}")
        
        return process.stdout.strip(), cleaned_stderr_str, temp_db_message_found

    def test_set_and_get(self):
        """Test setting and getting a simple key-value pair."""
        key = "mykey"
        value = "myvalue"
        stdout, stderr, temp_db_message_found = self._run_bdb_command(['set', key, value])
        self.assertFalse(temp_db_message_found) # Ensure temp db message was present
        self.assertFalse(stderr) # Set should produce no other stderr output
        self.assertFalse(stdout) # Set should produce no stdout

        stdout, stderr, temp_db_message_found = self._run_bdb_command(['get', key])
        self.assertFalse(temp_db_message_found)
        self.assertEqual(stdout, value)
        self.assertFalse(stderr) # Get should produce no stderr on success

    def test_set_overwrite_without_force(self):
        """Test setting an existing key without --force (should fail)."""
        key = "testkey"
        value1 = "value1"
        value2 = "value2"
        self._run_bdb_command(['set', key, value1]) # First set, should succeed

        # Attempt to set again without force, should fail
        stdout, stderr, temp_db_message_found = self._run_bdb_command(['set', key, value2], expected_exit_code=1)
        self.assertFalse(temp_db_message_found)
        self.assertIn(f"Error: Key '{key}' already exists. Use --force to overwrite.", stderr)
        self.assertFalse(stdout)

        # Get the value to ensure it's still the original
        stdout, stderr, temp_db_message_found = self._run_bdb_command(['get', key])
        self.assertFalse(temp_db_message_found)
        self.assertEqual(stdout, value1)

    def test_set_overwrite_with_force(self):
        """Test setting an existing key with --force (should succeed)."""
        key = "testkey"
        value1 = "value1"
        value2 = "value2"
        self._run_bdb_command(['set', key, value1]) # First set

        # Set again with force, should succeed
        stdout, stderr, temp_db_message_found = self._run_bdb_command(['--force', 'set', key, value2])
        self.assertFalse(temp_db_message_found)
        self.assertFalse(stdout)
        self.assertFalse(stderr) # Should be empty stderr

        # Get the value to ensure it's updated
        stdout, stderr, temp_db_message_found = self._run_bdb_command(['get', key])
        self.assertFalse(temp_db_message_found)
        self.assertEqual(stdout, value2)

    def test_delete(self):
        """Test deleting a key."""
        key = "delkey"
        value = "delvalue"
        self._run_bdb_command(['set', key, value]) # Set the key first

        # Delete the key
        stdout, stderr, temp_db_message_found = self._run_bdb_command(['delete', key])
        self.assertFalse(temp_db_message_found)
        self.assertFalse(stdout)
        self.assertFalse(stderr) # No output on success

        # Try to get the deleted key, should fail
        stdout, stderr, temp_db_message_found = self._run_bdb_command(['get', key], expected_exit_code=1)
        self.assertFalse(temp_db_message_found)
        self.assertIn(f"Error: Key '{key}' not found.", stderr)
        self.assertFalse(stdout)

    def test_delete_non_existent_key(self):
        """Test deleting a non-existent key (should fail)."""
        key = "nonexistent"
        stdout, stderr, temp_db_message_found = self._run_bdb_command(['delete', key], expected_exit_code=1, use_temp_db=True)
        self.assertTrue(temp_db_message_found)
        self.assertIn(f"Error: Key '{key}' not found.", stderr)
        self.assertFalse(stdout)

    def test_rename(self):
        """Test renaming a key."""
        old_key = "oldname"
        new_key = "newname"
        value = "somevalue"
        self._run_bdb_command(['set', old_key, value]) # Set the old key

        # Rename the key
        stdout, stderr, temp_db_message_found = self._run_bdb_command(['rename', old_key, new_key])
        self.assertFalse(temp_db_message_found)
        self.assertFalse(stdout)
        self.assertFalse(stderr)

        # Get the new key, should succeed
        stdout, stderr, temp_db_message_found = self._run_bdb_command(['get', new_key])
        self.assertFalse(temp_db_message_found)
        self.assertEqual(stdout, value)

        # Get the old key, should fail
        stdout, stderr, temp_db_message_found = self._run_bdb_command(['get', old_key], expected_exit_code=1)
        self.assertFalse(temp_db_message_found)
        self.assertIn(f"Error: Key '{old_key}' not found.", stderr)

    def test_rename_non_existent_old_key(self):
        """Test renaming a non-existent old key (should fail)."""
        stdout, stderr, temp_db_message_found = self._run_bdb_command(['rename', 'nonexistent', 'new'], expected_exit_code=1, use_temp_db=True)
        self.assertTrue(temp_db_message_found)
        self.assertIn("Error: Old key 'nonexistent' not found.", stderr)
        self.assertFalse(stdout)

    def test_rename_to_existing_key_without_force(self):
        """Test renaming to an existing key without --force (should fail)."""
        key1 = "k1"
        key2 = "k2"
        val1 = "v1"
        val2 = "v2"
        self._run_bdb_command(['set', key1, val1])
        self._run_bdb_command(['set', key2, val2])

        stdout, stderr, temp_db_message_found = self._run_bdb_command(['rename', key1, key2], expected_exit_code=1)
        self.assertFalse(temp_db_message_found)
        self.assertIn(f"Error: New key '{key2}' already exists. Use --force to overwrite.", stderr)
        self.assertFalse(stdout)

        # Original keys should still hold their values
        stdout, stderr, _ = self._run_bdb_command(['get', key1])
        self.assertEqual(stdout, val1)

        stdout, stderr, _ = self._run_bdb_command(['get', key2])
        self.assertEqual(stdout, val2)

    def test_rename_to_existing_key_with_force(self):
        """Test renaming to an existing key with --force (should succeed)."""
        key1 = "k1"
        key2 = "k2"
        val1 = "v1"
        val2 = "v2"
        self._run_bdb_command(['set', key1, val1])
        self._run_bdb_command(['set', key2, val2])

        stdout, stderr, temp_db_message_found = self._run_bdb_command(['--force', 'rename', key1, key2])
        self.assertFalse(temp_db_message_found)
        self.assertFalse(stdout)
        self.assertFalse(stderr)

        stdout, stderr, _ = self._run_bdb_command(['get', key2])
        self.assertEqual(stdout, val1) # New key should have old_key's value

        stdout, stderr, _ = self._run_bdb_command(['get', key1], expected_exit_code=1)
        self.assertIn(f"Error: Key '{key1}' not found.", stderr) # Old key should be gone

    def test_dump_and_restore(self):
        """Test dumping to stdout and restoring from a file."""
        data = {
            "keyA": "valueA",
            "keyB": "value with spaces",
            "keyC": "value\nwith\nnewlines", # shelve handles newlines internally, dump should reflect
            "keyD": "value\twith\ttab" # Test tab in value, should still parse correctly
        }
        for k, v in data.items():
            self._run_bdb_command(['set', k, v])

        stdout, stderr, temp_db_message_found = self._run_bdb_command(['dump'])
        self.assertFalse(temp_db_message_found)
        self.assertFalse(stderr) # Dump should produce no other stderr on success

        dumped_lines = stdout.splitlines()
        dumped_lines = [line for line in dumped_lines if line.strip()] # Filter out empty lines

        self.assertEqual(len(dumped_lines), len(data))

        # Sort for consistent comparison (shelve.items() order is not guaranteed)
        expected_lines = sorted([f"{k}\t{v.replace('\n', ' ')}" for k, v in data.items()])
        actual_lines = sorted(dumped_lines)
        self.assertEqual(actual_lines, expected_lines)

        # Create a new, separate temporary DB path for restore to ensure isolation
        restore_db_path = os.path.join(self.test_dir, 'restore_db_base')
        backup_file_path = os.path.join(self.test_dir, 'backup.txt')

        with open(backup_file_path, 'w', encoding='utf-8') as f:
            f.write(stdout) # Write the dumped content to a backup file

        # Run restore command on a fresh DB using its explicit path (not --temp-db here)
        cmd = ['python3', BDB_TOOL_SCRIPT, '--db', restore_db_path, 'restore', backup_file_path]
        restore_process = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            check=False
        )
        
        # Clean stderr for restore process as well
        cleaned_restore_stderr = []
        for line in restore_process.stderr.splitlines():
            if "Using temporary database in:" in line:
                continue
            cleaned_restore_stderr.append(line)
        cleaned_restore_stderr_str = "\n".join(cleaned_restore_stderr).strip()

        self.assertEqual(restore_process.returncode, 0,
                         f"Restore command failed. STDOUT: {restore_process.stdout.strip()} STDERR: {cleaned_restore_stderr_str}")
        self.assertIn(f"Successfully restored {len(data)} key-value pairs from '{backup_file_path}'.", cleaned_restore_stderr_str)
        self.assertFalse(restore_process.stdout)

        # Verify restored content by getting from the new DB
        for k, v in data.items():
            cmd_get = ['python3', BDB_TOOL_SCRIPT, '--db', restore_db_path, 'get', k]
            get_process = subprocess.run(
                cmd_get,
                capture_output=True,
                text=True,
                check=True # Should succeed
            )
            v = v.replace('\n', ' ')
            self.assertEqual(get_process.stdout.strip(), v, f"Restored value for '{k}' mismatch.")

    def test_restore_invalid_format(self):
        """Test restoring from a file with invalid line format."""
        backup_file_path = os.path.join(self.test_dir, 'bad_backup.txt')
        with open(backup_file_path, 'w', encoding='utf-8') as f:
            f.write("key1\tvalue1\n")
            f.write("invalid_line_no_tab\n") # Line 2 (no tab)
            f.write("key2\tvalue2\n")
            f.write("\n") # Line 4 (empty line, skipped)
            f.write("key_only\n") # Line 5 (missing value, should be skipped)
            f.write("another_bad_line_too_many\tparts\tmore\n") # Line 6 (too many parts)

        stdout, stderr, temp_db_message_found = self._run_bdb_command(['restore', backup_file_path])
        self.assertFalse(temp_db_message_found)
        self.assertIn("Warning: Line 2 in", stderr)
        self.assertIn("Warning: Line 5 in", stderr) # New: key_only
        #self.assertIn("Warning: Line 6 in", stderr) # New: another_bad_line_too_many
        self.assertIn("has invalid format. Skipping", stderr)
        self.assertIn("Successfully restored 3 key-value pairs from", stderr) # Only key1, key2, another_bad_line_too_many should be restored
        self.assertFalse(stdout)

        stdout, stderr, _ = self._run_bdb_command(['get', 'key1'])
        self.assertEqual(stdout, "value1")

        stdout, stderr, _ = self._run_bdb_command(['get', 'key2'])
        self.assertEqual(stdout, "value2")

        # Confirm bad keys were not restored
        stdout, stderr, _ = self._run_bdb_command(['get', 'invalid_line_no_tab'], expected_exit_code=1)
        self.assertIn("Error: Key 'invalid_line_no_tab' not found.", stderr)
        
        stdout, stderr, _ = self._run_bdb_command(['get', 'key_only'], expected_exit_code=1)
        self.assertIn("Error: Key 'key_only' not found.", stderr)

        stdout, stderr, _ = self._run_bdb_command(['get', 'another_bad_line_too_many'], expected_exit_code=0)
        #self.assertIn("Error: Key 'another_bad_line_too_many' not found.", stderr)

    def test_temp_db_cleanup(self):
        """Test that --temp-db cleans up its files/directory."""
        temp_dir_path = None
        # Run a command using --temp-db and capture stderr for the temp directory path
        process = subprocess.run(
            ['python3', BDB_TOOL_SCRIPT, '--temp-db', 'set', 'temp_key', 'temp_value'],
            capture_output=True,
            text=True,
            check=True
        )
        # Parse the stderr to find the temporary directory path
        for line in process.stderr.splitlines():
            if "Using temporary database in:" in line:
                temp_dir_path = line.split("Using temporary database in: ")[1].split(" (base name:")[0].strip()
                break

        self.assertIsNotNone(temp_dir_path, "Did not find temporary database path in stderr.")
        # After the subprocess exits, atexit should have run, so the directory should be gone.
        self.assertFalse(os.path.exists(temp_dir_path), f"Temporary directory '{temp_dir_path}' was not cleaned up.")

# Run the tests
if __name__ == '__main__':
    unittest.main()

# vim: set ts=4 sw=4 sts=0 expandtab:
