#!/usr/bin/env python3

import argparse
import os
import sys
import tempfile
import shutil
import atexit
from typing import Optional

import bsddb3 as bsddb
from bsddb3.db import DBError

class BDBTool:
    """
    A utility class for interacting with Berkeley DB (bsddb3).

    Supports opening, closing, setting, getting, deleting, renaming,
    dumping, and restoring key-value pairs.
    """
    def __init__(self, db_file: str):
        self.db_file = db_file
        self.db: Optional[bsddb.db.DB] = None
        self._temp_dir_to_clean: Optional[str] = None # Stores the temp directory to remove

        try:
            # 'c' mode creates the database if it doesn't exist
            # 0o666 is the file permission mask
            self.db = bsddb.hashopen(db_file, "c", 0o666)
        except DBError as e:
            sys.stderr.write(f"Error: Could not open database '{db_file}': {e}\n")
            sys.exit(1) # Exit immediately if DB cannot be opened

        # Register close to be called automatically on script exit
        atexit.register(self.close)

    def __enter__(self):
        """Context manager entry point."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit point, ensures close is called."""
        self.close()

    def close(self):
        """Closes the Berkeley DB instance and performs cleanup."""
        if self.db:
            try:
                self.db.close()
            except DBError as e:
                sys.stderr.write(f"Warning: Error closing database '{self.db_file}': {e}\n")
            finally:
                self.db = None # Ensure db reference is cleared

        # If a temporary directory was created for the database, clean it up
        if self._temp_dir_to_clean and os.path.exists(self._temp_dir_to_clean):
            try:
                shutil.rmtree(self._temp_dir_to_clean, ignore_errors=True)
            except OSError as e:
                sys.stderr.write(
                    f"Warning: Could not remove temporary directory '{self._temp_dir_to_clean}': {e}\n")
            finally:
                self._temp_dir_to_clean = None

    def set(self, key: str, value: str, force: bool = False) -> bool:
        """
        Sets a key-value pair. Overwrites if `force` is True or key doesn't exist.
        Returns True on success, False on failure.
        """
        key_bin = key.encode('utf-8')
        if not force and key_bin in self.db:
            sys.stderr.write(f"Error: Key '{key}' already exists. Use --force to overwrite.\n")
            return False

        # Normalize value: remove carriage returns and replace newlines with spaces
        normalized_value = value.replace('\r', '').replace('\n', ' ')
        value_bin = normalized_value.encode('utf-8')

        try:
            self.db[key_bin] = value_bin
            self.db.sync() # Ensure changes are written to disk
            return True
        except DBError as e:
            sys.stderr.write(f"Error setting key '{key}': {e}\n")
            return False

    def get(self, key: str) -> Optional[str]:
        """
        Retrieves the value for a given key.
        Returns the decoded string value, or None if the key is not found or an error occurs.
        """
        key_bin = key.encode('utf-8')
        try:
            value_bin = self.db.get(key_bin)
            if value_bin is None:
                return None

            # Decode and normalize value
            value = value_bin.decode('utf-8')
            normalized_value = value.replace('\r', '').replace('\n', ' ')
            return normalized_value
        except DBError as e:
            sys.stderr.write(f"Error retrieving key '{key}': {e}\n")
            return None

    def delete(self, key: str) -> bool:
        """
        Deletes a key-value pair from the database.
        Returns True on success, False if the key is not found or an error occurs.
        """
        key_bin = key.encode('utf-8')
        if key_bin not in self.db:
            sys.stderr.write(f"Error: Key '{key}' not found.\n")
            return False

        try:
            del self.db[key_bin]
            self.db.sync()
            return True
        except DBError as e:
            sys.stderr.write(f"Error deleting key '{key}': {e}\n")
            return False

    def rename(self, old_key: str, new_key: str, force: bool = False) -> bool:
        """
        Renames an existing key to a new key.
        Returns True on success, False on failure.
        """
        old_key_bin = old_key.encode('utf-8')
        new_key_bin = new_key.encode('utf-8')

        if old_key_bin not in self.db:
            sys.stderr.write(f"Error: Old key '{old_key}' not found.\n")
            return False

        if not force and new_key_bin in self.db:
            sys.stderr.write(
                f"Error: New key '{new_key}' already exists. Use --force to overwrite.\n")
            return False

        try:
            value_bin = self.db[old_key_bin]
            self.db[new_key_bin] = value_bin
            del self.db[old_key_bin]
            self.db.sync()
            return True
        except DBError as e:
            sys.stderr.write(f"Error renaming key from '{old_key}' to '{new_key}': {e}\n")
            return False

    def dump(self) -> bool:
        """
        Dumps all key-value pairs to standard output in 'key\\tvalue' format.
        Returns True on success, False on error.
        """
        try:
            for k_bin, v_bin in self.db.items():
                k = k_bin.decode('utf-8')
                v = v_bin.decode('utf-8')
                # Ensure dumped value also respects normalization (though get handles it)
                normalized_v = v.replace('\r', '').replace('\n', ' ')
                print(f"{k}\t{normalized_v}")
            return True
        except DBError as e:
            sys.stderr.write(f"Error dumping database: {e}\n")
            return False

    def restore(self, input_file_path: str, force: bool = False) -> bool:
        """
        Restores key-value pairs from a file in 'key\\tvalue' format.
        Returns True on success, False on failure.
        """
        count = 0
        try:
            with open(input_file_path, 'r', encoding='utf-8') as f:
                for line_num, line in enumerate(f, 1): # Start line numbering from 1
                    line = line.rstrip('\n')
                    if not line:
                        continue

                    parts = line.split('\t', 1)
                    if len(parts) != 2:
                        sys.stderr.write(
                            f"Warning: Line {line_num} in '{input_file_path}' has invalid format. Skipping: '{line}'\n")
                        continue

                    input_key, input_value = parts[0], parts[1]

                    # Use the set method's internal error handling for consistency
                    if self.set(input_key, input_value, force=force):
                        count += 1
            sys.stderr.write(f"Successfully restored {count} key-value pairs from '{input_file_path}'.\n")
            return True
        except FileNotFoundError:
            sys.stderr.write(f"Error: Input file '{input_file_path}' not found.\n")
            return False
        except OSError as e: # Catch other potential file I/O errors
            sys.stderr.write(f"Error reading input file '{input_file_path}': {e}\n")
            return False
        except DBError as e:
            sys.stderr.write(f"Error restoring database from '{input_file_path}': {e}\n")
            return False

def main():
    """Main function for the bdb tool command-line interface."""
    parser = argparse.ArgumentParser(
        description="A utility for interacting with Berkeley DB (bsddb3) files.",
        formatter_class=argparse.RawTextHelpFormatter
    )

    # Global options for database file selection
    db_group = parser.add_mutually_exclusive_group(required=True)
    db_group.add_argument('--db', type=str,
        help="Specify the Berkeley DB file path.")
    db_group.add_argument('--temp-db', action='store_true',
        help="Use a temporary Berkeley DB file (for testing/temporary operations).\n"
             "The database and its associated files will be automatically cleaned up on exit.")

    parser.add_argument('--force', action='store_true',
        help="Force operations (e.g., overwrite existing keys during set/rename/restore).")

    # Note: --no-lock was in the original but bsddb3.hashopen doesn't directly support
    # a 'no-lock' flag in its open signature like original Berkeley DB's C API might.
    # If locking control is critical, it would require deeper interaction with the
    # underlying DB_ENV or DB_LOCK functions, which is beyond the scope of simple shelve/hashopen.
    # For now, it's removed to avoid suggesting functionality not directly supported
    # by bsddb3.hashopen.
    # parser.add_argument('--no-lock', action='store_true',
    #     help="Disable file locking (use with caution).")

    # Subcommands
    subparsers = parser.add_subparsers(dest='command', required=True, help='Available commands')

    # Set subcommand
    parser_set = subparsers.add_parser('set', help='Set (put) a key-value pair.')
    parser_set.add_argument('KEY', type=str, help='The key.')
    parser_set.add_argument('VALUE', type=str, help='The value.')

    # Get subcommand
    parser_get = subparsers.add_parser('get', help='Get the value for a specific key.')
    parser_get.add_argument('KEY', type=str, help='The key.')

    # Delete subcommand
    parser_delete = subparsers.add_parser('delete', help='Delete a key from the database.')
    parser_delete.add_argument('KEY', type=str, help='The key to delete.')

    # Rename subcommand
    parser_rename = subparsers.add_parser('rename', help='Rename an existing key.')
    parser_rename.add_argument('OLD_KEY', type=str, help='The current key.')
    parser_rename.add_argument('NEW_KEY', type=str, help='The new key.')

    # Dump subcommand
    _parser_dump = subparsers.add_parser('dump',
        help='Dump all key-value pairs to standard output ("key\\tvalue" format).')

    # Restore subcommand
    parser_restore = subparsers.add_parser('restore',
        help='Restore key-value pairs from a file ("key\\tvalue" format).')
    parser_restore.add_argument('INPUT_FILE', type=str, help='The path to the input file.')

    args = parser.parse_args()

    db_file_path: str
    temp_db_dir: Optional[str] = None

    if args.temp_db:
        # Create a temporary directory and use a file inside it for the DB
        temp_db_dir = tempfile.mkdtemp()
        db_file_path = os.path.join(temp_db_dir, 'temp_bdb_base')
        sys.stderr.write(
            f"Using temporary database in: {temp_db_dir} (base name: {db_file_path})\n")
    else: # --db was specified
        db_file_path = args.db

    # Use the BDBTool as a context manager for automatic closing
    with BDBTool(db_file_path) as tool:
        # If using a temporary database, store the temp directory for cleanup by the tool
        if args.temp_db:
            tool._temp_dir_to_clean = temp_db_dir

        success = True # Flag to track if the command executed successfully

        # Execute commands
        if args.command == 'set':
            success = tool.set(args.KEY, args.VALUE, args.force)
        elif args.command == 'get':
            value = tool.get(args.KEY)
            if value is not None:
                print(value)
            else:
                sys.stderr.write(f"Error: Key '{args.KEY}' not found.\n")
                success = False
        elif args.command == 'delete':
            success = tool.delete(args.KEY)
        elif args.command == 'rename':
            success = tool.rename(args.OLD_KEY, args.NEW_KEY, args.force)
        elif args.command == 'dump':
            success = tool.dump()
        elif args.command == 'restore':
            success = tool.restore(args.INPUT_FILE, force=args.force)
        else:
            parser.print_help()
            success = False # Should not happen with required=True for command

        if not success:
            sys.exit(1) # Exit with error code if any command failed

if __name__ == "__main__":
    main()

# depends:
#   apt install python3-bsddb3
#
# v0.1.0  2024/05/25  first release.
# v0.2.0  2024/05/27  Improvements: Error handling, context manager, temp file cleanup.
#
# vim: set ts=4 sw=4 sts=0 expandtab:
