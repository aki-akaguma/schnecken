#!/usr/bin/env perl

use v5.28;    # Explicitly use Perl 5.28 (enables strict and warnings by default)
use Test::More; # Import the Test::More module for testing
use File::Temp qw(tempfile tempdir); # For creating temporary files and directories
use File::Basename; # For basename in cleanup

# --- Test Setup ---
my $temp_dir = tempdir(CLEANUP => 1);
#my ($temp_db_fh, $temp_db_path) = tempfile(UNLINK => 1, SUFFIX => '.db', DIR => $temp_dir);
#close $temp_db_fh; # Close the filehandle immediately, we just need the path
#my ($temp_backup_fh, $temp_backup_path) = tempfile(UNLINK => 1, SUFFIX => '.txt', DIR => $temp_dir);
#close $temp_backup_fh; # Close the filehandle immediately
my $temp_db_path = "$temp_dir/tmp.db";
my $temp_backup_path = "$temp_dir/tmp.txt";

# Determine the path to the main script
my $script = File::Basename::dirname(__FILE__) . '/../bin/bdb-tool-txn.pl';

# Ensure the script exists and is executable
-e $script or die "Test script '$script' not found!";
-x $script or die "Test script '$script' is not executable! Please `chmod +x $script`";

# --- Test Plan ---
plan tests => 15; # Adjust this number based on how many tests you have

# --- Helper function to run the main script ---
sub run_script {
    my (@args) = @_;
    # Prepend the database path argument
    unshift @args, '-d', $temp_db_path;
    # Execute the script and capture STDOUT and STDERR
    my $output = `$^X $script @args 2>&1`;
    return $output;
}

# --- Test Cases ---

# 1. Test 'version' command
my $version_output = run_script('--version');
like($version_output, qr/bdb-tool\.pl version \d+\.\d+\.\d+/, "Version command works");

# 2. Test 'set' command
my $set_output = run_script('set', 'key1', 'value1');
is($set_output, "Set: key1 => value1\n", "Set: key1=value1");

# 3. Test 'get' command (existing key)
my $get_output = run_script('get', 'key1');
is($get_output, "key1: value1\n", "Get: key1 returns value1");

# 4. Test 'get' command (non-existing key)
my $get_non_exist_output = run_script('get', 'non_exist_key');
is($get_non_exist_output, "non_exist_key: (not found)\n", "Get: non_exist_key returns (not found)");

# 5. Test 'count' command (1 entry)
my $count_output = run_script('count');
is($count_output, "Number of elements: 1\n", "Count: 1 element after set");

# 6. Test 'set' another key
run_script('set', 'key2', 'value2');
my $set_output_2 = run_script('set', 'key2', 'value2'); # Just to ensure it doesn't fail
is($set_output_2, "Set: key2 => value2\n", "Set: key2=value2");

# 7. Test 'count' command (2 entries)
$count_output = run_script('count');
is($count_output, "Number of elements: 2\n", "Count: 2 elements after second set");

# 8. Test 'rename' command
my $rename_output = run_script('rename', 'key1', 'newkey1');
is($rename_output, "Renamed key: key1 => newkey1\n", "Rename: key1 to newkey1");

# 9. Test 'get' command (renamed key)
$get_output = run_script('get', 'newkey1');
is($get_output, "newkey1: value1\n", "Get: newkey1 returns value1 after rename");

# 10. Test 'get' command (old key after rename)
$get_non_exist_output = run_script('get', 'key1');
is($get_non_exist_output, "key1: (not found)\n", "Get: old key1 returns (not found) after rename");

# 11. Test 'dump' command
my $dump_output = run_script('dump');
# The order might vary, so check for both lines in any order
ok($dump_output =~ qr/newkey1\tvalue1\n/ && $dump_output =~ qr/key2\tvalue2\n/, "Dump: contains both key-value pairs");

# 12. Test 'dump' to file and 'restore' from file
open my $fh, '>', $temp_backup_path or die "Could not open temporary backup file: $!";
print $fh $dump_output;
close $fh;

# Clear the database for restore test
run_script('delete', 'newkey1');
run_script('delete', 'key2');
my $count_zero = run_script('count');
is($count_zero, "Number of elements: 0\n", "Database cleared for restore test");

my $restore_output = run_script('restore', $temp_backup_path);
like($restore_output, qr/Restoring from '.*\.txt' to '.*\.db'\.\.\.\nRestore complete\. 2 entries restored\.\n/, "Restore: command works and reports 2 entries restored");

# 13. Verify restored data
$count_output = run_script('count');
is($count_output, "Number of elements: 2\n", "Count: 2 elements after restore");

# 14. Test 'delete' command
my $delete_output = run_script('delete', 'newkey1');
is($delete_output, "Deleted: newkey1\n", "Delete: newkey1");

# --- Cleanup (optional, as tempfile with UNLINK => 1 handles most cases) ---
# You can add explicit unlink here if tempfile UNLINK isn't sufficient for some reason
# unlink $temp_db_path;
# unlink $temp_backup_path;

