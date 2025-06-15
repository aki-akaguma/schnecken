#!/usr/bin/env perl

use 5.028;    # Explicitly use Perl 5.28 (enables strict and warnings by default)
use warnings;
use BerkeleyDB;
use Getopt::Long;
use File::Path qw/mkpath/;
use File::Basename;
use Try::Tiny;

# --- Configuration ---
my $VERSION = "0.1.0";    # Define the version number here

# Global variable for database path
my $env_home;
my $db_path;
my $env;

# Command dispatch table
my %commands = (
    'get'     => \&cmd_get,
    'set'     => \&cmd_set,
    'delete'  => \&cmd_delete,
    'rename'  => \&cmd_rename,
    'dump'    => \&cmd_dump,
    'restore' => \&cmd_restore,
    'count'   => \&cmd_count,
);

# perlcritic settings
## no critic (RegularExpressions::RequireExtendedFormatting)

##
# @brief Executes pod2usage with given options.
#
# This function is a wrapper around Pod::Usage's pod2usage function,
# primarily used for displaying help and man pages. It includes a
# `no critic` directive to allow the use of `eval` for dynamic
# module loading, which is sometimes necessary for Pod::Usage.
#
# @param %h A hash of options to pass to `pod2usage`.
#
# @returns void
sub podman {
    my (%h) = @_;
    ## no critic (BuiltinFunctions::ProhibitStringyEval)
    my $_r = eval "use Pod::Usage 'pod2usage';";
    ## use critic
    pod2usage(%h);
    return;
}

##
# @brief Displays the help message for the script.
#
# This function uses `podman` (which in turn uses `Pod::Usage`)
# to display the embedded Pod documentation as a help message.
# The script exits after displaying the help.
#
# @returns void
sub show_help {
    podman(-exitval => 0, -verbose => 2);
    return;
}

##
# @brief Displays the version information of the script.
#
# Prints the current version number of the script to standard output
# and then exits.
#
# @returns void
sub show_version {
    print "bdb-tool.pl version $VERSION\n";
    exit 0;
}

##
# @brief Opens a BerkeleyDB database, optionally within an environment.
#
# This function handles the opening of a BerkeleyDB hash database.
# If a global `$env_home` path is defined, it will first create
# and open a BerkeleyDB environment, ensuring proper transaction
# and concurrency management. It then opens the specified database
# within this environment.
#
# @param $path The filename of the BerkeleyDB database.
# @param $flags Flags for opening the database (e.g., `DB_RDONLY`, `DB_CREATE`).
#
# @returns BerkeleyDB::Hash object The opened BerkeleyDB database object.
# @throws Dies if the environment cannot be created/opened,
#         or if the database fails to open.
sub open_db {
    my ($path, $flags) = @_;
    if (defined $env_home) {
        unless (-d $env_home) {
            mkpath($env_home, 0, 0o774);
        }
        $env = BerkeleyDB::Env->new(
            -Home       => $env_home,
            -Flags      => DB_CREATE | DB_INIT_MPOOL | DB_INIT_CDB,
            -MaxLockers => 10,
            -MaxLocks   => 10,
            -MaxObjects => 10,
            -LockDetect => DB_LOCK_DEFAULT,    # Default: abort one conflicting locker
            -Mode       => 0o644,
        ) or die "Cannot create DB environment: $env_home [$!] $BerkeleyDB::Error\n";
    }
    my $db = BerkeleyDB::Hash->new(
        -Filename => $path,
        -Flags    => $flags,
        -Env      => $env,
        -Mode     => 0o644
    ) or die "Failed to open database: $path [$!] $BerkeleyDB::Error\n";
    return $db;
}

# --- Command Implementations ---

##
# @brief Implements the 'get' command to retrieve a value from the database.
#
# Retrieves and prints the value associated with a given key from the
# BerkeleyDB database. If the key is not found, it indicates so.
# This version uses `db_get` for direct interaction with BerkeleyDB.
#
# @param $args_ref A reference to an array containing the key to retrieve.
#                  Expected: `[ $key ]`.
#
# @returns void
# @throws Dies if the key is not provided.
sub cmd_get {
    my ($args_ref) = @_;
    my ($key)      = @$args_ref;
    unless (defined $key) {
        die "The 'get' command requires a key.\n";
    }
    my $status = undef;
    my $db     = open_db($db_path, DB_RDONLY);
    my $value  = '';
    $status = $db->db_get($key, $value);
    if ($status == 0) {
        print "$key: $value\n";
    }
    else {
        print "$key: (not found)\n";
    }
    return;
}

##
# @brief Implements the 'set' command to store a key-value pair in the database.
#
# Sets a specified value for a given key in the BerkeleyDB database.
# If the key already exists, its value will be updated. This command
# uses `db_put` and includes a `cds_lock` for concurrent data store safety.
#
# @param $args_ref A reference to an array containing the key and value to set.
#                  Expected: `[ $key, $value ]`.
#
# @returns void
# @throws Dies if either the key or value is not provided.
sub cmd_set {
    my ($args_ref) = @_;
    my ($key, $value) = @$args_ref;
    unless (defined $key && defined $value) {
        die "The 'set' command requires a key and a value.\n";
    }
    my $status = undef;
    my $db     = open_db($db_path, DB_CREATE);
    my $lock   = $db->cds_lock();

    #$status = $db->status(); print "AAA: $status\n";
    $status = $db->db_put($key, $value);
    if ($status != 0) {
        warn "db_put($key, $value): $status\n";
    }
    $lock->cds_unlock();

    #$status = $db->status(); print "BBB: $status\n";
    print "Set: $key => $value\n";
    return;
}

##
# @brief Implements the 'delete' command to remove a key-value pair from the database.
#
# Deletes a specified key and its associated value from the BerkeleyDB database.
# If the key is not found, it indicates that it could not delete.
# Uses `db_exists` and `db_del` and includes `cds_lock` for safety.
#
# @param $args_ref A reference to an array containing the key to delete.
#                  Expected: `[ $key ]`.
#
# @returns void
# @throws Dies if the key is not provided.
sub cmd_delete {
    my ($args_ref) = @_;
    my ($key)      = @$args_ref;
    unless (defined $key) {
        die "The 'delete' command requires a key.\n";
    }
    my $status = undef;
    my $db     = open_db($db_path, 0);
    my $lock   = $db->cds_lock();
    $status = $db->db_exists($key);
    if ($status == 0) {
        $status = $db->db_del($key);
        print "Deleted: $key\n";
    }
    else {
        print "$key: (not found, could not delete)\n";
    }
    $lock->cds_unlock();
    return;
}

##
# @brief Implements the 'rename' command to change a key's name in the database.
#
# Renames an existing key to a new key. The value associated with the old key
# is transferred to the new key. It checks for the existence of both keys
# and uses `db_get`, `db_put`, and `db_del` along with `cds_lock`.
#
# @param $args_ref A reference to an array containing the old key and the new key.
#                  Expected: `[ $oldkey, $newkey ]`.
#
# @returns void
# @throws Dies if either the old key or new key is not provided, or if the new key
#         already exists in the database.
sub cmd_rename {
    my ($args_ref) = @_;
    my ($oldkey, $newkey) = @$args_ref;
    unless (defined $oldkey && defined $newkey) {
        die "The 'rename' command requires an old key and a new key.\n";
    }
    my $status = undef;
    my $db     = open_db($db_path, 0);
    my $lock   = $db->cds_lock();
    $status = $db->db_exists($oldkey);
    if ($status == 0) {
        $status = $db->db_exists($newkey);
        if ($status == 0) {
            my $val = '';
            $status = $db->db_get($newkey, $val);
            die "Already exists: $newkey => $val\n";
        }
        my $oldval = '';
        $status = $db->db_get($oldkey, $oldval);
        $status = $db->db_put($newkey, $oldval);
        $status = $db->db_del($oldkey);
        print "Renamed key: $oldkey => $newkey\n";
    }
    else {
        print "$oldkey: (not found, could not rename)\n";
    }
    $lock->cds_unlock();
    return;
}

##
# @brief Implements the 'dump' command to output all key-value pairs from the database.
#
# Iterates through all entries in the BerkeleyDB database using a cursor
# and prints each key-value pair to standard output, separated by a tab character.
# This format is suitable for input to the 'restore' command.
#
# @returns void
sub cmd_dump {
    my $status = undef;
    my $db     = open_db($db_path, DB_RDONLY);
    my $cursor = $db->db_cursor();
    while (1) {
        my $key   = '';
        my $value = '';
        $status = $cursor->c_get($key, $value, DB_NEXT);
        if ($status != 0) {
            last;
        }
        print "$key\t$value\n";
    }
    $status = $cursor->c_close();
    return;
}

##
# @brief Implements the 'restore' command to load key-value pairs from a file into the database.
#
# Reads key-value pairs from a specified file (expected to be tab-separated,
# as generated by `cmd_dump`) and inserts them into the BerkeleyDB database.
# Includes `cds_lock` for the entire restore operation.
#
# @param $args_ref A reference to an array containing the path to the restore file.
#                  Expected: `[ $file_path ]`.
#
# @returns void
# @throws Dies if the file path is not provided or if the file cannot be opened.
sub cmd_restore {
    my ($args_ref)  = @_;
    my ($file_path) = @$args_ref;
    unless (defined $file_path) {
        die "The 'restore' command requires a file path to restore from.\n";
    }
    my $status = undef;
    my $db     = open_db($db_path, DB_CREATE);
    my $count  = 0;
    print "Restoring from '$file_path' to '$db_path'...\n";
    do {
        open my $fh, '<', $file_path
          or die "Could not open file '$file_path' for reading: $!\n";
        my $lock = $db->cds_lock();
        $count = _cmd_restore_proc1($db, $fh);
        $lock->cds_unlock();
        close $fh;
    };
    print "Restore complete. $count entries restored.\n";
    return;
}

##
# @brief Helper function for the 'restore' command to process a file handle.
#
# Reads lines from a given file handle, parses them as tab-separated
# key-value pairs, and inserts them into the provided BerkeleyDB database object
# using `db_put`. It skips malformed lines and reports them with a warning.
#
# @param $db A BerkeleyDB hash object where data will be restored.
# @param $fh A file handle to read the key-value pairs from.
#
# @returns The number of entries successfully restored.
sub _cmd_restore_proc1 {
    my ($db, $fh) = @_;
    my $count = 0;
    while (my $line = <$fh>) {
        chomp $line;

        # Skip empty lines or lines that don't look like key=value
        next unless $line =~ /\t/;

        # Split on the first '\t' to get key and value
        my ($key, $value) = split(/\t/, $line, 2);
        if (defined $key && defined $value) {
            my $status = $db->db_put($key, $value);
            if ($status != 0) {
                warn "db_put($key, $value): $status\n";
            }
            $count++;
        }
        else {
            warn "Skipping malformed line: '$line'\n";
        }
    }
    return $count;
}

##
# @brief Implements the 'count' command to display the number of entries in the database.
#
# Opens the BerkeleyDB database and counts the total number of key-value pairs
# stored within it by iterating through entries using a cursor.
#
# @returns void
sub cmd_count {
    my $db     = open_db($db_path, DB_RDONLY);
    my $count  = 0;
    my $cursor = $db->db_cursor();
    my $key    = '';
    my $value  = '';
    while ($cursor->c_get($key, $value, DB_NEXT) == 0) {
        $count++;
    }
    print "Number of elements: $count\n";
    return;
}

# --- Main Program Flow ---
MAIN: {
    my $help;
    my $man;
    my $version;    # New variable for version option
    my $opt_env_path;
    my $opt_db_path;

    GetOptions(
        'help|h'    => \$help,
        'man'       => \$man,
        'version|v' => \$version,        # Add version option
        'e=s'       => \$opt_env_path,
        'd=s'       => \$opt_db_path,
    ) or podman(-exitval => 2);

    if ($help) {
        show_help();
    }

    if ($man) {
        show_help();
    }

    if ($version) {    # Handle version option
        show_version();
    }

    # -e option
    if (defined $opt_env_path) {
        $env_home = $opt_env_path;
    }

    # -d option is mandatory unless -h, --man, or -v are used
    unless (defined $opt_db_path) {
        warn "Error: Database path (-d option) is not specified.\n";
        podman(-exitval => 1);
    }
    unless (defined $env_home) {
        $env_home = dirname($opt_db_path);
        $db_path  = basename($opt_db_path);
    }
    else {
        $db_path = $opt_db_path;
    }

    my $command = shift @ARGV;
    unless (defined $command) {
        warn "Error: Command is not specified.\n";
        podman(-exitval => 1);
    }

    my $command_func = $commands{$command};
    unless (defined $command_func) {
        warn "Error: Unknown command '$command'.\n";
        podman(-exitval => 1);
    }

    try {
        $command_func->(\@ARGV);
    }
    catch {
        croak("An error occurred during command execution: $_");
    };
}

__END__

=head1 NAME

bdb-tool.pl - A command-line tool for BerkeleyDB operations

=head1 SYNOPSIS

bdb-tool.pl [options] <command> [arguments]

 Examples:
   ./bdb-tool.pl -e /tmp/my_db_home -d my_db.db set mykey myvalue
   ./bdb-tool.pl -e /tmp/my_db_home -d my_db.db get mykey
   ./bdb-tool.pl -e /tmp/my_db_home -d my_db.db delete mykey
   ./bdb-tool.pl -e /tmp/my_db_home -d my_db.db rename oldkey newkey
   ./bdb-tool.pl -e /tmp/my_db_home -d my_db.db dump > backup.txt
   ./bdb-tool.pl -e /tmp/my_db_home -d my_db_new.db restore backup.txt
   ./bdb-tool.pl -e /tmp/my_db_home -d my_db.db count
   ./bdb-tool.pl -v
   ./bdb-tool.pl --version
   ./bdb-tool.pl -d /tmp/my_db_home/my_db.db set mykey myvalue

=head1 OPTIONS

=over 8

=item B<-h, --help>

Displays a brief help message and exits.

=item B<--man>

Displays the full manual page and exits.

=item B<-v, --version>

Displays the script version and exits.

=item B<-e ENV_PATH>

Specifies the path to the BerkeleyDB environment directory. This option is NOT mandatory for database commands.

=item B<-d DB_PATH>

Specifies the path to the BerkeleyDB database file. This option is mandatory for database commands.

=back

=head1 COMMANDS

=over 8

=item B<get KEY>

Retrieves and displays the value corresponding to the specified KEY.

=item B<set KEY VALUE>

Sets VALUE for the specified KEY. If KEY already exists, its value will be overwritten.

=item B<delete KEY>

Deletes the specified KEY and its corresponding value from the database.

=item B<rename OLD_KEY NEW_KEY>

Renames OLD_KEY to NEW_KEY. If OLD_KEY does not exist, an error will occur.

=item B<dump>

Displays all key-value pairs in the database, one per line, in `key\tvalue` format. This output can be redirected to a file for backup.

=item B<restore FILE_PATH>

Restores key-value pairs into the database from the specified file. The file is expected to contain data in `key\tvalue` format, one entry per line (as generated by the `dump` command).

=item B<count>

Displays the number of elements in the database.

=back

=head1 DESCRIPTION

This script provides a simple command-line interface for BerkeleyDB operations using the Perl BerkeleyDB module.
It supports key-value operations (get, set, delete, rename), dumping database contents to a file, restoring from a file, and counting elements.

=head1 AUTHOR

Your Name <your_email@example.com>

=head1 LICENSE

This program is free software; you can redistribute it and/or modify it under the same terms as Perl itself.

=cut

#
# depends:
#   apt install libberkeleydb-perl perl-doc
#
#   v0.1.0  2024/05/24  first release.
#
# support on:
#   perltidy -l 100 --check-syntax --paren-tightness=2
#   perlcritic -3 --verbose 9
#
# vim: set ts=4 sw=4 sts=0 expandtab:
