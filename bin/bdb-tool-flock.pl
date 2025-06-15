#!/usr/bin/env perl

use 5.028;    # Explicitly use Perl 5.28 (enables strict and warnings by default)
use warnings;
use BerkeleyDB;
use Getopt::Long;
use Fcntl qw(:flock O_RDWR O_CREAT);
use Try::Tiny;

# --- Configuration ---
my $VERSION = "0.1.0";    # Define the version number here

# Global variable for database path
my $db_path;

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
# primarily used for displaying help and man pages.It includes a
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
# @brief Opens a BerkeleyDB database and acquires a file lock.
#
# This function handles the opening of a BerkeleyDB hash database.
# It also acquires an exclusive file lock on a companion lock file
# to prevent concurrent access issues.
#
# @param $path The file path to the BerkeleyDB database.
# @param $flags Flags for opening the database (e.g., `DB_RDONLY`, `DB_CREATE`).
#
# @returns ($db_tie_object, $flock_filehandle) A tuple containing the tied database
#          object and the file handle for the acquired lock.
# @throws Dies if the lock file cannot be opened or locked, or if the database
#         fails to open.
sub open_db {
    my ($path, $flags) = @_;
    ##
    my $flock_path = "$path.flock";
    ## no critic (InputOutput::RequireBriefOpen)
    open(my $flock, '>>', $flock_path)
      or croak("Cannot open lock file: $flock_path [$!]");
    ## use critic
    flock($flock, LOCK_EX)
      or croak("Cannot get exclusive lock on file: $flock_path [$!]");
    ##
    my $db;
    my $status = tie %$db, 'BerkeleyDB::Hash',
      -Filename => $path,
      -Flags    => $flags;
    unless ($status) {
        die("Failed to open database: $path [$!] $BerkeleyDB::Error\n");
    }
    return ($db, $flock);
}

# --- Command Implementations ---

##
# @brief Implements the 'get' command to retrieve a value from the database.
#
# Retrieves and prints the value associated with a given key from the
# BerkeleyDB database. If the key is not found, it indicates so.
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
    my ($db, $flock) = open_db($db_path, DB_RDONLY);
    if (exists $db->{$key}) {
        print "$key: $db->{$key}\n";
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
# If the key already exists, its value will be updated.
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
    my ($db, $flock) = open_db($db_path, DB_CREATE);
    $db->{$key} = $value;
    print "Set: $key => $value\n";
    return;
}

##
# @brief Implements the 'delete' command to remove a key-value pair from the database.
#
# Deletes a specified key and its associated value from the BerkeleyDB database.
# If the key is not found, it indicates that it could not delete.
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
    my ($db, $flock) = open_db($db_path, 0);
    if (exists $db->{$key}) {
        delete $db->{$key};
        print "Deleted: $key\n";
    }
    else {
        print "$key: (not found, could not delete)\n";
    }
    return;
}

##
# @brief Implements the 'rename' command to change a key's name in the database.
#
# Renames an existing key to a new key. The value associated with the old key
# is transferred to the new key.
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
    my ($db, $flock) = open_db($db_path, 0);
    if (exists $db->{$oldkey}) {
        if (exists $db->{$newkey}) {
            my $val = $db->{$newkey};
            die "Already exists: $newkey => $val\n";
        }
        $db->{$newkey} = $db->{$oldkey};
        delete $db->{$oldkey};
        print "Renamed key: $oldkey => $newkey\n";
    }
    else {
        print "$oldkey: (not found, could not rename)\n";
    }
    return;
}

##
# @brief Implements the 'dump' command to output all key-value pairs from the database.
#
# Iterates through all entries in the BerkeleyDB database and prints
# each key-value pair to standard output, separated by a tab character.
# This format is suitable for input to the 'restore' command.
#
# @returns void
sub cmd_dump {
    my ($db, $flock) = open_db($db_path, DB_RDONLY);
    while (my ($key, $value) = each %$db) {
        print "$key\t$value\n";
    }
    return;
}

##
# @brief Implements the 'restore' command to load key-value pairs from a file into the database.
#
# Reads key-value pairs from a specified file (expected to be tab-separated,
# as generated by `cmd_dump`) and inserts them into the BerkeleyDB database.
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
    my ($db, $flock) = open_db($db_path, DB_CREATE);
    my $count = 0;
    print "Restoring from '$file_path' to '$db_path'...\n";
    do {
        open my $fh, '<', $file_path
          or die "Could not open file '$file_path' for reading: $!\n";
        $count = _cmd_restore_proc1($db, $fh);
        close $fh;
    };
    print "Restore complete. $count entries restored.\n";
    return;
}

##
# @brief Helper function for the 'restore' command to process a file handle.
#
# Reads lines from a given file handle, parses them as tab-separated
# key-value pairs, and inserts them into the provided BerkeleyDB database object.
# It skips malformed lines and reports them with a warning.
#
# @param $db A tied BerkeleyDB hash object where data will be restored.
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
            $db->{$key} = $value;
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
# stored within it.
#
# @returns void
sub cmd_count {
    my ($db, $flock) = open_db($db_path, DB_RDONLY);
    my $count = 0;
    while (my ($key, $value) = each %$db) {
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
    my $opt_db_path;

    GetOptions(
        'help|h'    => \$help,
        'man'       => \$man,
        'version|v' => \$version,       # Add version option
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

    # -d option is mandatory unless -h, --man, or -v are used
    unless (defined $opt_db_path) {
        warn "Error: Database path (-d option) is not specified.\n";
        podman(-exitval => 1);
    }
    $db_path = $opt_db_path;

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
   ./bdb-tool.pl -d /tmp/my_db.db set mykey myvalue
   ./bdb-tool.pl -d /tmp/my_db.db get mykey
   ./bdb-tool.pl -d /tmp/my_db.db delete mykey
   ./bdb-tool.pl -d /tmp/my_db.db rename oldkey newkey
   ./bdb-tool.pl -d /tmp/my_db.db dump > backup.txt
   ./bdb-tool.pl -d /tmp/my_db_new.db restore backup.txt
   ./bdb-tool.pl -d /tmp/my_db.db count
   ./bdb-tool.pl -v
   ./bdb-tool.pl --version

=head1 OPTIONS

=over 8

=item B<-h, --help>

Displays a brief help message and exits.

=item B<--man>

Displays the full manual page and exits.

=item B<-v, --version>

Displays the script version and exits.

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
#   v0.1.0  2024/05/30  first release.
#
# support on:
#   perltidy -b -l 100 --check-syntax --paren-tightness=2
#   perlcritic -3 --verbose 9
#
# vim: set ts=4 sw=4 sts=0 expandtab:
