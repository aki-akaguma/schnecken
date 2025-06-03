#!/usr/bin/env perl

use 5.028;    # Explicitly use Perl 5.28 (enables strict and warnings by default)
use warnings;
use BerkeleyDB;
use Getopt::Long;
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

sub podman {
    my (%h) = @_;
    ## no critic (BuiltinFunctions::ProhibitStringyEval)
    my $_r = eval "use Pod::Usage 'pod2usage';";
    ## use critic
    pod2usage(%h);
    return;
}

# Display help message
sub show_help {
    podman(-exitval => 0, -verbose => 2);
    return;
}

# Display version information
sub show_version {
    print "bdb-tool.pl version $VERSION\n";
    exit 0;
}

# Open the database
sub open_db {
    my ($path, $flags) = @_;
    my $db;
    my $status = tie %$db, 'BerkeleyDB::Hash',
      -Filename => $path,
      -Flags    => $flags;
    unless ($status) {
        die "Failed to open database: $path [$!] $BerkeleyDB::Error\n";
    }
    return $db;
}

# --- Command Implementations ---

# get command
sub cmd_get {
    my ($args_ref) = @_;
    my ($key)      = @$args_ref;
    unless (defined $key) {
        die "The 'get' command requires a key.\n";
    }
    my $db = open_db($db_path, DB_RDONLY);
    if (exists $db->{$key}) {
        print "$key: $db->{$key}\n";
    }
    else {
        print "$key: (not found)\n";
    }
    return;
}

# set command
sub cmd_set {
    my ($args_ref) = @_;
    my ($key, $value) = @$args_ref;
    unless (defined $key && defined $value) {
        die "The 'set' command requires a key and a value.\n";
    }
    my $db = open_db($db_path, DB_CREATE);
    $db->{$key} = $value;
    print "Set: $key => $value\n";
    return;
}

# delete command
sub cmd_delete {
    my ($args_ref) = @_;
    my ($key)      = @$args_ref;
    unless (defined $key) {
        die "The 'delete' command requires a key.\n";
    }
    my $db = open_db($db_path, 0);
    if (exists $db->{$key}) {
        delete $db->{$key};
        print "Deleted: $key\n";
    }
    else {
        print "$key: (not found, could not delete)\n";
    }
    return;
}

# rename command
sub cmd_rename {
    my ($args_ref) = @_;
    my ($oldkey, $newkey) = @$args_ref;
    unless (defined $oldkey && defined $newkey) {
        die "The 'rename' command requires an old key and a new key.\n";
    }
    my $db = open_db($db_path, 0);
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

# dump command (outputs in key\tvalue format for easier parsing by restore)
sub cmd_dump {
    my $db = open_db($db_path, DB_RDONLY);
    while (my ($key, $value) = each %$db) {
        print "$key\t$value\n";
    }
    return;
}

# restore command
sub cmd_restore {
    my ($args_ref)  = @_;
    my ($file_path) = @$args_ref;
    unless (defined $file_path) {
        die "The 'restore' command requires a file path to restore from.\n";
    }
    my $db    = open_db($db_path, DB_CREATE);
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

# count command
sub cmd_count {
    my $db    = open_db($db_path, DB_RDONLY);
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
#   v0.1.0  2024/05/24  first release.
#
# support on:
#   perltidy -b -l 100 --check-syntax --paren-tightness=2
#   perlcritic -4
# vim: set ts=4 sw=4 sts=0 expandtab:
