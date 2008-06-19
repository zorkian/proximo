#!/usr/bin/perl
#
# simple benchmarking tool... must be configured inline, doesn't use command line
# or anything else yet.  just for testing the raw connection speeds of a given setup
# and not the underlying MySQL instance...

use strict;
use DBI;
use Time::HiRes qw( gettimeofday tv_interval );

# configuration such as it is goes here
my $prox = 'DBI:mysql:database=test;host=127.0.0.1;port=2306';
my $mysql = 'DBI:mysql:database=test;host=127.0.0.1;port=3306';
my $thread = 1;
my $queries = 5000;

my $which = shift;
my $dbi;
if ( $which =~ /prox/i ) { $which = 'Proximo'; $dbi = $prox; } else { $which = 'MySQL'; $dbi = $mysql; }

# spawn children for us 
foreach ( 1..$thread-1 ) { fork && last; }

# okay, start spamming straight to proximo...
#print "[$$] Benchmarking Proximo...\n";

my $start = [ gettimeofday ];
foreach ( 1..$queries ) {
    my $dbh = DBI->connect( $dbi )
        or die "can't connect: ". DBI->errstr . "\n";
    my $ref = $dbh->selectrow_array('SELECT 234');
    die "uh, didn't get 234\n"
        unless $ref == 234;
}
my $rv = tv_interval( $start );
printf "[$$ $which] $queries queries in \%0.2f seconds: \%0.3f spq, \%0.2f qps.\n", $rv, $rv/$queries, $queries/$rv;

#sleep ;
