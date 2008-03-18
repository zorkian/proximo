#!/usr/bin/perl
#
# utility file with various utility functions

package Proximo::MySQL::Util;

use strict;
use Digest::SHA1;

# xor function, taken from DBIx::MyServer by Philip Stoev <philip@stoev.org>
sub _xor {
    my ( $s1, $s2 ) = ( $_[0], $_[1] );

    # empty result, do the iteration, and go
    # FIXME: can't we just use chr/ord here...?  or does that have implications on
    # characterset/encoding/whatever?
    my $result = '';
    for my $i ( 0 .. length( $s1 ) - 1 ) {
        $result .= pack 'C', ( unpack( 'C', substr( $s1, $i, 1 ) ) ^
                               unpack( 'C', substr( $s2, $i, 1 ) ) );
    }
    return $result;
}

# the password stages are the version that is actually stored in the database
# and is used in both encryption and decryption
sub get_password_stages {
    my $password = $_[0];

    # here there be SHA1
    my $sha = Digest::SHA1->new;

    # stage 1: of the password
    $sha->reset;
    $sha->add( $password );
    my $stg1 = $sha->digest;

    # stage 2: of the stage 1
    $sha->reset;
    $sha->add( $stg1 );
    my $stg2 = $sha->digest;

    # return both
    return ( $stg1, $stg2 );
}

# given a scramble buffer and a password, return the resultant digest
sub get_password_digest {
    my ( $scramble, $password ) = ( $_[0], $_[1] );

    # if no password then return empty, not undef!
    return ''
        unless defined $password && length( $password ) > 0;

    # get the stages
    my ( $stage1, $stage2 ) = get_password_stages( $password );

    # here there be more SHA1
    my $sha = Digest::SHA1->new;

    # intermediate result: salt and stage 2
    $sha->reset;
    $sha->add( $scramble );
    $sha->add( $stage2 );
    my $intres = $sha->digest;

    # return the xor'd value
    return _xor( $intres, $stage1 );
}

# verifies a password...
sub verify_password {
    my ( $scramble, $user, $password ) = ( $_[0], $_[1], $_[2] );

    # simply redo the work the client did and see if it matches
    my $hash = get_password_digest( $scramble, $password );
    return 1
        if $hash eq $user;
    return 0;
}

1;