#!/usr/bin/perl

package Proximo::Configuration;

use strict;

# load a configuration file
sub load_config_file {
    my $fn = shift;

    # ensure we have a valid file and we can open it
    Proximo::fatal( 'Configuration file %s not found.', $fn )
        unless -e $fn;
    open FILE, "<$fn"
        or Proximo::fatal( 'Failed opening configuration file: ##' );

    # now execute each line
    my $ctx = {};
    while (<FILE>) {
        s/[\r\n]+$//;
        s/\s*#.*$//;
        s/^\s+//;
        s/\s+$//;
        next unless $_;

        Proximo::Configuration::exec_management_command( $ctx, $_ );
    }

    # close and we're done
    close FILE;
}

# given a management command, run it
sub exec_management_command {
    # optional first parameter allows us to keep some context between invocations
    # if the caller wants to support that
    my ( $ctx, $cmd ) = ( {}, shift );
    if ( ref $cmd ) {
        ( $ctx, $cmd ) = ( $cmd, shift );
    }

    # this is a very straightforward pattern match on commands...
    if ( $cmd =~ /^create\s+service\s+([\w\d]+)$/i ) {
        my $name = $1;
        Proximo::info( 'Creating service %s.', $name );
        $ctx->{cur_service_name} = $name;

    # setting the value of something
    } elsif ( $cmd =~ /^(?:([\w\d]+)\.)?([\w\d]+)\s*=\s*(.+)$/ ) {
        my ( $svc, $name, $val ) = ( $1, $2, $3 );
        $svc ||= $ctx->{cur_service_name};
        Proximo::fatal( 'Unable to determine service to set %s on.', $name )
            unless $svc;

    } elsif ( $cmd =~ /^enable\s+([\w\d]+)$/i ) {
        my $name = $1;

    } else {
        Proximo::warn( 'Unknown configuration command: %s', $cmd );
    }
}

1;