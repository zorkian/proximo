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

    # helper for getting a service name
    my $svc_from_name = sub {
        my $svc_name = shift;
        unless ( $svc_name ) {
            Proximo::warn( 'Unable to determine service to use for management command.' );
            return undef;
        }

        # now get the service
        my $svc = Proximo::Service->GetServiceByName( $svc_name );
        unless ( $svc ) {
            Proximo::warn( 'Service %s not defined in set.', $svc_name );
            return undef;
        }
        
        return $svc;
    };

    # this is a very straightforward pattern match on commands...
    if ( $cmd =~ /^create\s+service\s+([\w\d]+)$/i ) {
        my $name = $1;

        # make a new service
        Proximo::info( 'Creating service %s.', $name );
        my $svc = Proximo::Service->new( $name );
        $ctx->{cur_service} = $svc;

    # setting the value of something
    } elsif ( $cmd =~ /^(?:([\w\d]+)\.)?([\w\d]+)\s*=\s*(.+)$/ ) {
        my ( $svc_name, $name, $val ) = ( $1, $2, $3 );

        # try to get the name if we can
        $svc_name ||= $ctx->{cur_service}->name
            if $ctx->{cur_service};

        # now set
        my $svc = $svc_from_name->( $svc_name )
            or return undef;
        $svc->set( $name, $val );

    } elsif ( $cmd =~ /^enable(?:\s+([\w\d]+))?$/i ) {
        my $name = $1;
        $name ||= $ctx->{cur_service}->name
            if $ctx->{cur_service};
        my $svc = $svc_from_name->( $name )
            or return undef;

        $svc->enable;

    } else {
        Proximo::warn( 'Unknown configuration command: %s', $cmd );
    }
}

1;