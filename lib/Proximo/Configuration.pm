#!/usr/bin/perl

package Proximo::Configuration;

use strict;
use Proximo::MySQL::Service;
use Proximo::Management::Service;

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
    if ( $cmd =~ /^create\s+(\w+?)\s+service\s+([\w\d]+)$/i ) {
        my ( $type, $name ) = ( lc $1, $2 );

        # make a new service
        if ( $type eq 'mysql' ) {
            $ctx->{cur_service} = Proximo::MySQL::Service->new( $name );
        } elsif ( $type eq 'management' ) {
            $ctx->{cur_service} = Proximo::Management::Service->new( $name );
        } else {
            return Proximo::warn( 'Service type %s unknown to create service named %s.', $type, $name );
        }

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

        # again with the names
        $name ||= $ctx->{cur_service}->name
            if $ctx->{cur_service};
        my $svc = $svc_from_name->( $name )
            or return undef;

        # main screen turn on (comment OCD strikes again)
        $svc->enable;

    } else {
        Proximo::warn( 'Unknown configuration command: %s.', $cmd );

    }
}

1;