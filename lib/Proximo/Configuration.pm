#!/usr/bin/perl

package Proximo::Configuration;

use strict;
use Proximo::MySQL::Cluster;
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
    my $ctx = { errors_fatal => 1 };
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

    # FIXME: the management command system is really not well done, Perlbal does this
    # much better... might want to get some tips there.  the big thing is that we need
    # to send errors back to the client who is issuing the commands instead of just
    # having them see 'Failed.'

    # helper for getting a service name, can also return a cluster, both use the same
    # syntax for SET commands so this works okay
    my $svc_from_name = sub {
        my $svc_name = shift;
        return Proximo::warn( 'Unable to determine service to use for management command.' )
            unless $svc_name;

        # now get the service or the cluster
        my $svc = Proximo::Service->GetServiceByName( $svc_name ) ||
                  Proximo::Cluster->GetClusterByName( $svc_name );
        return Proximo::warn( 'No service or cluster named %s defined.', $svc_name )
            unless $svc;

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

        return 1;
    
    # create a cluster
    } elsif ( $cmd =~ /^create\s+(\w+?)\s+cluster\s+([\w\d]+)$/i ) {
        my ( $type, $name ) = ( lc $1, $2 );

        # make a new service
        if ( $type eq 'mysql' ) {
            $ctx->{cur_service} = Proximo::MySQL::Cluster->new( $name );
        } else {
            return Proximo::warn( 'Cluster type %s unknown to create cluster named %s.', $type, $name );
        }

        return 1;

    # setting the value of something
    } elsif ( $cmd =~ /^(?:set\s+)?(?:([\w\d]+)\.)?([\w\d]+)\s*=\s*(.+)$/i ) {
        my ( $svc_name, $name, $val ) = ( $1, $2, $3 );

        # try to get the name if we can
        $svc_name ||= $ctx->{cur_service}->name
            if $ctx->{cur_service};

        # now set
        my $svc = $svc_from_name->( $svc_name )
            or return undef;
        return $svc->set( $name, $val );

    # turning a service on or off
    } elsif ( $cmd =~ /^enable(?:\s+([\w\d]+))?$/i ) {
        my $name = $1;

        # again with the names
        $name ||= $ctx->{cur_service}->name
            if $ctx->{cur_service};
        my $svc = $svc_from_name->( $name )
            or return undef;

        # main screen turn on (comment OCD strikes again)
        return $svc->enable;

    } else {
        if ( $ctx->{errors_fatal} ) {
            Proximo::fatal( 'Unknown configuration command: %s.', $cmd );
        } else {
            return Proximo::warn( 'Unknown configuration command: %s.', $cmd );
        }
    }

    return Proximo::warn( 'Management executor fell through to bottom.' );
}

1;