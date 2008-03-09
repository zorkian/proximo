#!/usr/bin/perl
#
# a service is defined as something that listens on a port and takes incoming
# connects and does something with it.  this usually involves creating another
# class to pass the incoming socket off to.

package Proximo::Service;

use strict;
use Proximo::Service::Listener;
use Proximo::Socket;

use fields (
        'enabled',    # 1/0 if we're enabled or not
        'listen_on',  # array of what we're configured to listen on
        'listeners',  # array of objects listening for us
        'name',       # name of this service
    );
    
# class variables
our ( %Services );

# construct a new Proximo server socket, this accepts new connections and
# allows us to do something with them.
sub new {
    my Proximo::Service $self = shift;
    $self = fields::new( $self ) unless ref $self;

    # arguments are name, and that's about it
    my $name = shift;
    unless ( $name ) {
        Proximo::warn( 'Attempted to create a service with no name!' );
        return undef;
    }        

    # error check
    if ( exists $Services{$name} ) {
        Proximo::warn( 'Attempted to redeclare service with name %s.', $name );
        return undef;
    }    

    # get input arguments and setup
    $self->{name}      = $name;
    $self->{listeners} = [];
    $self->{listen_on} = [];

    # store this service and note it's been built
    $Services{$self->name} = $self;
    Proximo::debug( 'Proximo::Service named %s constructed.', $self->name );

    return $self;
}

# attempt to enable a service, which basically means that we will start
# listening on the given port
sub enable {
    my Proximo::Service $self = shift;

    # if no listener objects, fail
    unless ( scalar( @{ $self->listen_on } ) > 0 ) {
        Proximo::warn( 'Attempted to enable service %s, but no listen defined.', $self->name );
        return undef;
    }

    # if we're already enabled, bail
    if ( $self->enabled ) {
        Proximo::warn( 'Attempted to enable already enabled service %s.', $self->name );
        return 1;
    }

    # now enable the listeners
    foreach my $listen ( @{ $self->listen_on } ) {
        Proximo::debug( 'Spawning listener for service %s on %s.', $self->name, $listen );
        push @{ $self->{listeners} },
            Proximo::Service::Listener->new( $self, $listen );
    }

    return 1;
}

# return our name (if you couldn't tell)
sub name {
    my Proximo::Service $self = $_[0];
    return $self->{name};
}

# set somewhere for us to listen
sub listen {
    my Proximo::Service $self = shift;

    # ensure we got something useful
    my $str = shift;
    Proximo::fatal( 'Proximo::Service->listen() called with no arguments.' )
        unless $str;

    # FIXME: we should be able to change where we listen on the fly...
    if ( scalar( @{ $self->listeners } ) > 0 ) {
        Proximo::warn( 'Attempted to change listen config of running service %s.', $self->name );
        return;
    }

    # split on comma
    foreach my $combo ( split( /,/, $str ) ) {
        # trim and default to localhost
        $combo =~ s/^\s+//;
        $combo =~ s/\s+$//;
        $combo ||= '127.0.0.1:2306';
        $combo = "127.0.0.1:$combo"
            if $combo =~ /^\d+$/;

        # store this address
        push @{ $self->{listen_on} }, $combo;
    }
}

# return arrayref of objects listening for us
sub listeners {
    my Proximo::Service $self = $_[0];
    return $self->{listeners};
}

# return arrayref of addresses we're configured to listen on
sub listen_on {
    my Proximo::Service $self = $_[0];
    return $self->{listen_on};
}

# return whether we're enabled or not
sub enabled {
    my Proximo::Service $self = $_[0];
    return $self->{enabled} ? 1 : 0;
}

# set some variables
sub set {
    my Proximo::Service $self = shift;
    my ( $key, $val ) = @_;

    $key = lc $key;
    Proximo::debug( 'Service %s setting %s = %s.', $self->name, $key, $val );

    # now split out and set what they want
    if ( $key eq 'listen' ) {
        $self->listen( $val );
    }
}

# return a service by name, capitalization this way to annotate that you can
# call this on the class, don't need an object...
sub GetServiceByName {
    return $Services{$_[1]};
}

# return the raw services hash, this is a bit low level, so hopefully if you're
# messing with this you know what you're doing
sub GetServices {
    return \%Services;
}

1;
