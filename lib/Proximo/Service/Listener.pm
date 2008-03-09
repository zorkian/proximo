#!/usr/bin/perl

package Proximo::Service::Listener;

use strict;
use IO::Socket::INET;
use Proximo::MySQL::Server;
use Proximo::Management::Server;
use Proximo::Socket;
use Socket;
use base 'Proximo::Socket';

use fields (
        'listen',   # ip:port we're listening on
        'service',  # who owns us (Proximo::Service)
    );

# construct a new Proximo server socket, this accepts new connections and
# allows us to do something with them.
sub new {
    my Proximo::Service::Listener $self = shift;
    $self = fields::new( $self ) unless ref $self;

    # sanitize a listening port option ...
    my ( $service, $listen ) = @_;
    $self->{listen}  = $listen;
    $self->{service} = $service;

    # now setup the socket for listening
    my $sock = IO::Socket::INET->new(
            LocalAddr => $listen,
            Proto => 'tcp',
            Listen => 1024,
            ReuseAddr => 1,
        );
    Proximo::fatal( "Failed to listen on $listen: ##" )
        unless $sock;
    Proximo::info( "Server listening on $listen." );

    # try to make this non-blocking
    IO::Handle::blocking( $sock, 0 )
        or Proximo::fatal( "Unable to make listener non-blocking: ##" );

    # now we can do this final setup, we delayed it earlier...
    $self->SUPER::new( $service, $sock );

    # turn on watching for readability (new connections)
    $self->watch_read( 1 );

    return $self;
}

# this is fired when we have a new connection come in
# FIXME: it would be pretty neat if we made this configurable so it could spawn
# anything and not a MySQL connection ... a generic perl proxy app, cool
sub event_read {
    my Proximo::Service::Listener $self = $_[0];

    Proximo::debug( "One or more connections are available to accept." );

    while ( my ( $sock, $addr ) = $self->{sock}->accept ) {
        # disable blocking
        IO::Handle::blocking( $sock , 0 );

        # simple_proxy is (for now) just MySQL...
        # FIXME: generic needs to happen here
        if ( $self->service->mode eq 'simple_proxy' ) {
            Proximo::MySQL::Server->new( $self->service, $sock, $addr );

        # management console happens elsewhere
        } elsif ( $self->service->mode eq 'management' ) {
            Proximo::Management::Server->new( $self->service, $sock, $addr );

        # this is really bad actually, really bad...
        } else {
            Proximo::warn( 'Attempted to accept socket for service %s with unknown mode %s.', $self->service->name, $self->service->mode );
            close $sock;
        }
    }
}

# return our service object
sub service {
    my Proximo::Service::Listener $self = $_[0];
    return $self->{service};
}

# returns our current listen configuration
sub listening_on {
    my Proximo::Service::Listener $self = $_[0];
    return $self->{listen};
}

1;