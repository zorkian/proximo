#!/usr/bin/perl

package Proximo::Server;

use strict;
use IO::Socket::INET;
use Proximo::MySQL::Server;
use Proximo::Socket;
use Socket;
use base 'Proximo::Socket';

use fields (
        'prox',    # Proximo object, who owns us
    );

# construct a new Proximo server socket, this accepts new connections and
# allows us to do something with them.
sub new {
    my Proximo::Server $self = shift;
    $self = fields::new( $self ) unless ref $self;

    # get input arguments and setup
    my $prox = shift;
    $self->{prox} = $prox;

    # debug \o/
    Proximo::debug( "Proximo::Server construction begin." );

    # sanitize a listening port option ...
    my $listen = $self->{prox}->opt('listen');
    $listen ||= '127.0.0.1:2306';
    $listen = "127.0.0.1:$listen" if $listen =~ /^\d+$/;

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

    # now create and setup final things
    $self->SUPER::new( $sock );

    # turn on watching for readability (new connections)
    $self->watch_read( 1 );

    return $self;
}

# this is fired when we have a new connection come in
sub event_read {
    my Proximo::Server $self = shift;

    Proximo::debug( "One or more connections are available to accept." );

    while ( my ( $sock, $addr ) = $self->{sock}->accept ) {
        # disable blocking
        IO::Handle::blocking( $sock , 0 );

        # FIXME: put this in some IF on debugging/verbosity...
        my ($pport, $pipr) = Socket::sockaddr_in($addr);
        my $pip = Socket::inet_ntoa($pipr);
        Proximo::info( "New connection $sock from: $pip:$pport" );

        # now send this off to our client
        Proximo::MySQL::Server->new( $self->{prox}, $sock );
    }
}

1;
