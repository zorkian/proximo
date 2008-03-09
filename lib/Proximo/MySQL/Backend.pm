#!/usr/bin/perl

package Proximo::MySQL::Backend;

use strict;
use IO::Handle;
use Proximo::MySQL::Connection;
use Proximo::MySQL::Constants;
use Proximo::MySQL::Packet;
use Socket qw/ PF_INET IPPROTO_TCP SOCK_STREAM SOL_SOCKET SO_ERROR
               AF_UNIX PF_UNSPEC /;
use base 'Proximo::MySQL::Connection';

use fields (
        'server',
    );
    
# construction is fun for you and me
sub new {
    my Proximo::MySQL::Backend $self = $_[0];
    $self = fields::new( $self ) unless ref $self;

    # arguments
    my ( $svc, $server ) = ( $_[1], $_[2] );

    # get where we're proxying to
    my $ipport = $svc->proxy_to;
    my ( $ip, $port ) = ( $1, $2 )
        if $ipport =~ /^(.+?):(\d+)$/;

    # setup the socket
    my $sock;
    socket $sock, PF_INET, SOCK_STREAM, IPPROTO_TCP;
    return Proximo::warn( 'Failed creating socket: ##' )
        unless $sock && defined fileno( $sock );

    # get structures
    my $inet_aton = Socket::inet_aton( $ip )
        or return Proximo::warn( 'Failed to get inet_aton for ip %s.', $ip );
    my $addr = Socket::sockaddr_in( $port, $inet_aton )
        or return Proximo::warn( 'Failed to get sockaddr_in for ip %s port %d.', $ip, $port );

    # non-block and launch the connect
    IO::Handle::blocking( $sock, 0 );
    connect $sock, $addr;

    # save our server
    $self->{server} = $server;
    $self->current_database( $self->server->current_database );

    # initialize the work via our parent
    $self->SUPER::new( $svc, $sock, $addr );

    # now turn on watching for reads, as the first thing that happens is
    # the server will send us a packet saying "hey what's up my name's bob"
    $self->state( 'connecting' );
    $self->watch_read( 1 );

    return $self;
}

# called when we get a packet
sub event_packet {
    my Proximo::MySQL::Backend $self = $_[0];
    
    my ( $seq, $packet_raw ) = ( $_[1], $_[2] );
    Proximo::debug( 'Backend processing packet with sequence %d of length %d bytes.', $seq, length( $$packet_raw ) );

    # depending on state, could be any sort of packet...
    if ( $self->state eq 'connecting' ) {
        my $packet = Proximo::MySQL::Packet::ServerHandshakeInitialization->new_from_raw( $seq, $packet_raw );
        Proximo::debug( 'Established connection with backend of version %s.', $packet->server_version );

        # now let's send an authentication packet
        $self->state( 'authenticating' );
        $self->_send_packet(
                Proximo::MySQL::Packet::ClientAuthentication->new( $self, $packet->sequence_number + 1, )
            );

    }
}

# return the server
sub server {
    my Proximo::MySQL::Backend $self = $_[0];
    return $self->{server};
}

1;