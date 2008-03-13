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
        'server',   # our P::M::Server object
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
    return Proximo::warn( 'Failed creating socket: ##.' )
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

    # initialize the work via our parent
    $self->SUPER::new( $svc, $sock, $addr );

    # now turn on watching for reads, as the first thing that happens is
    # the server will send us a packet saying "hey what's up my name's bob"
    $self->current_database( $self->server->current_database );
    $self->state( 'connecting' );
    $self->watch_read( 1 );

    # and now, we belong to you
    $self->server->backend( $self );

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

    # second stage, get a packet back from the server
    } elsif ( $self->state eq 'authenticating' ) {
        my $peek = ord( substr( $$packet_raw, 0, 1 ) );

        # OK packet
        if ( $peek == 0 ) {
            # we've authenticated, yay
            my $packet = Proximo::MySQL::Packet::OK->new_from_raw( $seq, $packet_raw );
            Proximo::debug( 'Backend server authentication successful.' );
            $self->state( 'idle' );

            # okay, now let's inform the server
            $self->server->backend_available( $self );

        # error packet
        } elsif ( $peek == 255 ) {
            my $packet = Proximo::MySQL::Packet::Error->new_from_raw( $seq, $packet_raw );
            Proximo::warn( 'Got an error from the server, lame.' );

            # FIXME: is this right?  I'm too tired to really think if this is the proper thing
            # to do in this case.  test, test...
            $self->close( 'error' );
            $self->server->close( 'error' );

        # something else
        } else {
            Proximo::fatal( 'Really bad peek value %d.', $peek );
            
        }

    # when we get a response in this state, we can send it to the client
    } elsif ( $self->state eq 'wait_response' ) {
        # four possible responses in this case, let's see what we got
        my $val = unpack( 'C', substr( $$packet_raw, 0, 1 ) );
        
        # OK happens if we have no result set
        my $packet;
        if ( $val == PEEK_OK ) {
            $self->state( 'wait_client' );
            $packet = Proximo::MySQL::Packet::OK->new_from_raw( $seq, $packet_raw );
            Proximo::debug( 'Result OK: server status = %d, affected rows = %d, insert id = %d, warnings = %d, message = %s.',
                            $packet->server_status, $packet->affected_rows, $packet->insert_id, $packet->warning_count,
                            $packet->message || '(none)' );

        # ERROR packets indicate ... an error
        } elsif ( $val == PEEK_ERROR ) {
            $packet = Proximo::MySQL::Packet::Error->new_from_raw( $seq, $packet_raw );
            Proximo::debug( 'Result ERROR: error number = %d, SQL state = %s, message = %s.',
                            $packet->error_number, $packet->sql_state, $packet->message );

        # it should never be an EOF packet
        } elsif ( $val == PEEK_EOF ) { 
            Proximo::warn( 'Backend got EOF packet in wait_response state.' );
            return $self->close( 'unexpected_packet' );

        # else it's the beginning of a fieldset
        } else {
            # FIXME: need new_from_raw for this type of packet
            #my $packet = Proximo::MySQL::Packet::
            $self->state( 'recv_fields' );

        }

        # FIXME: this is manual tweaking we should be able to get rid of.
        if ( defined $packet ) {
            $self->server->_send_packet( $packet );

        } else {
            my $buf = substr( pack( 'V', length( $$packet_raw ) ), 0, 3) . chr( $seq ) . $$packet_raw;
            $self->server->write( \$buf );
            $self->server->watch_write( 1 );
        }

    # in this state, the server is sending fields at us
    } elsif ( $self->state eq 'recv_fields' ) {
        # decode peek value
        my $val = unpack( 'C', substr( $$packet_raw, 0, 1 ) );
        
        # if this is an EOF packet, set our state
        if ( $val == PEEK_EOF ) {
            $self->state( 'recv_rows' );
        }

        # FIXME: this is manual and shouldn't be done this way
        my $buf = substr( pack( 'V', length( $$packet_raw ) ), 0, 3) . chr( $seq ) . $$packet_raw;
        $self->server->write( \$buf );
        $self->server->watch_write( 1 );

    # server is blasting actual row data at us
    } elsif ( $self->state eq 'recv_rows' ) {
        # decode peek value
        my $val = unpack( 'C', substr( $$packet_raw, 0, 1 ) );
        
        # if this is an EOF packet, set our state
        if ( $val == PEEK_EOF ) {
            $self->state( 'wait_client' );
        }
        
        # FIXME: this is manual and shouldn't be done this way
        my $buf = substr( pack( 'V', length( $$packet_raw ) ), 0, 3) . chr( $seq ) . $$packet_raw;
        $self->server->write( \$buf );
        $self->server->watch_write( 1 );

    # haven't put in any handling for this state?
    } else {
        Proximo::fatal( 'Backend received packet in unexpected state %s.', $self->state );
        
    }
}

# send a packet from the client to the backend
sub send_packet {
    my Proximo::MySQL::Backend $self = $_[0];
    my Proximo::MySQL::Packet $pkt = $_[1];

    $self->state( 'wait_response' );
    $self->_send_packet( $pkt );

    return 1;
}

# return the server
sub server {
    my Proximo::MySQL::Backend $self = $_[0];
    return $self->{server};
}

# if we get closed out...
sub close {
    my Proximo::MySQL::Backend $self = shift;

    # if our state is currently not 
    if ( $self->server ) {
        $self->server->backend( undef );
    }

    $self->SUPER::close( @_ );
}

1;