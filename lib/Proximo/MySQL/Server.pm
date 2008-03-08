#!/usr/bin/perl

package Proximo::MySQL::Server;

use strict;
use Proximo::MySQL::Connection;
use Proximo::MySQL::Constants;
use Proximo::MySQL::Packet;
use base 'Proximo::MySQL::Connection';

use fields (
    );

# construct a new server connection, this is the connection between us
# and the user, whoever they may be
sub new {
    my Proximo::MySQL::Server $self = shift;
    $self = fields::new( $self ) unless ref $self;

    # initialize the work via our parent
    my ( $prox, $sock ) = @_;
    $self->SUPER::new( $prox, $sock );

    # set some internal shizzle
    $self->{mode}  = 1;      # server mode
    $self->{state} = 'init'; # initial state

    # protocol dictates that we are responsible for sending a greeting to begin
    # with, so we start writable but ignore reading
    $self->watch_write( 1 );

    return $self;
}

# when we're writable, we probably need to do something that involves sending out
# a new or a queued packet...
sub event_write {
    my Proximo::MySQL::Server $self = shift;

    # we've just begin, send handshake
    if ( $self->state eq 'init' ) {
        $self->_send_handshake;

    # fallback case, if we don't know what is going on, we pass this back to our
    # parent so that they can do any processing
    } else {
        $self->SUPER::event_write;

    }
}

# called when we get a packet from the client
sub event_packet {
    my Proximo::MySQL::Server $self = shift;

    my ( $seq, $packet_raw ) = @_;
    Proximo::debug( 'Server processing packet with sequence %d of length %d bytes.', $seq, length( $$packet_raw ) );

    # if we're waiting on the handshake response, let's get that
    if ( $self->state eq 'handshake' ) {
        my $packet = Proximo::MySQL::Packet::ClientAuthentication->new_from_raw( $seq, $packet_raw );
        Proximo::debug( 'Attempted connection from: user=%s, database=%s.', $packet->user, $packet->database );

        #$self->_send_packet(
        #        Proximo::MySQL::Packet::Error->new( $self, $packet->sequence_number + 1, 2000, 'Access denied, bitch!' ),
        #    );

        $self->_send_packet(
                Proximo::MySQL::Packet::OK->new( $self, $packet->sequence_number + 1, 0, 0, 0, 0, 'So awesome!' ),
            );

        $self->state( 'wait_command' );

    # get a command from the user and execute it
    } elsif ( $self->state eq 'wait_command' ) {
        my $packet = Proximo::MySQL::Packet::Command->new_from_raw( $seq, $packet_raw );
        Proximo::debug( 'Got command: type=%d, arg=%s.', $packet->command_type, $packet->argument );

        #$self->_send_packet(
        #        Proximo::MySQL::Packet::Error->new( $self, $packet->sequence_number + 1, 666, 'I can\'t do that, Dave.' ),
        #    );
        $self->_make_simple_result(
                $packet->sequence_number + 1,
                [ 'box 1', 'box 2' ],
                [ 'data here', 'data there' ],
                [ 'data goes', 'EVERYWHERE' ],
            );
        
    # if we're in an unknown state, well fail...
    } else {
        Proximo::fatal( 'Got a packet in unknown state %s.', $self->state );

    }
}

# used to send a handshake to the user.  preconditions: we're writable, and we're
# in the 'init' state... so we're just going to assume that's true and move on
sub _send_handshake {
    my Proximo::MySQL::Server $self = shift;

    Proximo::debug( "Server sending welcome handshake to connecting client." );

    # construct and send the packet
    $self->_send_packet(
            Proximo::MySQL::Packet::ServerHandshakeInitialization->new( $self ),
        );

    # set our state and watch for a response
    $self->state( 'handshake' );
    $self->watch_read( 1 );
}

1;
