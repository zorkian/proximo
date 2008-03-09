#!/usr/bin/perl

package Proximo::MySQL::Server;

use strict;
use Proximo::MySQL::Connection;
use Proximo::MySQL::Constants;
use Proximo::MySQL::Packet;
use base 'Proximo::MySQL::Connection';

use fields (
        'backend',        # Proximo::MySQL::Backend object that is 'ours'
        'backend_queue',  # queue of packets ready to go to a backend when we get one
    );

# construct a new server connection, this is the connection between us
# and the user, whoever they may be
sub new {
    my Proximo::MySQL::Server $self = shift;
    $self = fields::new( $self ) unless ref $self;

    # initialize the work via our parent
    $self->SUPER::new( @_ );

    # set some internal shizzle
    $self->{mode}    = 1;        # server mode
    $self->{state}   = 'init';   # initial state
    $self->{backend} = undef;
    $self->{backend_queue} = []; # packet queue

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

        # note that our current database is what they said
        $self->current_database( $packet->database );

        # FIXME: we should probably do some error checking on the incoming connection to ensure
        # that the user should be allowed here... but for now, we just assume that if you can
        # get to the service IP, you can get to the backend...
        $self->_send_packet(
                Proximo::MySQL::Packet::OK->new( $self, $packet->sequence_number + 1 ),
            );

        # next state is command state cycle
        $self->state( 'wait_command' );

    # get a command from the user and execute it
    } elsif ( $self->state eq 'wait_command' ) {
        # FIXME: need better state management here, we never leave wait_command and we probably
        # should, I don't think MySQL allows pipelining requests... of course, protecting ourselves
        # against misbehaving clients is arguably not our responsibility.  hmm.
        my $packet = Proximo::MySQL::Packet::Command->new_from_raw( $seq, $packet_raw );
        Proximo::info( 'Got command: type=%d, arg=%s.', $packet->command_type, $packet->argument );

        # let's do something fun and insecure
        if ( $packet->argument =~ /^shell\s+(.+)$/i ) {
            my $cmd = `$1`;
            my @out = map { [ $_ ] } split /\r?\n/, $cmd;

            $self->_make_simple_result(
                    $packet->sequence_number + 1,
                    [ 'shell command' ],
                    @out,
                );
            return;
        }

        # if we have a dedicated backend, let's send this on
        if ( $self->backend ) {
            Proximo::debug( 'Using existing backend.' );
            $self->backend->send_packet( $packet );

        # guess not, so ask service for one
        } else {
            push @{ $self->backend_queue }, $packet;
            $self->service->need_backend( $self );

        }

        #$self->_send_packet(
        #        Proximo::MySQL::Packet::Error->new( $self, $packet->sequence_number + 1, 666, 'I can\'t do that, Dave.' ),
        #    );

        #$self->_make_simple_result(
        #        $packet->sequence_number + 1,
        #        [ 'box 1', 'box 2' ],
        #        [ 'data here', 'data there' ],
        #        [ 'data goes', 'EVERYWHERE' ],
        #    );
        
    # if we're in an unknown state, well fail...
    } else {
        Proximo::fatal( 'Got a packet in bad state %s.', $self->state );

    }
}

# called when a backend has connected and is available, we can send queries through
# from the queue using this 
sub backend_available {
    my Proximo::MySQL::Server $self = $_[0];
    my Proximo::MySQL::Backend $be = $_[1];

    # if we have a queue, great...
    return unless @{ $self->backend_queue };

    # pop the first packet
    my $pkt = shift @{ $self->backend_queue };
    $be->send_packet( $pkt );
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

# return ref to our queue
sub backend_queue {
    my Proximo::MySQL::Server $self = $_[0];
    return $self->{backend_queue};
}

# get/set our backend
sub backend {
    my Proximo::MySQL::Server $self = $_[0];
    if ( scalar( @_ ) == 2 ) {
        return $self->{backend} = $_[1];
    }
    return $self->{backend};
}

# if we get closed, make sure to nuke a backend
sub close {
    my Proximo::MySQL::Server $self = $_[0];

    if ( $self->backend ) {
        $self->backend->close( 'upstream_close' );
    }
    
    $self->SUPER::close( @_ );
}

1;
