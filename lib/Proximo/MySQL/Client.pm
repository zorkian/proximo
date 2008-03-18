#!/usr/bin/perl

package Proximo::MySQL::Client;

use strict;
use Proximo::MySQL::Connection;
use Proximo::MySQL::Constants;
use Proximo::MySQL::Packet;
use base 'Proximo::MySQL::Connection';

use fields (
        'cluster_inst',   # P::M::Cluster::Instance object
        'sequence',       # internal sequence counter
    );

# construct a new server connection, this is the connection between us
# and the user, whoever they may be
sub new {
    my Proximo::MySQL::Client $self = $_[0];
    $self = fields::new( $self ) unless ref $self;

    # initialize the work via our parent
    my ( $srvc, $sock, $addr ) = ( $_[1], $_[2], $_[3] );
    $self->SUPER::new( $srvc, $sock, $addr );

    # set some internal shizzle
    $self->{mode}          = 1;        # server mode
    $self->{state}         = 'init';   # initial state
    $self->{cluster_inst}  = undef;
    $self->{sequence}      = undef;

    # try to get a cluster instance
    if ( my $cluster = $self->service->proxy_to ) {
        $self->{cluster_inst} = $cluster->instance( $self );

    # this is a bad error :(
    } else {
        Proximo::warn( 'Closing socket - service has no proxy_to cluster.' );
        return $self->close( 'no_cluster_defined' );
    }

    # protocol dictates that we are responsible for sending a greeting to begin
    # with, so we start writable but ignore reading
    $self->watch_write( 1 );

    return $self;
}

# when we're writable, we probably need to do something that involves sending out
# a new or a queued packet...
sub event_write {
    my Proximo::MySQL::Client $self = shift;

    # we've just begin, send handshake
    if ( $self->state eq 'init' ) {
        $self->_send_handshake;

    # fallback case, if we don't know what is going on, we pass this back to our
    # parent so that they can do any processing
    } else {
        $self->SUPER::event_write;

    }
}

# get next sequence counter
sub next_sequence {
    my Proximo::MySQL::Client $self = $_[0];
    return ++$self->{sequence};
}

# called when we get a packet from the client
sub event_packet {
    my Proximo::MySQL::Client $self = $_[0];

    my ( $seq, $packet_raw ) = ( $_[1], $_[2] );
    Proximo::debug( 'Server processing packet with sequence %d of length %d bytes.', $seq, length( $$packet_raw ) );
    
    # save sequence
    $self->{sequence} = $seq;

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
                    [ 'results' ],
                    @out,
                );
            return;
        }

        # change our active database if it's type 2
        # FIXME: this could be a problem if they USE an invalid database and it errors,
        # we should detect that situation
        $self->current_database( $packet->argument )
            if $packet->command_type == 2;

        # pass the packet to the cluster instance for handling
        # FIXME: set our state to something more appropriate like wait_backend, that way we
        # can die out if the user sends us packets when we don't expect them (oh, I have this
        # same note up above... lol)
        return $self->inst->query( $packet->command_type, $packet->argument_ref );

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

# used to send a handshake to the user.  preconditions: we're writable, and we're
# in the 'init' state... so we're just going to assume that's true and move on
sub _send_handshake {
    my Proximo::MySQL::Client $self = shift;

    Proximo::debug( "Server sending welcome handshake to connecting client." );

    # construct and send the packet
    $self->_send_packet(
            Proximo::MySQL::Packet::ServerHandshakeInitialization->new( $self ),
        );

    # set our state and watch for a response
    $self->state( 'handshake' );
    $self->watch_read( 1 );
}

# return our cluster instance
sub inst {
    my Proximo::MySQL::Client $self = $_[0];
    return $self->{cluster_inst};
}

# if we get closed, make sure to nuke a backend
sub close {
    my Proximo::MySQL::Client $self = $_[0];

    # save backend information, then blow away instance links
    my $be = $self->inst->backend;
    $self->inst->destroy_links;
    $self->{cluster_inst} = undef;

    # now close it
    $be->close( $_[1] )
        if $be;

    # proxy up to the superclass
    return $self->SUPER::close( $_[1] );
}

1;
