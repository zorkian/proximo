#!/usr/bin/perl

package Proximo::MySQL::Connection;

use strict;
use Proximo::Socket;
use Proximo::MySQL::Constants;
use Proximo::MySQL::Packet;
use base 'Proximo::Socket';

use fields (
        'mode',   # what mode we're in
        'state',  # what state this MySQL connection is in
        'buffer', # internal buffer of incoming data
        'dbname', # active database name
    );

# construct a new Proximo::Client connection, this is the connection between us
# and the user, whoever they may be
sub new {
    my Proximo::MySQL::Connection $self = shift;
    $self = fields::new( $self ) unless ref $self;

    # pass through to our parent
    $self->SUPER::new( @_ );

    # generic initialization
    $self->{dbname} = undef;
    $self->{mode}   = undef;
    $self->{state}  = 'new';
    $self->{buffer} = '';

    return $self;
}

# returns 1 if we're in server mode
sub is_server {
    my Proximo::MySQL::Connection $self = $_[0];
    return $self->{mode} == 1 ? 1 : 2;
}

# returns 1 if we're in client mode
sub is_client {
    my Proximo::MySQL::Connection $self = $_[0];
    return $self->{mode} == 2 ? 1 : 2;
}

# get/set the current state, just a string that use to figure out what is
# going on right now
sub state {
    my Proximo::MySQL::Connection $self = $_[0];
    if ( scalar( @_ ) == 2 ) {
        #Proximo::debug( "$self entering state $_[1]." );
        return $self->{state} = $_[1];
    }
    return $self->{state};
}

# sends a packet through
sub _send_packet {
    my Proximo::MySQL::Connection $self = $_[0];
    my Proximo::MySQL::Packet $packet = $_[1];

    # write out the built packet, and also turn on watching for writability
    # to ensure that we finish sending this packet out
    my $raw = $packet->_raw;
    #Proximo::debug( 'Preparing to write %d bytes.', length( $$raw ) );

    # now send and turn on watching for writable notifications
    $self->write( $raw );
    $self->watch_write( 1 );
}

# creates a few packets to make a simple result set and then stuffs them into
# the pipe to be sent out
sub _make_simple_result {
    my Proximo::MySQL::Connection $self = shift;
    my $seq = shift() + 0;

    my @packets;

    # construct the output packet list
    my $fields = shift;
    push @packets, Proximo::MySQL::Packet::ResultSetHeader->new( $self, $seq++, scalar( @$fields ) );
    push @packets, Proximo::MySQL::Packet::Field->new( $self, $seq++, name => $_ )
        foreach @$fields;
    push @packets, Proximo::MySQL::Packet::EOF->new( $self, $seq++ );
    push @packets, Proximo::MySQL::Packet::RowData->new( $self, $seq++, @$_ )
        foreach @_;
    push @packets, Proximo::MySQL::Packet::EOF->new( $self, $seq++ );

    # now send these in a bottle, so we don't waste time blasting little packets
    # when we know we're just going to be sending more data shortly
    $self->tcp_cork( 1 );
    $self->_send_packet( $_ )
        foreach @packets;
    $self->tcp_cork( 0 );

    # sending packets turns writing on, so we should be good...
    return scalar( @packets );
} 

# read in a packet from somewhere, parse out what it should be and instantiate it,
# then call down to whatever and let them know
sub event_read {
    my Proximo::MySQL::Connection $self = $_[0];

    # loop and try to read in data
    while ( 1 ) {
        my $read = $self->read( 1024 * 1024 );

        # on undef, the socket is closed
        unless ( defined $read ) {
            Proximo::debug( 'Connection noticed dead during read.' );
            $self->close( 'disconnected' );
            return;
        }

        # if empty buffer, nothing to read, done
        last unless $$read;

        # got some bytes, let's append to our internal buffer
        #Proximo::debug( 'Read %d bytes from socket.', length( $$read ) );
        $self->{buffer} .= $$read;
    }

    # now try to bang some packets off
    my $buflen = length( $self->{buffer} );
    while ( $buflen >= 4 ) {
        # now get length of this packet and then packet sequence number
        my $len = unpack( 'V', substr( $self->{buffer}, 0, 3 ) . "\0" );
        my $seq = unpack( 'C', substr( $self->{buffer}, 3, 1 ) );
        last unless $buflen >= $len + 4;

        # we've got the full thing, rip it out; note we append a null here because
        # the protocol likes to stick strings at the end of a packet and this is
        # the easiest way for us to parse them out
        my $packet_raw = substr( $self->{buffer}, 4, $len ) . "\0";
        $self->{buffer} = substr( $self->{buffer}, 4 + $len );
        $buflen = length( $self->{buffer} );

        # pass this raw packet data to our children
        $self->event_packet( $seq, \$packet_raw );
    }
}

# render ourselves out for the management console
sub as_string {
    my Proximo::MySQL::Connection $self = $_[0];

    return sprintf(
            '%s: connected to %s:%d for %d seconds; state=%s, service=%s, db=%s.',
            ref( $self ), $self->remote_ip, $self->remote_port, time - $self->time_established,
            $self->state, $self->service->name, $self->current_database,
        ); 
}

# get/set the current database for this connection
sub current_database {
    my Proximo::MySQL::Connection $self = $_[0];
    if ( scalar( @_ ) == 2 ) {
        Proximo::debug( '%s current database now %s.', $self, $_[1] );
        return $self->{dbname} = $_[1];
    }
    return $self->{dbname};
}

# set state on close
sub close {
    my Proximo::MySQL::Connection $self = $_[0];

    # state management
    Proximo::debug( '%s is now closed: %s.', $self, $_[1] );
    $self->state( 'closed' );
    return $self->SUPER::close( $_[1] );
}

# these handlers are called in various states, we need to smack around anybody
# who hits these, because they should be overriden by our children
sub event_packet { Proximo::fatal( "Proximo::MySQL::Connection::event_packet() not overriden!" ); }
sub event_err    { Proximo::fatal( "Proximo::MySQL::Connection::event_err() not overriden!" );    }
sub event_hup    { Proximo::fatal( "Proximo::MySQL::Connection::event_hup() not overriden!" );    }

1;
