#!/usr/bin/perl

package Proximo::Socket;

use strict;
use Danga::Socket;
use base 'Danga::Socket';

use fields (
        'time_est',    # time this socket was established
        'service',     # service we belong to
        'ps_buffer',   # internal Proximo::Socket buffer we use, see get_line below...
        'remoteip',    # IP of the remote end
        'remoteport',  # port of the remote end
    );

# construct a new Proximo::Socket
# argument: $socket
sub new {
    my Proximo::Socket $self = shift;
    $self = fields::new( $self ) unless ref $self;

    # debug output
    Proximo::debug( "Proximo::Socket construction beginning." );

    # store the information on the remote end for later
    my ( $srvc, $sock, $addr ) = @_;

    # if we are connected to a remote...
    if ( defined $addr ) {
        # convert this to human readable
        my ( $pport, $pipr ) = Socket::sockaddr_in( $addr );
        my $pip = Socket::inet_ntoa( $pipr );
        Proximo::info( "New connection $sock from: $pip:$pport" );

        # init our fields
        $self->{remoteip} = $pip;
        $self->{remoteport} = $pport;
    }

    # the rest of the init
    $self->{service} = $srvc;
    $self->{time_est} = time;

    # continue construction upstream (sound of swinging hammers)
    $self->SUPER::new( $sock );

    return $self;
}

# this is used when the caller wants to get a line of text.  this is useful in the
# management console, and could be uesful for anybody who wants to write that sort of
# module.  this handles all reading/writing.
#
# NOTE: if you use this method, you can't use the normal read method!!!  think of it
# like the distinction between read and sysread in typical Perl IO...
sub get_line {
    my Proximo::Socket $self = $_[0];

    # try reading from the socket
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
        Proximo::debug( 'Read %d bytes from socket.', length( $$read ) );
        $self->{ps_buffer} .= $$read;
    }

    # okay, now see if we can pull a line off
    if ( $self->{ps_buffer} =~ s/^(.*?)\r?\n$//m ) {
        return $1;
    }
    return undef;
}

# very simply write out a line and then turn writing on, this is a convenience
# method.  NOTE: this method appends \n to your output!  it also takes input
# just like printf (we run the args through sprintf).
sub write_line {
    my Proximo::Socket $self = shift;

    my $string = shift;
    $self->write( sprintf( "$string\n", @_ ) );
    $self->watch_write( 1 );
    return 1;
}

# get remote end ip
sub remote_ip {
    my Proximo::Socket $self = $_[0];
    return $self->{remoteip};
}

# and port
sub remote_port {
    my Proximo::Socket $self = $_[0];
    return $self->{remoteport};
}

# what time we started
sub time_established {
    my Proximo::Socket $self = $_[0];
    return $self->{time_est};    
}

# return our service
sub service {
    my Proximo::Socket $self = $_[0];
    return $self->{service};
}

# generic string information
sub as_string {
    my Proximo::Socket $self = $_[0];

    return sprintf(
            '%s: connected to %s:%d for %d seconds; service=%s.',
            ref( $self ), $self->remote_ip, $self->remote_port, time - $self->time_established,
            $self->service ? $self->service->name : "(none)",
        ); 
}

# default writable event handling, mostly we just send out any pending data and
# shut down writing if we have nothing left to do
sub event_write {
    my Proximo::Socket $self = $_[0];

    # if nothing to write shut off watching for further writes
    $self->watch_write( 0 )
        if $self->write;
}

1;
