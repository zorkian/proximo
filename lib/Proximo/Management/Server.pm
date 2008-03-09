#!/usr/bin/perl

package Proximo::Management::Server;

use strict;
use Proximo::Socket;
use base 'Proximo::Socket';

use fields (
    );

# construct a new server connection, this is the connection between us
# and the user, whoever they may be
sub new {
    my Proximo::Management::Server $self = shift;
    $self = fields::new( $self ) unless ref $self;

    # initialize the work via our parent
    $self->SUPER::new( @_ );

    # wait for user to send us something
    $self->watch_read( 1 );

    return $self;
}

# when the user sends something, we handle it.  management console is very much
# oriented to this 1:1 response.  we don't typically send them any information
# other than that...
sub event_read {
    my Proximo::Management::Server $self = $_[0];

    # get lines
    while ( my $line = $self->get_line ) {
        # socks command - show open sockets
        if ( $line =~ /^socks/i ) {
            $self->write_line( "socks" );

        # lame, they are sending junk
        } else {
            $self->write_line( "Unknown command: $line" );

        }
    }
}

1;