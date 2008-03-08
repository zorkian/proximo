#!/usr/bin/perl

package Proximo::Socket;

use strict;
use Danga::Socket;
use base 'Danga::Socket';

use fields (
    );

# construct a new Proximo::Socket
# argument: $socket
sub new {
    my Proximo::Socket $self = shift;
    $self = fields::new( $self ) unless ref $self;

    Proximo::debug( "Proximo::Socket construction begin." );

    # now construct parent, passing the socket
    my $sock = shift;
    $self->SUPER::new( $sock );

    return $self;
}

1;
