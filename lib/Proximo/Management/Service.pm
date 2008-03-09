#!/usr/bin/perl
#
# a service is defined as something that listens on a port and takes incoming
# connects and does something with it.  this usually involves creating another
# class to pass the incoming socket off to.

package Proximo::Management::Service;

use strict;
use Proximo::Service;
use base 'Proximo::Service';

use fields (
    );

# construct a new Proximo server socket, this accepts new connections and
# allows us to do something with them.
sub new {
    my Proximo::Management::Service $self = shift;
    $self = fields::new( $self ) unless ref $self;

    # now create from our parent
    $self->SUPER::new( @_ );

    # store this service and note it's been built
    Proximo::debug( 'Proximo::Management::Service named %s constructed.', $self->name );

    return $self;
}

# take in a socket and create a server for it
sub take_accepted_connection {
    Proximo::Management::Server->new( @_ );
}

1;
