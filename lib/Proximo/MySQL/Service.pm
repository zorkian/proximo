#!/usr/bin/perl
#
# a service is defined as something that listens on a port and takes incoming
# connects and does something with it.  this usually involves creating another
# class to pass the incoming socket off to.

package Proximo::MySQL::Service;

use strict;
use Proximo::MySQL::Backend;
use Proximo::Service;
use base 'Proximo::Service';

use fields (
        'proxy_user',   # username to use on the cluster
        'proxy_pass',   # password   "" ""
    );

# construct a new Proximo server socket, this accepts new connections and
# allows us to do something with them.
sub new {
    my Proximo::MySQL::Service $self = shift;
    $self = fields::new( $self ) unless ref $self;

    # now create from our parent
    $self->SUPER::new( @_ );

    # get input arguments and setup
    $self->{proxy_user} = undef;
    $self->{proxy_pass} = undef;

    return $self;
}

# take in a socket and create a server for it
sub take_accepted_connection {
    # simply pass through to build a new client object
    my ( $srvc, $sock, $addr ) = ( $_[0], $_[1], $_[2] );
    Proximo::MySQL::Client->new( $srvc, $sock, $addr );
}

# attempt to enable a service, which basically means that we will start
# listening on the given port
sub enable {
    my Proximo::MySQL::Service $self = shift;

    # if we don't have the right stuff to enable, bail
    return Proximo::warn( 'Unable to enable service %s: proxy_to not defined.', $self->name )
        unless $self->proxy_to;

    # fall through to parent
    return $self->SUPER::enable;
}

# return what proxy_user is configured as
sub proxy_user {
    my Proximo::MySQL::Service $self = $_[0];
    return $self->{proxy_user};
}

# return what proxy_pass is configured as
sub proxy_pass {
    my Proximo::MySQL::Service $self = $_[0];
    return $self->{proxy_pass};
}

# set some variables
sub set {
    my Proximo::MySQL::Service $self = shift;
    my ( $key, $val ) = @_;

    $key = lc $key;

    # now split out and set what they want
    if ( $key =~ /^(?:proxy_user|proxy_pass)$/ ) {
        $self->{$key} = $val;

    # fallback is simply to pass to our parent
    } else {
        return $self->SUPER::set( $key, $val );

    }
}

# called by Server connections when they need a backend connection
sub need_backend {
    my Proximo::MySQL::Service $self = $_[0];
    my Proximo::MySQL::Client $srvr = $_[1];

    # debug
    Proximo::debug( 'Spawning new backend to %s.', $self->proxy_to );

    # start connecting a new backend and tell it who to notify when it
    # gets connected and setup
    Proximo::MySQL::Backend->new( $self, $srvr );
}

1;
