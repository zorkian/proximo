#!/usr/bin/perl
#
# a service is defined as something that listens on a port and takes incoming
# connects and does something with it.  this usually involves creating another
# class to pass the incoming socket off to.

package Proximo::MySQL::Service;

use strict;
use Proximo::Service;
use base 'Proximo::Service';

use fields (
        'proxy_to',   # ip:port to proxy incoming requests to
        'proxy_user', # username of the remote server
        'proxy_pass', # password of the remote server
    );

# construct a new Proximo server socket, this accepts new connections and
# allows us to do something with them.
sub new {
    my Proximo::MySQL::Service $self = shift;
    $self = fields::new( $self ) unless ref $self;

    # get input arguments and setup
    $self->{proxy_to}   = undef;
    $self->{proxy_user} = undef;
    $self->{proxy_pass} = undef;

    # now create from our parent
    $self->SUPER::new( @_ );

    # store this service and note it's been built
    Proximo::debug( 'Proximo::MySQL::Service named %s constructed.', $self->name );

    return $self;
}

# take in a socket and create a server for it
sub take_accepted_connection {
    Proximo::MySQL::Server->new( @_ );
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

# return what proxy_to is configured as
sub proxy_to {
    my Proximo::MySQL::Service $self = $_[0];
    return $self->{proxy_to};
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
    if ( $key =~ /^(?:proxy_to|proxy_user|proxy_pass)$/ ) {
        $self->{$key} = $val;

    # fallback is simply to pass to our parent
    } else {
        return $self->SUPER::set( $key, $val );

    }
}

1;
