#!/usr/bin/perl
#
# the implementation of the basic Cluster class, mostly this is just defining the
# interface that the rest of the classes that implement this have to adhere to.

package Proximo::Cluster;

use strict;

use fields (
        'name',    # name of this cluster
    );

# constructs a new cluster for people to use.  only one of these should be created
# per cluster defined, not one per connection (see the Instance classes)
sub new {
    my Proximo::Cluster $self = $_[0];
    $self = fields::new( $self ) unless ref $self;

    # initialization
    $self->{name} = $_[1];

    return $self;
}

# constructs an instance of this particular cluster and returns it, must be
# implemented by the descendant classes
sub instance {
    Proximo::fatal( 'Proximo::Cluster::instance() not implemented!' );
}

# called by the management console to set various attributes
sub set {
    my Proximo::Cluster $self = $_[0];
    my ( $key, $val ) = ( $_[1], $_[2] );

    $key = lc $key;

    # we actually don't have anything we set right now, but
    # it's possible we will in the future, so we expect the subclasses
    # to call up to us...
    return Proximo::warn( 'Unable to set key %s on Cluster instance.', $key );
}

1;