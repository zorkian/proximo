#!/usr/bin/perl
#
# represents a logical icluster of MySQL databases.  this has NOTHING to do with
# NDB Cluster or MySQL Cluster products!  this is just a good term to mean a set
# of databases that work together to accomplish something.
#
# this module is responsible for handling logic about where to send incoming
# queries, when to connect new backends, etc.

package Proximo::MySQL::Cluster;

use strict;

use fields (
        'name',    # name of this cluster
        'setup',   # how this cluster is configured
        'masters', # array of nodes designated
    );

# constructs a new cluster for people to use.  only one of these should be created
# per cluster defined, not one per connection (see P::M::Cluster::Instance for that)
sub new {
    my Proximo::MySQL::Cluster $self = $_[0];
    $self = fields::new( $self ) unless ref $self;

    # initialization
    $self->{name} = $_[1];

    return $self;
}

# constructs an instance of this particular cluster and returns it
sub instance {
    my Proximo::MySQL::Cluster $self = $_[0];
    my Proximo::MySQL::Client $client = $_[1];

    return Proximo::MySQL::Cluster::Instance->new( $self, $client );
}

# called by the management console to set various attributes
sub set {
    my Proximo::MySQL::Cluster $self = $_[0];
    my ( $key, $val ) = ( $_[1], $_[2] );

    $key = lc $key;

    if ( $key eq 'name' ) {
        $self->{name} = $val;

    }
}

1;