#!/usr/bin/perl
#
# represents a particular instance of a cluster being used by an incoming
# connection.  this module should try to remain fast and low memory, for as
# much as that's worth saying...

package Proximo::MySQL::Cluster::Instance;

use strict;

use fields (
        'cluster',   # P::M::Cluster object
        'client',    # P::M::Client object
    );

# construct a new instance for somebody
sub new {
    my Proximo::MySQL::Cluster::Instance $self = $_[0];
    $self = fields::new( $self ) unless ref $self;

    # get parameters
    $self->{cluster} = $_[1];
    $self->{client}  = $_[2];

    # all good
    return $self;
}

# returns our client, read only
sub client {
    my Proximo::MySQL::Cluster::Instance $self = $_[0];
    return $self->{client};
}

# returns our cluster, read only
sub cluster {
    my Proximo::MySQL::Cluster::Instance $self = $_[0];
    return $self->{cluster};
}

1;