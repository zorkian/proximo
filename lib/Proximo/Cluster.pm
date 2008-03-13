#!/usr/bin/perl
#
# the implementation of the basic Cluster class, mostly this is just defining the
# interface that the rest of the classes that implement this have to adhere to.

package Proximo::Cluster;

use strict;

use fields (
        'name',    # name of this cluster
    );

# class stuff
our ( %Clusters );

# constructs a new cluster for people to use.  only one of these should be created
# per cluster defined, not one per connection (see the Instance classes)
sub new {
    my Proximo::Cluster $self = $_[0];
    $self = fields::new( $self ) unless ref $self;

    # arguments are name, and that's about it
    my $name = $_[1];
    return Proximo::warn( 'Attempted to create a cluster with no name!' )
        unless $name;
    return Proximo::warn( 'Attempted to redeclare cluster with name %s.', $name )
        if exists $Clusters{$name};

    # get input arguments and setup
    $self->{name} = $name;

    # store this service and note it's been built
    $Clusters{$self->name} = $self;
    Proximo::debug( 'Cluster named %s constructed.', $self->name );

    return $self;
}

# constructs an instance of this particular cluster and returns it, must be
# implemented by the descendant classes
sub instance {
    Proximo::fatal( 'Proximo::Cluster::instance() not implemented!' );
}

# get our name
sub name {
    my Proximo::Cluster $self = $_[0];
    return $self->{name};
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

# return a service by name, capitalization this way to annotate that you can
# call this on the class, don't need an object...
sub GetClusterByName {
    return $Clusters{$_[1]};
}

# return the raw services hash, this is a bit low level, so hopefully if you're
# messing with this you know what you're doing
sub GetClusters {
    return \%Clusters;
}

1;