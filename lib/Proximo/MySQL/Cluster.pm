#!/usr/bin/perl
#
# represents a logical icluster of MySQL databases.  this has NOTHING to do with
# NDB Cluster or MySQL Cluster products!  this is just a good term to mean a set
# of databases that work together to accomplish something.
#
# this module is responsible for handling logic about where to send incoming
# queries, when to connect new backends, etc.

# forward declaration to make fields happy
package Proximo::MySQL::Cluster::Instance;

# now back to your regularly scheduled package
package Proximo::MySQL::Cluster;

use strict;
use Proximo::Cluster;
use Proximo::MySQL::Backend;
use Proximo::MySQL::Client;
use base 'Proximo::Cluster';

use fields (
        'setup',     # how this cluster is configured
        'masters',   # array of nodes designated as masters
        'slaves',    # array of nodes designated as slaves
        'readonly',  # if enabled, do no allow writes
    );

# constructs a new cluster for people to use.  only one of these should be created
# per cluster defined, not one per connection (see P::M::Cluster::Instance for that)
sub new {
    my Proximo::MySQL::Cluster $self = $_[0];
    $self = fields::new( $self ) unless ref $self;

    # init parent
    $self->SUPER::new( $_[1] );

    # self variables
    $self->{setup}    = 'single';
    $self->{masters}  = [];
    $self->{slaves}   = [];
    $self->{readonly} = 0;

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
    my $truefalse = sub {
        my $val = shift;
        return 1 if $val =~ /^(?:true|on|yes|1|affirmative)$/;
        return 0 if $val =~ /^(?:false|off|no|0|negative)$/;
        return undef;
    };

    # set list of masters/slaves
    if ( $key eq 'masters' || $key eq 'slaves' ) {
        my @new;
        my @ips = split /\s*,\s*/, $val;
        foreach my $ip ( @ips ) {
            return Proximo::warn( 'IP %s not valid in set of %s.', $ip, $key )
                unless $ip =~ /^\d+\.\d+\.\d+\.\d+(?::\d+)?$/;
            $ip = "$ip:3306" unless $ip =~ /:/;
            push @new, $ip;
        }
        
        # looks like they're valid, ok, let's replace our list with them...
        $self->{$key} = \@new;
        return 1;

    # maybe they want to change the cluster setup?
    } elsif ( $key eq 'setup' ) {
        return Proximo::warn( 'Invalid setup %s for MySQL cluster.', $key )
            unless $val =~ /^(?:single|master-slave|master-master)$/;
    
        # save it
        $self->{setup} = $val;    
        return 1;

    # turn readonly on/off
    } elsif ( $key eq 'readonly' ) {
        my $rv = $truefalse->( $val );
        return Proximo::warn( 'Invalid value %s for readonly.', $key )
            unless $rv;

        # save it
        $self->{readonly} = $rv;
        return 1;

    }

    # fall through to parent
    return $self->SUPER::set( $key, $val );
}

# returns the arrayref of masters
sub masters {
    my Proximo::MySQL::Cluster $self = $_[0];
    return $self->{masters};
}

# how many masters we have, utility function
sub master_count {
    my Proximo::MySQL::Cluster $self = $_[0];
    return scalar( @{ $self->{masters} } );
}

# arrayref of slaves
sub slaves {
    my Proximo::MySQL::Cluster $self = $_[0];
    return $self->{slaves};
}

# how many slaves we have, utility function
sub slave_count {
    my Proximo::MySQL::Cluster $self = $_[0];
    return scalar( @{ $self->{slaves} } );
}

# takes in a query and does something with it, notably executing it ideally
sub query {
    my Proximo::MySQL::Cluster $self = $_[0];
    my Proximo::MySQL::Cluster::Instance $instance = $_[1];
    my $q_ref = $_[2];

    # one of two things happens, either we are in a mode that requires us to be
    # sticky on the backend, or we can get whatever is available
    if ( $instance->sticky ) {
        my $be = $instance->backend;

        # should never happen, if so this is a bad error case
        unless ( $be ) {
            Proximo::warn( 'Client with sticky flag has no backend.  Bailing!' );
            $self->close( 'sticky_no_backend' );
            return undef;
        }

        # okay, pass this to our backend
        #$be->
    }
}

#############################################################################
#############################################################################
#############################################################################

package Proximo::MySQL::Cluster::Instance;

use strict;

use fields (
        'cluster',   # P::M::Cluster object
        'client',    # P::M::Client object
        'backend',   # P::M::Backend object
        'sticky',    # 1/0; if we should be stuck to our backend
    );

# construct a new instance for somebody
sub new {
    my Proximo::MySQL::Cluster::Instance $self = $_[0];
    $self = fields::new( $self ) unless ref $self;

    # get parameters
    $self->{cluster} = $_[1];
    $self->{client}  = $_[2];
    $self->{backend} = undef;
    $self->{sticky}  = 0;

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

# get/set our backend
sub backend {
    my Proximo::MySQL::Cluster::Instance $self = $_[0];
    if ( scalar( @_ ) == 2 ) {
        return $self->{backend} = $_[1];
    }
    return $self->{backend};
}

# get/set the sticky bit
sub sticky {
    my Proximo::MySQL::Cluster::Instance $self = $_[0];
    if ( scalar( @_ ) == 2 ) {
        return $self->{sticky} = $_[1];
    }
    return $self->{sticky};
}

# called when the client has sent a query for us to handle, this simply asks the
# cluster what to do with it...
sub query {
    my Proximo::MySQL::Cluster::Instance $self = $_[0];
    return $self->cluster->query( $self, $_[1] );
}

1;