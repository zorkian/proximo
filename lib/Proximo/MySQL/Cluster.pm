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
use Proximo::Cluster;
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
    $self->SUPER::new( @_ );

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

#############################################################################
#############################################################################
#############################################################################

package Proximo::MySQL::Cluster::Instance;

use strict;
use Proximo::MySQL::Cluster;
use base 'Proximo::MySQL::Cluster';



1;