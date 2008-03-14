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
        'setup',     # how this cluster is configured (S_* constants)
        'masters',   # array of nodes designated as masters
        'slaves',    # array of nodes designated as slaves
        'readonly',  # if enabled, do not allow writes
        'backends',  # hashref; { "ip:port" => [ queue of free backends ] }
    );

# the 'single' setup, this indicates only a single database, and it is capable
# of doing both reads and writes
use constant S_SINGLE => 0;

# round robin/random databases, used when you don't care where the traffic goes, but
# will of course respect sticky to send write traffic to the same machine from
# the same client.  all databases must be read/write.
use constant S_RANDOM => 1;

# typical master-slave setup with N masters and N slaves.  writes will be sent only
# to the database indicated as master, reads can come from anywhere but we will set
# preference to slaves.  only allows a single master.
use constant S_MASTER_SLAVE => 2;

# master master will send traffic to only one machine at a time, and not send traffic
# to another master unless the first one fails on us.  if you want to send traffic
# to all masters, see S_RANDOM.
use constant S_MASTER_MASTER => 3;

# some number of read-only slaves, does not allow masters, and does not allow writes.
# will throw an error on any query that is detected as being a write.
use constant S_READONLY => 4;

# constructs a new cluster for people to use.  only one of these should be created
# per cluster defined, not one per connection (see P::M::Cluster::Instance for that)
sub new {
    my Proximo::MySQL::Cluster $self = $_[0];
    $self = fields::new( $self ) unless ref $self;

    # init parent
    $self->SUPER::new( $_[1] );

    # self variables
    $self->{setup}    = -1; # error case
    $self->{masters}  = [];
    $self->{slaves}   = [];
    $self->{readonly} = 0;

    return $self;
}

# constructs an instance of this particular cluster and returns it
sub instance {
    my Proximo::MySQL::Cluster $self = $_[0];
    my Proximo::MySQL::Client $client = $_[1];

    # just build a new instance
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
        return Proximo::warn( 'Unable to set masters/slaves until you set the cluster configuration.' )
            unless $self->setup >= 0;

        # now process
        my @new;
        my @ips = split /\s*,\s*/, $val;
        foreach my $ip ( @ips ) {
            return Proximo::warn( 'IP %s not valid in set of %s.', $ip, $key )
                unless $ip =~ /^\d+\.\d+\.\d+\.\d+(?::\d+)?$/;
            $ip = "$ip:3306" unless $ip =~ /:/;
            push @new, $ip;
        }

        # error? nothing?
        return Proximo::warn( 'No databases defined in set.' )
            unless scalar( @new ) > 0;

        # ensure they can do that
        return Proximo::warn( 'Unable to set slaves on cluster in single mode.' )
            if $self->is_setup_single && $key eq 'slaves';
        return Proximo::warn( 'Unable to set slaves on cluster in master-master mode.' )
            if $self->is_setup_master_master && $key eq 'slaves';
        return Proximo::warn( 'Unable to set masters on cluster in readonly mode.' )
            if $self->is_setup_readonly && $key eq 'masters';

        # now do some warning if they are trying to set more than one in the case of single
        return Proximo::warn( 'Unable to add more than one database to cluster in single mode.' )
            if $self->is_setup_single && scalar( @new ) > 1;

        # looks like they're valid, ok, let's replace our list with them...
        $self->{$key} = \@new;
        return 1;

    # maybe they want to change the cluster setup?
    } elsif ( $key eq 'setup' ) {
        return Proximo::warn( 'Invalid setup %s for MySQL cluster.', $key )
            unless $val =~ /^(?:single|random|master-slave|master-master|readonly)$/;
        return Proximo::warn( 'Unable to change configuration of cluster at this time.' )
            if $self->slave_count > 0 || $self->master_count > 0;

        # now set values, see documentation above where we define these constants if
        # you want to know what these particular values mean
        $self->{setup} = S_SINGLE        if $val eq 'single';
        $self->{setup} = S_READONLY      if $val eq 'readonly';
        $self->{setup} = S_RANDOM        if $val eq 'random';
        $self->{setup} = S_MASTER_SLAVE  if $val eq 'master-slave';
        $self->{setup} = S_MASTER_MASTER if $val eq 'master-master';
        return 1;

    # turn readonly on/off
    } elsif ( $key eq 'readonly' ) {
        my $rv = $truefalse->( $val );
        return Proximo::warn( 'Invalid value %s for readonly flag.', $key )
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

# our current setup
sub setup {
    my Proximo::MySQL::Cluster $self = $_[0];
    return $self->{setup};
}

# convenience accessors
sub is_setup_single {
    my Proximo::MySQL::Cluster $self = $_[0];
    return $self->{setup} == S_SINGLE;
}

sub is_setup_random {
    my Proximo::MySQL::Cluster $self = $_[0];
    return $self->{setup} == S_RANDOM;
}

sub is_setup_master_slave {
    my Proximo::MySQL::Cluster $self = $_[0];
    return $self->{setup} == S_MASTER_SLAVE;
}

sub is_setup_master_master {
    my Proximo::MySQL::Cluster $self = $_[0];
    return $self->{setup} == S_MASTER_MASTER;
}

sub is_setup_readonly {
    my Proximo::MySQL::Cluster $self = $_[0];
    return $self->{setup} == S_READONLY;
}

# internal: given an ip:port, get a backend and if we can't get one then go
# ahead and spawn one
sub _get_backend {
    my Proximo::MySQL::Cluster $self = $_[0];
    my ( $ipport, $cb ) = ( $_[1], $_[2] );

    my $be = shift( @{ $self->{backends}->{$ipport} } );
    return $cb->( $be ) if $be;

    return Proximo::MySQL::Backend->new( )
}

# so in general, we should be able to say "give me something I can send a
# read only query to" ... this is just a generic function that respects the
# current setup and status
sub get_readonly_backend {
    my Proximo::MySQL::Cluster $self = $_[0];

    # depending on mode we pull from a different area
}

# takes in a query and does something with it, notably executing it ideally
sub query {
    my Proximo::MySQL::Cluster $self = $_[0];
    my Proximo::MySQL::Cluster::Instance $instance = $_[1];
    my ( $q_type, $q_ref ) = ( $_[2], $_[3] );

=pod

okay, let's see how this works out... we have a command type and a query reference
at this point, plus we know what the state of the instance is

if we are sticky right now:
    only choices are to use existing backend or go unsticky
if we are not sticky:
    choices are to either get random backend or go sticky
after the result comes back:
    determinator for going unsticky, might be useful

so we need determinators for 'should go sticky' or 'should go unsticky' which is
going to depend on the exact state and query being run

are we going to do this logic here?  seems to make more sense to have this logic
separated out into cluster logic classes... or maybe plugins?  maybe that makes
the most sense...


=cut

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

        # see if we should stop being sticky
        $be->do_command( $q_type, $q_ref );
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
    return $self->cluster->query( $self, $_[1], $_[2] );
}

# close up connections
sub destroy_links {
    my Proximo::MySQL::Cluster::Instance $self = $_[0];

    # undef all links
    $self->{cluster} = undef;
    $self->{client}  = undef;
    $self->{backend} = undef;
    return 1;
}

1;