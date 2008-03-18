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
use Proximo::MySQL::Query;
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
    $self->{backends} = {};

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
    my ( $inst, $ipport, $allow_writes ) = ( $_[1], $_[2], $_[3] );
    return Proximo::warn( 'Low level failure in _get_backend.' )
        unless defined $inst && defined $ipport;

    # try to return the first available backend, we have to be wary about
    # them being closed here
    my $key = "$allow_writes-$ipport";
    while ( my $be = shift( @{ $self->{backends}->{$key} || [] } ) ) {
        # do a little sanity check
        unless ( $be->state eq 'idle' &&
                 $be->allow_writes == $allow_writes ) {
            # ouch, guess something is bogus, kill this one
            $be->close( 'insane' );
            next;
        }

        Proximo::debug( 'EXISTING backend to %s.', $key );
        return $be;
    }

    # guess none available, so make a new one and return it.  note that we
    # attempt to create the backends hash entry so later we can accept this
    # node coming back to us.
    Proximo::debug( 'CREATING backend to %s.', $key );
    $self->{backends}->{$key} ||= [];
    return Proximo::MySQL::Backend->new( $inst, $ipport, $allow_writes );
}

# called when a backend is now available
sub backend_available {
    my Proximo::MySQL::Cluster $self = $_[0];
    my Proximo::MySQL::Backend $be = $_[1];

    # deny this backend if the instance is pinning
    my $inst = $be->inst;
    if ( $inst && $inst->pins ) {
        Proximo::debug( 'Cluster keeping pinned backend out of the pool.' );
        $inst->backend( undef );
        return 1;
    }

    # push onto end of list for the ipport
    my $key = $be->allow_writes . '-' . $be->ipport;
    return Proximo::warn( 'Cluster %s told about backend %s which is not ours?', $self->name, $key )
        unless exists $self->{backends}->{$key};

    # now detach it from the instance it had
    if ( $inst ) {
        $inst->backend( undef );
        $be->inst( undef );
    }

    # we're good, note and store
    Proximo::debug( 'Cluster %s accepting idle backend %s to pool.', $self->name, $key );
    push @{ $self->{backends}->{$key} ||= [] }, $be;
    return 1;
}

# so in general, we should be able to say "give me something I can send a
# read only query to" ... this is just a generic function that respects the
# current setup and status
sub get_readonly_backend {
    my Proximo::MySQL::Cluster $self = $_[0];
    my Proximo::MySQL::Cluster::Instance $inst = $_[1];

    # in single mode, we pull the one master ipport
    my $ipport;
    if ( $self->is_setup_single ) {
        $ipport = $self->{masters}->[0];

    # in random mode, we just pick a random from the master, since everything
    # is assumed to be read/write...
    } elsif ( $self->is_setup_random ) {
        $ipport = $self->{masters}->[ int( rand( scalar( @{ $self->{masters} } ) ) ) ];

    # in the master-master case, we are using a single machine until something
    # causes us to decide to failover
    } elsif ( $self->is_setup_master_master ) {

    # readonly uses random slaves, master-slave uses random slaves
    } elsif ( $self->is_setup_readonly || $self->is_setup_master_slave ) {
        $ipport = $self->{slaves}->[ int( rand( scalar( @{ $self->{slaves} } ) ) ) ];

    # uh big problem?
    } else {
        Proximo::fatal( 'Big trouble in little China.  Cluster mode unknown.' );

    }

    # return new based on set ipport
    return Proximo::warn( 'Unable to decide on backend.' )
        unless defined $ipport;
    return $self->_get_backend( $inst, $ipport, 0 );
}

# writes go here
sub get_readwrite_backend {
    my Proximo::MySQL::Cluster $self = $_[0];
    my Proximo::MySQL::Cluster::Instance $inst = $_[1];

    # in single mode, we pull the one master ipport
    my $ipport;
    if ( $self->is_setup_single ) {
        $ipport = $self->{masters}->[0];

    # in random mode, we just pick a random from the master, since everything
    # is assumed to be read/write... same with master slave
    } elsif ( $self->is_setup_random || $self->is_setup_master_slave ) {
        $ipport = $self->{masters}->[ int( rand( scalar( @{ $self->{masters} } ) ) ) ];

    # in the master-master case, we are using a single machine until something
    # causes us to decide to failover
    } elsif ( $self->is_setup_master_master ) {

    # there are no masters for writing in readonly...
    } elsif ( $self->is_setup_readonly ) {

    # uh big problem?
    } else {
        Proximo::fatal( 'Big trouble in little China.  Cluster mode unknown.' );

    }

    # return new based on set ipport
    return Proximo::warn( 'Unable to decide on backend.' )
        unless defined $ipport;
    return $self->_get_backend( $inst, $ipport, 1 );
}

# takes in a query and does something with it, notably executing it ideally
sub query {
    my Proximo::MySQL::Cluster $self = $_[0];
    my Proximo::MySQL::Cluster::Instance $inst = $_[1];
    my ( $q_type, $q_ref ) = ( $_[2], $_[3] );

    # annotate that we've gotten this far
    #Proximo::debug( 'Cluster %s handling type=%d query=%s.', $self->name, $q_type, $$q_ref );

    # if we're sticky and NOT in a transaction, then check the time of our last write
    # to see if we can disable the sticky bit and go back to normal read handling
    if ( $inst->sticky && ! $inst->in_transaction ) {
        # FIXME: make this timeout configurable
        if ( $inst->last_write_age > 5 ) {
            # suitable, drop the sticky bit
            Proximo::debug( 'Dropping sticky bit.' );
            $inst->sticky( 0 );

        # FIXME: probably remove this debugging too :-)
        } else {
            Proximo::debug( 'Maintaining sticky bit for a few more seconds.' );

        }
    }

    # analyze the query
    my $q = Proximo::MySQL::Query->new( $q_type, $q_ref );

    # if it's a write, we peg sticky on and note that
    if ( $q->is_write ) {
        $inst->sticky( 1 );
        $inst->note_write_query;

    # it might be a state command?  if so, then we need to enable pinning
    } elsif ( $q->is_state_command ) {
        # add the state command
        Proximo::debug( 'Detected state command, pinning enabled for client.' );
        $inst->add_state_command( $$q_ref );

        # and now execute it on backends
        foreach my $be ( $inst->pinned_readonly_backend, $inst->pinned_readwrite_backend ) {
            next unless defined $be;

            Proximo::debug( '%s running state command immediately: %s.', $be, $$q_ref );
            $be->run_state_command( $$q_ref );
        }

        # blow away a backend if we had one sitting around
        if ( my $be = $inst->backend ) {
            # detach this backend from the instance, otherwise closing it will blow away the
            # client, which would be sad.  NOTE: this is normally all done by backend_available,
            # I don't really like doing this here.  be that as it may...
            $be->inst( undef );
            $inst->backend( undef );

            # we only care if we have executed state commands on this backend, if so,
            # then blow it away
            if ( $be->state_commands ) {
                $be->close( 'closing_stateful' );

            # if we didn't then we can return this to the pool
            } else {
                $self->backend_available( $be );

            }
        }

        # since this is just a state command we just return
        # FIXME: this assumes that the user is sending a valid state command, we probably
        # don't want to be doing this...
        return $inst->client->_send_packet(
                Proximo::MySQL::Packet::OK->new( $inst->client, $inst->client->next_sequence ),
            );
    }

    # helper sub, this does the right thing for sending a query to a particular
    # backend, regardless of what it us
    my $query_to = sub {
        my $allow_writes = $_[0];
        my Proximo::MySQL::Backend $be = $_[1];

        # note that the backend could be undefined
        if ( defined $be ) {
            # query log goes here
            my $qq = $$q_ref;
            $qq =~ s/[\r\n\0]+/ /sg;
            Proximo::log( '[%s:%d -> %s(%d): %s%s%s] %s',
                          $inst->client->remote_ip, $inst->client->remote_port, $be->ipport, $be->command_count,
                          ( $allow_writes ? 'rw' : 'ro' ), ( $inst->sticky ? ' sticky' : '' ),
                          ( $inst->pins ? ' pins' : '' ), $qq );

            # pin it if necessary
            if ( $inst->state_commands ) {
                # pin this backend
                if ( $allow_writes ) {
                    $inst->pinned_readwrite_backend( $be );
                } else {
                    $inst->pinned_readonly_backend( $be );
                }
                
                # now execute state commands if needed
                # FIXME: this is grossly inefficient with any sort of volume, this needs to be redone in
                # a way that will work well at scale.  perhaps we keep a generation count of the number of
                # state commands used, then we just compare a simple number.
                $be->run_state_command( $_ )
                    foreach @{ $inst->state_commands };
            }

            # but if it's not, send a packet
            $inst->backend( $be );
            $inst->backend->send_packet(
                    Proximo::MySQL::Packet::Command->new( $q_type, $q_ref )
                );

        # and if it is, we send an error
        } else {
            $inst->client->_send_packet(
                    Proximo::MySQL::Packet::Error->new(
                            $self, $inst->client->next_sequence, 9999, 'Unable to find an appropriate backend for query.'
                        ),
                );
        }
    };

    # so by this point we know what's going on with the query, so let's actually
    # figure out what backend to send it to.  if we're sticky they might have a backend
    # already...
    if ( $inst->sticky ) {
        # see if they have a backend already
        $query_to->( 1,
                     $inst->pinned_readwrite_backend ||
                     $inst->backend ||
                     $self->get_readwrite_backend( $inst ) );

    # if sticky is not on, then let's go ahead and just get them a readable backend
    } else {
        # if they had a backend, free it up.  we do not want to reuse this backend
        # below because if we were sticky (and therefore have a backend) then it is
        # a readwrite backend.  it MAY be the same as a readonly backend, but instead
        # of doing logic here to determine that, we just free it up and get a new one.
        if ( my $be = $inst->backend ) {
            Proximo::debug( 'Freeing up sticky backend.' );
            $self->backend_available( $be );
        }

        # now do the logic of 
        $query_to->( 0,
                     $inst->pinned_readonly_backend ||
                     $self->get_readonly_backend( $inst ) );

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
        'in_trans',  # 1/0; true if we are currently in a transaction
        'last_cmd',  # timestamp of last command (client -> backend)
        'last_pkt',  # timestamp of last packet (backend -> client)
        'last_wrt',  # timestamp of last write command (client -> backend)
        'pinned_ro', # pinned readonly backend
        'pinned_rw', # pinned readwrite backend
        'states',    # statements used for state (i.e. SET commands)
    );

# construct a new instance for somebody
sub new {
    my Proximo::MySQL::Cluster::Instance $self = $_[0];
    $self = fields::new( $self ) unless ref $self;

    # get parameters
    $self->{cluster}   = $_[1];
    $self->{client}    = $_[2];
    $self->{backend}   = undef;
    $self->{sticky}    = 0;
    $self->{in_trans}  = 0;
    $self->{last_cmd}  = time;
    $self->{last_pkt}  = time;
    $self->{last_wrt}  = 0;
    $self->{pinned_ro} = undef;
    $self->{pinned_rw} = undef;
    $self->{states}    = undef;

    # all good
    return $self;
}

# shortcut to return client's service
sub service {
    my Proximo::MySQL::Cluster::Instance $self = $_[0];
    return $self->{client}->service;
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

# return arrayref of state commands
sub state_commands {
    my Proximo::MySQL::Cluster::Instance $self = $_[0];
    return $self->{states};
}

# add command to list of state commands, but only if that exact command is not
# already in the so-called list
sub add_state_command {
    my Proximo::MySQL::Cluster::Instance $self = $_[0];
    foreach my $cmd ( @{ $self->{states} ||= [] } ) {
        return if $cmd eq $_[1];
    }
    push @{ $self->{states} }, $_[1];
    return 1;
}

# get/set our backend
sub backend {
    my Proximo::MySQL::Cluster::Instance $self = $_[0];
    if ( scalar( @_ ) == 2 ) {
        $_[1]->inst( $self )
            if defined $_[1];
        return $self->{backend} = $_[1];
    }
    return $self->{backend};
}

# get/set our backend
sub pinned_readwrite_backend {
    my Proximo::MySQL::Cluster::Instance $self = $_[0];
    if ( scalar( @_ ) == 2 ) {
        Proximo::debug( 'Pinning readwrite backend %s.', $_[1] || '(undef)' );
        $_[1]->inst( $self )
            if defined $_[1];
        return $self->{pinned_rw} = $_[1];
    }
    return $self->{pinned_rw};
}

# get/set our backend
sub pinned_readonly_backend {
    my Proximo::MySQL::Cluster::Instance $self = $_[0];
    if ( scalar( @_ ) == 2 ) {
        Proximo::debug( 'Pinning readonly backend %s.', $_[1] || '(undef)' );
        $_[1]->inst( $self )
            if defined $_[1];
        return $self->{pinned_ro} = $_[1];
    }
    return $self->{pinned_ro};
}

# time of last command
sub last_command {
    my Proximo::MySQL::Cluster::Instance $self = $_[0];
    return $self->{last_cmd};
}

# return time of last packet
sub last_packet {
    my Proximo::MySQL::Cluster::Instance $self = $_[0];
    return $self->{last_pkt};
}

# time of most recent activity either way
sub last_active {
    my Proximo::MySQL::Cluster::Instance $self = $_[0];
    return $self->{last_pkt} > $self->{last_cmd} ? $self->{last_pkt} : $self->{last_cmd};
}

# when last write was
sub last_write {
    my Proximo::MySQL::Cluster::Instance $self = $_[0];
    return $self->{last_wrt};
}

# age
sub last_write_age {
    my Proximo::MySQL::Cluster::Instance $self = $_[0];
    return abs( time - ( $self->{last_wrt} || 0 ) );
}

# note that we just sent a write query
sub note_write_query {
    my Proximo::MySQL::Cluster::Instance $self = $_[0];
    return $self->{last_wrt} = time;
}

# get/set the sticky bit
sub sticky {
    my Proximo::MySQL::Cluster::Instance $self = $_[0];
    if ( scalar( @_ ) == 2 ) {
        return $self->{sticky} = $_[1];
    }
    return $self->{sticky};
}

# note that we're in a transaction, automatically turns sticky on
sub start_transaction {
    my Proximo::MySQL::Cluster::Instance $self = $_[0];
    return if $self->{in_trans};

    # set internal flags and return
    $self->{in_trans} = 1;
    $self->{sticky}   = 1;
    return 1;
}

# stops transaction, DOES NOT turn off sticky though, as we usually want
# reads to be sticky for a little bit.  NOTE: we also set that the last
# write happened now, we treat the entire transaction as a write.
sub end_transaction {
    my Proximo::MySQL::Cluster::Instance $self = $_[0];

    # close transaction, but maintain sticky
    $self->{sticky}   = 1;
    $self->{in_trans} = 0;
    $self->{last_wrt} = time;
    return 1;
}

# returns if we're in a transaction
sub in_transaction {
    my Proximo::MySQL::Cluster::Instance $self = $_[0];
    return $self->{in_trans};
}

# called when the client has sent a query for us to handle, this simply asks the
# cluster what to do with it...
sub query {
    my Proximo::MySQL::Cluster::Instance $self = $_[0];
    $self->{last_cmd} = time;
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

# when a backend finishes with stuff they call this
sub backend_idle {
    my Proximo::MySQL::Cluster::Instance $self = $_[0];

    # if sticky, bail
    return 1 if $self->sticky;

    # okay, let's free it up to the pool
    my $be = $self->backend;
    $self->cluster->backend_available( $be )
        if defined $be;
    return 1;
}

# true if we are pinning backends
sub pins {
    my Proximo::MySQL::Cluster::Instance $self = $_[0];
    return $self->{states} ? 1 : 0;
}

1;