#!/usr/bin/perl

package Proximo::MySQL::Backend;

use strict;
use IO::Handle;
use Proximo::MySQL::Connection;
use Proximo::MySQL::Constants;
use Proximo::MySQL::Packet;
use Socket qw/ PF_INET IPPROTO_TCP SOCK_STREAM SOL_SOCKET SO_ERROR
               AF_UNIX PF_UNSPEC /;
use base 'Proximo::MySQL::Connection';

use fields (
        'cluster_inst',   # our P::M::Cluster::Instance object
        'pkt',            # arrayref of packets queued
        'ipport',         # initial connect argument
        'cmd_type',       # current command type
        'last_cmd',       # time of last command/packet out
        'writable',       # 1/0; our writability (just for sanity)
        'cmd_count',      # count of commands we've sent
        'state_cmds',     # hash of executed state commands
    );
    
# construction is fun for you and me
sub new {
    my Proximo::MySQL::Backend $self = $_[0];
    $self = fields::new( $self ) unless ref $self;

    # arguments
    my ( $clust, $ipport, $writable ) = ( $_[1], $_[2], $_[3] );
    my ( $ip, $port ) = ( $1, $2 )
        if $ipport =~ /^(.+?):(\d+)$/;

    # setup the socket
    my $sock;
    socket $sock, PF_INET, SOCK_STREAM, IPPROTO_TCP;
    return Proximo::warn( 'Failed creating socket: ##.' )
        unless $sock && defined fileno( $sock );

    # get structures
    my $inet_aton = Socket::inet_aton( $ip )
        or return Proximo::warn( 'Failed to get inet_aton for ip %s.', $ip );
    my $addr = Socket::sockaddr_in( $port, $inet_aton )
        or return Proximo::warn( 'Failed to get sockaddr_in for ip %s port %d.', $ip, $port );

    # non-block and launch the connect
    IO::Handle::blocking( $sock, 0 );
    connect $sock, $addr;

    # initialize the work via our parent
    $self->SUPER::new( $clust->service, $sock, $addr );

    # save our cluster instance
    $self->{cluster_inst} = $clust;
    $self->{pkt}          = [];
    $self->{ipport}       = $ipport;
    $self->{cmd_type}     = undef;
    $self->{last_cmd}     = undef;
    $self->{writable}     = $writable ? 1 : 0;
    $self->{cmd_count}    = 0;
    $self->{state_cmds}   = undef;

    # now turn on watching for reads, as the first thing that happens is
    # the server will send us a packet saying "hey what's up my name's bob"
    $self->current_database( $self->inst->client->current_database );
    $self->state( 'connecting' );
    $self->watch_read( 1 );

    return $self;
}

# return our cluster instance
sub inst {
    my Proximo::MySQL::Backend $self = $_[0];

    # if they're changing the instance we're on...
    if ( scalar( @_ ) == 2 ) {
        my $inst = $self->{cluster_inst} = $_[1];
        #Proximo::debug( '%s has new instance %s.', $self, $inst || '(undef)' );

        # ...then we might need to change databases!
        if ( defined $inst ) {
            #Proximo::debug( 'Backend getting cluster instance: client db=%s, my db=%s.',
            #                $inst->client->current_database, $self->current_database );
            $self->switch_database( $inst->client->current_database )
                if $inst->client->current_database ne $self->current_database;
        }

        # either way, return it
        return $inst;
    }

    # simple return
    return $self->{cluster_inst};
}

# called when we get a packet
sub event_packet {
    my Proximo::MySQL::Backend $self = $_[0];
    
    my ( $seq, $packet_raw ) = ( $_[1], $_[2] );

    # depending on state, could be any sort of packet...
    if ( $self->state eq 'connecting' ) {
        my $packet = Proximo::MySQL::Packet::ServerHandshakeInitialization->new_from_raw( $seq, $packet_raw );
        Proximo::debug( 'Established connection with backend of version %s.', $packet->server_version );

        # now let's send an authentication packet
        $self->state( 'authenticating' );
        $self->_send_packet(
                Proximo::MySQL::Packet::ClientAuthentication->new(
                        $self,
                        $packet->sequence_number + 1,
                        $packet->scramble_buffer,
                    )
            );

    # second stage, get a packet back from the server
    } elsif ( $self->state eq 'authenticating' ) {
        my $peek = ord( substr( $$packet_raw, 0, 1 ) );

        # OK packet
        if ( $peek == 0 ) {
            # we've authenticated, yay
            my $packet = Proximo::MySQL::Packet::OK->new_from_raw( $seq, $packet_raw );
            Proximo::debug( 'Backend server authentication successful.' );
            
            # we just go idle here, this handles stuff
            return $self->idle;

        # error packet
        } elsif ( $peek == 255 ) {
            my $packet = Proximo::MySQL::Packet::Error->new_from_raw( $seq, $packet_raw );
            Proximo::warn( 'Got an error from the server, lame.' );

            # FIXME: is this right?  I'm too tired to really think if this is the proper thing
            # to do in this case.  test, test...
            $self->close( 'error' );

        # something else
        } else {
            Proximo::fatal( 'Really bad peek value %d.', $peek );
            
        }
    
    # used when we're internally changing the database
    } elsif ( $self->state eq 'run_state_command' ) {
        # four possible responses in this case, let's see what we got
        my $val = unpack( 'C', substr( $$packet_raw, 0, 1 ) );

        # okay is cool
        if ( $val == PEEK_OK ) {
            return $self->idle;
            
        # anything else, we just close.  we aren't sure what can be done at this point,
        # if we are unable to switch database, something is gloriously fubar.
        } else {
            if ( $val == PEEK_ERROR ) {
                my $packet = Proximo::MySQL::Packet::Error->new_from_raw( $seq, $packet_raw );
                Proximo::debug( 'Result ERROR: error number = %d, SQL state = %s, message = %s.',
                                $packet->error_number, $packet->sql_state, $packet->message );
            }

            Proximo::warn( 'Unable to run state command!' );
            return $self->close( 'run_state_command_fail' );

        }

        # used when we're internally changing the database
        } elsif ( $self->state eq 'switching_database' ) {
            # four possible responses in this case, let's see what we got
            my $val = unpack( 'C', substr( $$packet_raw, 0, 1 ) );

            # okay is cool
            if ( $val == PEEK_OK ) {
                return $self->idle;

            # anything else, we just close.  we aren't sure what can be done at this point,
            # if we are unable to switch database, something is gloriously fubar.
            } else {
                if ( $val == PEEK_ERROR ) {
                    my $packet = Proximo::MySQL::Packet::Error->new_from_raw( $seq, $packet_raw );
                    Proximo::debug( 'Result ERROR: error number = %d, SQL state = %s, message = %s.',
                                    $packet->error_number, $packet->sql_state, $packet->message );
                }

                Proximo::warn( 'Unable to change database for backend!' );
                return $self->close( 'switch_db_fail' );

            }

    # when we get a response in this state, we can send it to the client
    } elsif ( $self->state eq 'wait_response' ) {
        # four possible responses in this case, let's see what we got
        my $val = unpack( 'C', substr( $$packet_raw, 0, 1 ) );

        # OK happens if we have no result set
        my ( $packet, $go_idle );
        if ( $val == PEEK_OK ) {
            $packet = Proximo::MySQL::Packet::OK->new_from_raw( $seq, $packet_raw );
            Proximo::debug( 'Result OK: server status = %d, affected rows = %d, insert id = %d, warnings = %d, message = %s.',
                            $packet->server_status, $packet->affected_rows, $packet->insert_id, $packet->warning_count,
                            $packet->message || '(none)' );

            # do transaction handling here.  thankfully, MySQL is pretty cool and tells us
            # when a transaction has begun!
            if ( $packet->in_transaction ) {
                Proximo::debug( 'Detected transaction, setting appropriate flags.' );
                $self->inst->start_transaction;

            # well, if this is not on, then we could have potentially ended a transaction
            # with this very command
            } else {
                # so see if we were in one, then end it
                if ( $self->inst->in_transaction ) {
                    Proximo::debug( 'Detected end of transaction.' );
                    $self->inst->end_transaction;
                }

            }
            
            # either way, we go idle
            $go_idle = 1;

        # ERROR packets indicate ... an error
        } elsif ( $val == PEEK_ERROR ) {
            $go_idle = 1;
            $packet = Proximo::MySQL::Packet::Error->new_from_raw( $seq, $packet_raw );
            Proximo::debug( 'Result ERROR: error number = %d, SQL state = %s, message = %s.',
                            $packet->error_number, $packet->sql_state, $packet->message );

        # it should never be an EOF packet
        } elsif ( $val == PEEK_EOF ) { 
            Proximo::warn( 'Backend got EOF packet in wait_response state.' );
            return $self->close( 'unexpected_packet' );

        # else it's the beginning of a fieldset
        } else {
            # FIXME: need new_from_raw for this type of packet
            #my $packet = Proximo::MySQL::Packet::
            $self->state( 'recv_fields' );

        }

        # FIXME: this is manual tweaking we should be able to get rid of.
        if ( defined $packet ) {
            $self->inst->client->_send_packet( $packet );

        } else {
            my $buf = substr( pack( 'V', length( $$packet_raw ) ), 0, 3) . chr( $seq ) . $$packet_raw;
            $self->inst->client->write( \$buf );
        }
        
        # if to go idle...
        $self->idle
            if $go_idle;

    # in this state, the server is sending fields at us
    } elsif ( $self->state eq 'recv_fields' ) {
        # decode peek value
        my $val = unpack( 'C', substr( $$packet_raw, 0, 1 ) );
        
        # FIXME: this is manual and shouldn't be done this way
        my $buf = substr( pack( 'V', length( $$packet_raw ) ), 0, 3) . chr( $seq ) . $$packet_raw;
        $self->inst->client->write( \$buf );

        # if this is an EOF packet, set our state
        if ( $val == PEEK_EOF ) {
            # special case type 4, which is listing fields, as there is no recv_rows set
            if ( $self->command_type == 4 ) {
                $self->idle;
                
            # guess not, so we can treat like normal
            } else {
                $self->state( 'recv_rows' );                
            }
        }

    # server is blasting actual row data at us
    } elsif ( $self->state eq 'recv_rows' ) {
        # decode peek value
        my $val = unpack( 'C', substr( $$packet_raw, 0, 1 ) );
        
        # FIXME: this is manual and shouldn't be done this way
        my $buf = substr( pack( 'V', length( $$packet_raw ) ), 0, 3) . chr( $seq ) . $$packet_raw;
        $self->inst->client->write( \$buf );

        # if this is an EOF packet, set our state
        $self->idle
            if $val == PEEK_EOF;

    # haven't put in any handling for this state?
    } else {
        Proximo::fatal( 'Backend received packet in unexpected state %s.', $self->state );
        
    }
}

# invoke a manual database switch
sub switch_database {
    my Proximo::MySQL::Backend $self = $_[0];

    # set state, send packet
    Proximo::debug( 'Switching database on backend to %s.', $_[1] );
    $self->state( 'switching_database' );
    $self->current_database( $_[1] );
    $self->_send_packet(
            Proximo::MySQL::Packet::Command->new( 2, $_[1] )
        );
    return 1;
}

# call when we are idle
sub idle {
    my Proximo::MySQL::Backend $self = $_[0];

    # set state
    Proximo::debug( '%s going idle.', $self );
    $self->state( 'idle' );
    
    # try to dequeue a packet, which will send stuff out
    $self->_dequeue_packet;
    
    # and now let the backend know we're idle, IF we actually still are!
    # the above dequeue might have changed state
    $self->inst->backend_idle
        if $self->state eq 'idle';
    return 1;
}

# send a packet from the client to the backend
sub send_packet {
    my Proximo::MySQL::Backend $self = $_[0];
    my Proximo::MySQL::Packet $pkt = $_[1];

    # we simply queue, then try to dequeue, which will instantly send
    # the packet if we can, but otherwise will do nothing.  this is done this
    # way to ensure we don't jump the queue on anybody.
    $self->queue_packet( 'wait_response', $pkt );
    $self->_dequeue_packet;
    return 1;
}

# queues up a packet to go out to this backend
sub queue_packet {
    my Proximo::MySQL::Backend $self = $_[0];

    # just push it on and let dequeue handle sending if we need to
    push @{ $self->{pkt} }, [ $_[1], $_[2] ];
    $self->_dequeue_packet;
    return 1;
}

# send a queued packet if we can
sub _dequeue_packet {
    my Proximo::MySQL::Backend $self = $_[0];

    # if we're already idle, see if we have a packet
    if ( $self->state eq 'idle' &&
         ( my $q = shift @{ $self->{pkt} } ) ) {

        # if it's a command packet, we keep track of some data about it which
        # influences how we behave in the future
        if ( ref( $q->[1] ) eq 'Proximo::MySQL::Packet::Command' ) {     
            $self->{last_cmd} = time;
            $self->{cmd_type} = $q->[1]->command_type;
            $self->{cmd_count}++;
        }

        # now setup and go
        $self->state( $q->[0] );
        return $self->_send_packet( $q->[1] );
    }

    # just return
    return 1;
}

# returns our ipport so we know where this backend goes
sub ipport {
    my Proximo::MySQL::Backend $self = $_[0];
    return $self->{ipport};
}

# type of last command sent to backend
sub command_type {
    my Proximo::MySQL::Backend $self = $_[0];
    return $self->{cmd_type};
}

# if we get closed out...
sub close {
    my Proximo::MySQL::Backend $self = $_[0];

    # blow up a client that is still connected
    if ( $self->inst ) {
        # pew pew
        my $cl = $self->inst->client;
        $self->inst->destroy_links;
        $self->inst( undef );

        # proxy this to client
        $cl->close( $_[1] )
            if $cl;
    }

    $self->SUPER::close( $_[1] );
}

# how long we've been idle
sub idle_time {
    my Proximo::MySQL::Backend $self = $_[0];
    return defined $self->{last_cmd} ? ( time - $self->{last_cmd} ) : 'none';
}

# how many commands we've run
sub command_count {
    my Proximo::MySQL::Backend $self = $_[0];
    return $self->{cmd_count};
}

# render ourselves out for the management console
sub as_string {
    my Proximo::MySQL::Backend $self = $_[0];

    return sprintf(
            '%s: connected to %s:%d for %d seconds; state=%s, service=%s, db=%s, idle_time=%s, %s, cmds=%d.',
            ref( $self ), $self->remote_ip, $self->remote_port, time - $self->time_established,
            $self->state, $self->service->name, $self->current_database, $self->idle_time,
            $self->allow_writes ? 'rw' : 'ro', $self->command_count,
        ); 
}

# return writability state (called allow_writes because I'm not sure
# how to spell writability? writeability? and I don't want to codify something
# that I will never remember how to spell... heh)
sub allow_writes {
    my Proximo::MySQL::Backend $self = $_[0];
    return $self->{writable};
}

# return what state commands we've run (hashref, or nothing)
sub state_commands {
    my Proximo::MySQL::Backend $self = $_[0];
    return $self->{state_cmds};
}

# run a state command on this backend
sub run_state_command {
    my Proximo::MySQL::Backend $self = $_[0];
    my $cmd = $_[1];

    # see if we ran this already
    my $hr = ( $self->{state_cmds} ||= {} );
    return 1 if $hr->{$cmd};
    $hr->{$cmd} = 1;

    # send this command to the backend
    Proximo::debug( 'Queueing state command on backend: %s.', $cmd );
    $self->queue_packet(
            'run_state_command',
            Proximo::MySQL::Packet::Command->new( 3, $cmd )
        );
    return 1;
}

1;