#!/usr/bin/perl

package Proximo::Management::Server;

use strict;
use Proximo::Socket;
use base 'Proximo::Socket';

use fields (
        'ctx',  # context for sending commands to the configurator
    );

# construct a new server connection, this is the connection between us
# and the user, whoever they may be
sub new {
    my Proximo::Management::Server $self = shift;
    $self = fields::new( $self ) unless ref $self;

    # initialize the work via our parent
    $self->SUPER::new( @_ );

    # self init
    $self->{ctx} = {};

    # wait for user to send us something
    $self->watch_read( 1 );

    return $self;
}

# when the user sends something, we handle it.  management console is very much
# oriented to this 1:1 response.  we don't typically send them any information
# other than that...
sub event_read {
    my Proximo::Management::Server $self = $_[0];

    # get lines
    while ( my $line = $self->get_line ) {
        # socks command - show open sockets
        if ( $line =~ /^socks/i ) {
            my $desc = Proximo::Socket->DescriptorMap;
            foreach my $sock ( values %$desc ) {
                $self->write_line( $sock->as_string );
            }
            $self->write_line( '---' );

        # list out defined services
        # FIXME: put more detail here :-)
        } elsif ( $line =~ /^show\s+serv/ ) {
            my $svcs = Proximo::Service->GetServices;
            foreach my $svc_name ( sort { $a cmp $b } keys %$svcs ) {
                $self->write_line( '%s: %s', $svc_name, $svcs->{$svc_name} );
            }
            $self->write_line( '---' );

        # maybe it's a configuration command, try that
        } else {
            my $rv = Proximo::Configuration::exec_management_command( $self->{ctx}, $line );
            $self->write_line( $rv ? 'Ok.' : 'Failed.' );

        }
    }
}

1;