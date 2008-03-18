#!/usr/bin/perl

package Proximo;

use strict;
use Carp qw/ confess /;
use Data::Dumper;
use Proximo::Configuration;
use Proximo::Service;
use Proximo::Socket;

use constant VERBOSITY => 3; # 0 = quiet (no warnings), 1 = normal (warnings),
                             # 2 = verbose (info), 3 = very verbose (debug)

our $VERSION = '0.01';
our ( %Hooks, $QLogFN );

# create a new instance of Proximo ... basically defines what exactly we are
# going to be doing and how we're getting there
sub LoadConfigFile {
    my ( $class, $fn ) = @_;
    Proximo::info( 'Loading configuration from %s.', $fn );
    Proximo::Configuration::load_config_file( $fn );
}

# turns the query log on/off and where to put it
sub QueryLog {
    my ( $class, $fn ) = @_;
    Proximo::info( 'Setting query log to %s.', $fn );
    $QLogFN = $fn;
    Proximo::log( 'Starting log.' );
}

# called on a fatal error.  put ## in your string to insert the contents of
# any error message that we have in $@ or $!
sub fatal {
    my ( $string, @args ) = @_;

    my $err = $@ || $! || "no error message";
    $string =~ s/##/$err/;

    $string =~ s/[\r\n]+$//;
    die sprintf( "[EXIT] \%s > $string\n", scalar(localtime), @args );
}

# print out a warning on the STDERR and call it good
sub warn {
    my ( $string, @args ) = @_;

    $string =~ s/[\r\n]+$//;
    warn sprintf( "[WARN] \%s > $string\n", scalar(localtime), @args )
        if VERBOSITY >= 1;

    return undef;
}

# call as if this was printf ... prints out some text if the verbose option
# is set, or if we're logging at above a certain level
sub info {
    my ( $string, @args ) = @_;

    $string =~ s/[\r\n]+$//;
    printf "[INFO] \%s > $string\n", scalar(localtime), @args
        if VERBOSITY >= 2;

    return 1;
}

# ultra verbose debugging stuff
sub debug {
    my ( $string, @args ) = @_;

    $string =~ s/[\r\n]+$//;
    printf "[DBUG] \%s > $string\n", scalar(localtime), @args
        if VERBOSITY >= 3;
}

# writes to the query log
sub log {
    my ( $string, @args ) = @_;
    return unless defined $QLogFN;
    open FILE, ">>$QLogFN"
        or Proximo::fatal( 'Unable to write to query log: ##.' );
    print FILE sprintf( "[QLOG] \%s > $string\n", scalar(localtime), @args );
    close FILE;
}

# die with a backtrace
sub bt {
    Proximo::warn( @_ );
    confess();
}

# all purpose debugging for dumping stuff out
sub d {
    print Dumper( $_ )
        foreach @_;
}

# called by someone when they want to hook into the main Proximo functionality
# and get some callback when something interesting happens.  this is the main
# way that people can customize the behavior of this proxy.
sub add_hook {
    my ( $name, $cb ) = @_;

    push @{$Hooks{$name} ||= []}, $cb;

    return scalar( @{ $Hooks{$name} } );
}

# used internally when we want to call hooks.  this particular invocation
# executes all of the hooks in a given list and ignores the return values of
# those hooks.
sub _run_hooks {
    my ( $name, @args ) = @_;

    Proximo::debug( "Executing all hooks: $name" );

    foreach my $cb ( @{ $Hooks{$name} || [] } ) {
        $cb->( @args );
    }

    return undef;
}

# used internally when we want to call a single hook.  this will run the
# hooks from top to bottom and stop as soon as a hook returns a defined value.
# if you don't want your hook to stop the execution, just return undef.
sub _try_hooks {
    my ( $name, @args ) = @_;

    Proximo::debug( 'Finding first matching hook: %s.', $name );

    foreach my $cb ( @{ $Hooks{$name} || [] } ) {
        my $rv = $cb->( @args );
        if ( defined $rv ) {
            Proximo::debug( 'Found matching hook: rv=%s.', $rv );
            return $rv;
        }
    }

    Proximo::debug( 'Found no matching hook, falling through.' );
    return undef;
}

# called internally every loop cycle so that we can do things related to
# housekeeping.  this is safe to call whenever you want but it should never
# be called by end-users, only by Proximo.
sub _loop_maintenance {
    # nothing for now...
}

# called by anybody using us when they want us to go ahead and start running
# the main loop.  this means we now have control of the situation.  if the
# user wants to process things, they can insert themselves into one of our
# various hooks, or use the main callback.
sub Run {
    # annotate that we're off to the races
    Proximo::info( 'Proximo beginning main execution.' );

    # ensure we have some services to run
    Proximo::fatal( 'No configured services, unable to run.' )
        unless scalar( %{ Proximo::Service->GetServices } ) > 0;

    # this actually passes through to Danga::Socket ...
    Proximo::Socket->SetLoopTimeout( 5000 );
    Proximo::Socket->SetPostLoopCallback( sub {
            Proximo::_loop_maintenance();
            return 1;
        } );

    # we're ready to go, let's roll
    eval {
        Proximo::Socket->EventLoop();
    };

    # if we had an error, report it, else roll out
    die "$@\n" if $@;
    Proximo::info( "Proximo clean shutdown, good-bye!" );
}

1;
