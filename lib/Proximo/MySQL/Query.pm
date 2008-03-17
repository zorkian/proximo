#!/usr/bin/perl
#
# does basic query analysis to determine what kind of query we have.  this is in
# a separate module because it's a really good candidate for an XS module, and
# I want to have it separated out early on.

package Proximo::MySQL::Query;

use strict;

use fields (
        'raw_qtype',   # query type
        'raw_qref',    # scalar ref of query
        'is_write',    # is a write query
        'is_stateful', # not a write, but uses state information
        'is_statecmd', # i.e. a SET command, puts some state on the connection 
    );

# construct a new query object
sub new {
    my Proximo::MySQL::Query $self = $_[0];
    $self = fields::new( $self ) unless ref $self;

    # set innards
    $self->{raw_qtype}   = $_[1];
    $self->{raw_qref}    = $_[2];
    $self->{is_write}    = undef;
    $self->{is_stateful} = 0;
    $self->{is_statecmd} = 0;

    # and analyze!
    $self->analyze;

    return $self;
}

# analyze our query
sub analyze {
    my Proximo::MySQL::Query $self = $_[0];

    # we use this as a simple flag, if it's defined we've already analyzed
    return if defined $self->{is_write};

    # if this is type 4 (show fields) then bail early
    if ( $self->{raw_qtype} == 4 ) {
        Proximo::debug( 'List fields query, considering a read.' );
        $self->{is_write} = 0;
        return 1;
    }
    
    # FIXME: we forgot about LAST_INSERT_ID here...

    # FIXME: this is retarded logic, we should probably make this somewhat more complicated
    # and detect more cases... oh well, I don't think it matters TOO much, as long as we get
    # 90% of the queries we're good...
    if ( ${ $self->{raw_qref} } =~ /^\s*(?:SHOW|SELECT).*?(GET_LOCK|RELEASE_LOCK)?.*?(FOR\s+UPDATE|LOCK\s+IN\s+SHARE\s+MODE)?.*$/i ) {
        # it may be a write if we have $1/$2
        if ( $1 || $2 ) {
            Proximo::debug( 'Query suspected a write: %s, %s.', $1 || '(undef)', $2 || '(undef)' );
            $self->{is_write} = 1;
        } else {
            Proximo::debug( 'Query suspected a read.' );
            $self->{is_write} = 0;
        }

    # see if it might be a state command
    } elsif ( ${ $self->{raw_qref} } =~ /^\s*SET\s+/i ) {
        Proximo::debug( 'Query suspected a state command.' );
        $self->{is_write}    = 0;
        $self->{is_statecmd} = 1; 

    # if it flat out didn't match the query, then it's a write
    } else {
        Proximo::debug( 'Query definitely a write.' );
        $self->{is_write} = 1;

    }

    return 1;
}

# return if something is a write or not
sub is_write {
    my Proximo::MySQL::Query $self = $_[0];
    return $self->{is_write};
}

# if something is a state command or not
sub is_state_command {
    my Proximo::MySQL::Query $self = $_[0];
    return $self->{is_statecmd};
}

1;