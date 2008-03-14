#!/usr/bin/perl
#
# does basic query analysis to determine what kind of query we have.  this is in
# a separate module because it's a really good candidate for an XS module, and
# I want to have it separated out early on.

package Proximo::MySQL::Query;

use strict;

use fields (
        'raw_qref',    # scalar ref of query
        'is_write',    # is a write query
    );

# construct a new query object
sub new {
    my Proximo::MySQL::Query $self = $_[0];
    $self = fields::new( $self ) unless ref $self;

    # set innards
    $self->{raw_qref} = $_[1];
    $self->{is_write} = undef;

    # and analyze!
    $self->analyze;

    return $self;
}

# analyze our query
sub analyze {
    my Proximo::MySQL::Query $self = $_[0];

    # we use this as a simple flag, if it's defined we've already analyzed
    return if defined $self->{is_write};

    # FIXME: this is retarded logic, we should probably make this somewhat more complicated
    # and detect more cases... oh well, I don't think it matters TOO much, as long as we get
    # 90% of the queries we're good...
    if ( $$self->{raw_qref} =~ /^\s*SELECT.*?(GET_LOCK|RELEASE_LOCK)?.*?(FOR\s+UPDATE|LOCK\s+IN\s+SHARE\s+MODE)?.*$/i ) {
        # it may be a write if we have $1/$2
        if ( $1 || $2 ) {
            Proximo::debug( 'Query suspected a write: %s, %s.', $1 || '(undef)', $2 || '(undef)' );
            $self->{is_write} = 1;
        } else {
            Proximo::debug( 'Query suspected a read.' );
            $self->{is_write} = 0;
        }

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