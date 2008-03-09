#!/usr/bin/perl

package Proximo::MySQL::Packet;

use strict;
use Proximo::MySQL::Constants;

use fields (
        'type',     # references the packet type
        'seq',      # sequence number of this packet 0-255
        'raw',      # scalar-ref to the raw packet contents
    );
    
# general purpose helper to parse a stream and make packets
sub new_from_raw {
    my Proximo::MySQL::Packet $self = shift;

    # get arguments
    $self->{seq} = shift;

    # prepare to run
    my $bufref = shift;
    my $bufpos = 0;
    my $buflen = length( $$bufref );

    # decode a length coded binary number and adjusts the buffer position accordingly
    my $lcbin = sub {
        my $first = unpack( 'C', substr( $$bufref, $bufpos++, 1 ) );

        # easy cases
        return $first
            if $first <= 250; # this is the value
        return undef
            if $first == 251; # null

        # 16 bit number (short)
        if ( $first == 252 ) {
            my $out = unpack( 'v', substr( $$bufref, $bufpos, 2 ) );
            $bufpos += 2;
            return $out;

        # 24 bit number (fucked up)
        } elsif ( $first == 253 ) {
            Proximo::fatal( 'Lazy bastard, implement type 253 P_LCBIN!' );

        # 64 bit number
        } elsif ( $first == 254 ) {
            Proximo::fatal( 'More laziness, figure out 64 bit unpack.' );

        }
    };

    # now iterate over the packets and start pulling data out of the buffer
    my @output;
    foreach my $type ( @_ ) {
        Proximo::fatal( 'Got bad packet or bad format: buflen=%d, bufpos=%d, type=%d.', $buflen, $bufpos, $type )
            if $bufpos > $buflen;

        # now decompile...
        if ( $type == P_BYTE ) {
            push @output, unpack( 'C', substr( $$bufref, $bufpos, 1 ) );
            $bufpos++;

        } elsif ( $type == P_SHORT ) {
            push @output, unpack( 'v', substr( $$bufref, $bufpos, 2 ) );
            $bufpos += 2;
            
        } elsif ( $type == P_LONG ) {
            push @output, unpack( 'V', substr( $$bufref, $bufpos, 4 ) );
            $bufpos += 4;

        } elsif ( $type == P_NULLSTR ) {
            my $start = $bufpos;
            $bufpos++
                while $bufpos <= $buflen &&
                      substr( $$bufref, $bufpos, 1 ) ne "\0"; 
            push @output, substr( $$bufref, $start, $bufpos - $start );
            $bufpos++;

        } elsif ( $type == P_LCSTR ) {
            my $len = unpack( 'C', substr( $$bufref, $bufpos, 1 ) );
            push @output, substr( $$bufref, $bufpos + 1, $len );
            $bufpos += $len + 1;

        } elsif ( $type == P_LCBIN ) {
            push @output, $lcbin->();

        } elsif ( $type == P_RAW ) {
            push @output, substr( $$bufref, $bufpos );
            $bufpos = $buflen + 1;

        } elsif ( $type == P_FILL23 ) {
            push @output, undef;
            $bufpos += 23;

        } elsif ( $type == P_FILL13 ) {
            push @output, undef;
            $bufpos += 13;

        } elsif ( $type == P_GRAB8 ) {
            push @output, substr( $$bufref, $bufpos, 8 );
            $bufpos += 8;

        } elsif ( $type == P_GRAB13 ) {
            push @output, substr( $$bufref, $bufpos, 13 );
            $bufpos += 13;

        } else {
            Proximo::fatal( 'Unknown packet contents type in parse %d.', $type );
            
        }
    }

    return @output;
}
    
# construct a bare packet based on some data
sub new {
    my Proximo::MySQL::Packet $self = shift;
    $self = fields::new( $self ) unless ref $self;

    # input is type, sequence ... but type may be undef
    my ( $type, $seq ) = @_;
    $self->{type} = $type;
    $self->{seq}  = $seq;

    # error if seq is unexpected
    Proximo::fatal( 'Sequence %d out of range.', $self->{seq} )
        if $self->{seq} < 0 || $self->{seq} > 255;

    return $self;
}

# construct the packet
sub _build {
    my Proximo::MySQL::Packet $self = shift;

    # assemble the packet into this buffer
    my $buf = "";

    # helper for the two places we have to do this
    my $lcbin = sub {
        my $val = $_[0];
        if ( $val <= 250 ) {
            return pack( 'C', $val );
        } elsif ( $val <= 1 << 16 ) {
            return pack( 'C', 252 ) . pack( 'v', $val );
        } elsif ( $val <= 1 << 24 ) {
            return pack( 'C', 253 ) . substr( pack( 'V', $val ), 0, 3 );
        } else {
            # FIXME: verify if Q is the right pack re: endianness... I'm thinking I might
            # have to do something more exciting here?
            return pack( 'C', 254 ) . pack( 'Q', $val );
        }  
    };

    # only assemble if we have something
    if ( scalar( @_ ) > 0 ) {
        my $ct = int( scalar( @_ ) / 2 );
        foreach my $num ( 0..$ct-1 ) {
            my ( $type, $val ) = ( $_[$num*2], $_[$num*2+1] );

            if    ( $type == P_BYTE    ) { $buf .= pack( 'C', $val );                  }
            elsif ( $type == P_SHORT   ) { $buf .= pack( 'v', $val );                  }
            elsif ( $type == P_LONG    ) { $buf .= pack( 'V', $val );                  }
            elsif ( $type == P_NULLSTR ) { $buf .= $val . "\0";                        }
            elsif ( $type == P_RAW     ) { $buf .= $val;                               }
            elsif ( $type == P_LCBIN   ) { $buf .= $lcbin->($val);                     }
            elsif ( $type == P_LCSTR   ) {
                # undef is NULL which gets a special treatment
                if ( defined $val ) {
                    $buf .= $lcbin->( length( $val ) ) . $val;
                } else {
                    $buf .= pack( 'C', 251 );
                }
            }
            else  { Proximo::fatal( 'Unknown packet contents type %d.', $type );      }
        }
    }

    # prepend the 3 byte length, our packet id, and call it good
    $buf = substr( pack( 'V', length( $buf ) ), 0, 3) . chr( $self->{seq} ) . $buf;

    # debugging makes us happy (I am OCD about comments...)
    Proximo::debug( "Built packet of \%d bytes.", length( $buf ) );
    
    # save the raw, return length of this new total packet
    $self->{raw} = \$buf;
    return length( $buf );
}

# return the wire representation of this packet
sub _raw {
    my Proximo::MySQL::Packet $self = shift;

    return $self->{raw};
}

# return the current sequence
sub sequence_number {
    my Proximo::MySQL::Packet $self = shift;
    return $self->{seq};
}

#############################################################################
#############################################################################
#############################################################################

package Proximo::MySQL::Packet::ServerHandshakeInitialization;

use strict;
use Proximo::MySQL::Constants;
use Proximo::MySQL::Packet;
use base 'Proximo::MySQL::Packet';

use fields (
        'scramble',    # the scramble buffer used for password stuff
        'protocol_id',
        'version',
        'thread_id',
        'flags',
        'charset',
        'caps',        # capabilities
    );

# construct a new packet for sending the initialization to the client
sub new {
    my Proximo::MySQL::Packet::ServerHandshakeInitialization $self = $_[0];
    $self = fields::new( $self ) unless ref $self;

    my Proximo::MySQL::Connection $conn = $_[1];
    
    # new packet, no type, new sequence
    $self->SUPER::new( undef, 0 );

    # setup a new scramble buff of 23 bytes...
    $self->{scramble} = '';
    $self->{scramble} .= chr( int( rand( 256 ) ) )
        foreach 1..21;

    # now put together the packet itself
    $self->_build(
            P_BYTE,    10,              # protocol version 10, this is 4.1-ish
            P_NULLSTR, "Proximo-" . $Proximo::VERSION,
            P_LONG,    $conn->{fd},     # our thread id (aka the fd)
            P_RAW,     substr( $self->{scramble}, 0, 8 ),
            P_BYTE,    0,               # a null filler byte
            P_SHORT,   CLIENT_LONG_PASSWORD | CLIENT_CONNECT_WITH_DB | CLIENT_PROTOCOL_41 | CLIENT_SECURE_CONNECTION,
            P_BYTE,    0x21,            # FIXME: something other than latin?
            P_SHORT,   SERVER_STATUS_AUTOCOMMIT,
            P_RAW,     "\0" x 13,       # more filler bytes
            P_RAW,     substr( $self->{scramble}, 8, 13 ),
        );

    return $self;
}

# instantiate packet from network stream
sub new_from_raw {
    my Proximo::MySQL::Packet::ServerHandshakeInitialization $self = $_[0];
    $self = fields::new( $self ) unless ref $self;

    # now get the bytes and back-convert it
    my ( $seq, $bytes ) = ( $_[1], $_[2] );
    my @data = $self->SUPER::new_from_raw(
            $seq, $bytes,
            P_BYTE, P_NULLSTR, P_LONG, P_GRAB8, P_BYTE, P_SHORT, P_BYTE,
            P_SHORT, P_FILL13, P_GRAB13,
        );

    # load up the data
    $self->{protocol_id} = $data[0];
    $self->{version}     = $data[1];
    $self->{thread_id}   = $data[2];
    $self->{scramble}    = $data[3] . $data[9]; # two chunks
    $self->{flags}       = $data[5];
    $self->{charset}     = $data[6];
    $self->{caps}        = $data[7];

    return $self;
}

# return the contents of the scramble buffer, should be 21 bytes of random noise
# generated when this packet was created
sub scramble_buffer {
    my Proximo::MySQL::Packet::ServerHandshakeInitialization $self = $_[0];
    return $self->{scramble};
}

# I'm not going to comment the rest of these, OCD be damned
sub protocol_id {
    my Proximo::MySQL::Packet::ServerHandshakeInitialization $self = $_[0];
    return $self->{protocol_id};
}

sub server_version {
    my Proximo::MySQL::Packet::ServerHandshakeInitialization $self = $_[0];
    return $self->{version};
}

sub thread_id {
    my Proximo::MySQL::Packet::ServerHandshakeInitialization $self = $_[0];
    return $self->{thread_id};
}

sub server_flags {
    my Proximo::MySQL::Packet::ServerHandshakeInitialization $self = $_[0];
    return $self->{flags};
}

sub server_language {
    my Proximo::MySQL::Packet::ServerHandshakeInitialization $self = $_[0];
    return $self->{charset};
}

sub server_status {
    my Proximo::MySQL::Packet::ServerHandshakeInitialization $self = $_[0];
    return $self->{caps};
}

#############################################################################
#############################################################################
#############################################################################

package Proximo::MySQL::Packet::ClientAuthentication;

use strict;
use Proximo::MySQL::Constants;
use Proximo::MySQL::Packet;
use base 'Proximo::MySQL::Packet';

use fields (
        'client_flags',
        'max_packet_size',
        'charset_number',
        'user',
        'scramble',
        'database',
    );

# create a packet to send out
sub new {
    my Proximo::MySQL::Packet::ClientAuthentication $self = $_[0];
    $self = fields::new( $self ) unless ref $self;

    my Proximo::MySQL::Connection $conn = $_[1];
    
    # new packet, no type, new sequence
    $self->SUPER::new( undef, $_[2] );

    # now put together the packet itself
    $self->_build(
            P_LONG,    CLIENT_LONG_PASSWORD | CLIENT_CONNECT_WITH_DB | CLIENT_PROTOCOL_41 | CLIENT_SECURE_CONNECTION,
            P_LONG,    16777216,    # max packet size
            P_BYTE,    0x21,        # language, again, latin?
            P_RAW,     "\0" x 23,   # filler
            P_NULLSTR, $conn->service->proxy_user,
            P_LCBIN,   0,           # SCRAMBLE BUFF GOES HERE FIXME FIXME
            P_BYTE,    0,           # filler
            P_NULLSTR, $conn->current_database,
        );

    return $self;
}

# called when we get this packet sent to us and we want to build it up from
# some raw bytes
sub new_from_raw {
    my Proximo::MySQL::Packet::ClientAuthentication $self = shift;
    $self = fields::new( $self ) unless ref $self;

    # now get the bytes and back-convert it
    my ( $seq, $bytes ) = @_;
    my @data = $self->SUPER::new_from_raw( $seq, $bytes, P_LONG, P_LONG, P_BYTE, P_FILL23, P_NULLSTR, P_LCSTR, P_NULLSTR );

    # load up the data
    $self->{client_flags} = $data[0];
    $self->{max_packet_size} = $data[1];
    $self->{charset_number} = $data[2];
    $self->{user} = $data[4];
    $self->{scramble} = $data[5];
    $self->{database} = $data[6];

    return $self;
}

# return the database they want to use
sub database {
    my Proximo::MySQL::Packet::ClientAuthentication $self = shift;
    return $self->{database};
}

# return the user they're trying to connect as
sub user {
    my Proximo::MySQL::Packet::ClientAuthentication $self = shift;
    return $self->{user};
}

# return the client's password (scrambled)
sub scramble_buffer {
    my Proximo::MySQL::Packet::ClientAuthentication $self = shift;
    return $self->{scramble};
}

# get the max packet size
sub max_packet_size {
    my Proximo::MySQL::Packet::ClientAuthentication $self = shift;
    return $self->{max_packet_size};
}

#############################################################################
#############################################################################
#############################################################################

package Proximo::MySQL::Packet::Error;

use strict;
use Proximo::MySQL::Constants;
use Proximo::MySQL::Packet;
use base 'Proximo::MySQL::Packet';

use fields (
        'errno',
        'message',
    );
    
sub new {
    my Proximo::MySQL::Packet::Error $self = shift;
    $self = fields::new( $self ) unless ref $self;

    my Proximo::MySQL::Connection $conn = shift;
    my $seq = shift() + 0;
    
    # new packet, no type, new sequence
    $self->SUPER::new( undef, $seq );

    $self->{errno} = shift() + 0;
    $self->{message} = shift;

    # now put together the packet itself
    $self->_build(
            P_BYTE,    0xff,
            P_SHORT,   $self->{errno},
            P_RAW,     '#',
            P_RAW,     'OWNED',
            P_RAW,     $self->{message},  # NOT a null terminated string!
        );

    return $self;
}

#############################################################################
#############################################################################
#############################################################################

package Proximo::MySQL::Packet::OK;

use strict;
use Proximo::MySQL::Constants;
use Proximo::MySQL::Packet;
use base 'Proximo::MySQL::Packet';

use fields (
        'affected_rows',
        'insert_id',
        'server_status',
        'warning_count',
        'message',
    );
    
sub new {
    my Proximo::MySQL::Packet::OK $self = shift;
    $self = fields::new( $self ) unless ref $self;

    my Proximo::MySQL::Connection $conn = shift;
    my $seq = shift() + 0;
    
    # new packet, no type, new sequence
    $self->SUPER::new( undef, $seq );

    # put in arguments
    $self->{affected_rows} = shift() + 0;
    $self->{insert_id} = shift() + 0;
    $self->{server_status} = shift() + 0;
    $self->{warning_count} = shift() + 0;
    $self->{message} = shift() || '';

    # now put together the packet itself
    $self->_build(
            P_BYTE,    0,
            P_LCBIN,   $self->{affected_rows},
            P_LCBIN,   $self->{insert_id},
            P_SHORT,   $self->{server_status},
            P_SHORT,   $self->{warning_count},
            P_RAW,     $self->{message},        # NOT a null terminated string!

        );

    return $self;
}

sub new_from_raw {
    my Proximo::MySQL::Packet::OK $self = $_[0];
    $self = fields::new( $self ) unless ref $self;

    # note the null append to make P_NULLSTR work
    my ( $seq, $bytes ) = ( $_[1], $_[2] );
    my @data = $self->SUPER::new_from_raw( $seq, $bytes, P_BYTE, P_LCBIN, P_LCBIN, P_SHORT, P_SHORT, P_NULLSTR );

    # get data into structures
    $self->{affected_rows} = $data[1];
    $self->{insert_id}     = $data[2];
    $self->{server_status} = $data[3];
    $self->{warning_count} = $data[4];
    $self->{message}       = $data[5];

    return $self;
}

sub message {
    my Proximo::MySQL::Packet::OK $self = $_[0];
    return $self->{message};
}

sub affected_rows {
    my Proximo::MySQL::Packet::OK $self = $_[0];
    return $self->{affected_rows};
}

sub insert_id {
    my Proximo::MySQL::Packet::OK $self = $_[0];
    return $self->{insert_id};
}

sub warning_count {
    my Proximo::MySQL::Packet::OK $self = $_[0];
    return $self->{warning_count};
}

sub server_status {
    my Proximo::MySQL::Packet::OK $self = $_[0];
    return $self->{server_status};
}

#############################################################################
#############################################################################
#############################################################################

package Proximo::MySQL::Packet::Command;

use strict;
use Proximo::MySQL::Constants;
use Proximo::MySQL::Packet;
use base 'Proximo::MySQL::Packet';

use fields (
        'command',
        'arg'
    );
    
sub new_from_raw {
    my Proximo::MySQL::Packet::Command $self = $_[0];
    $self = fields::new( $self ) unless ref $self;

    # now get the bytes and back-convert it
    my ( $seq, $bytes ) = ( $_[1], $_[2] );
    my @data = $self->SUPER::new_from_raw( $seq, $bytes, P_BYTE, P_RAW );

    # load up the data
    $self->{command} = $data[0];
    $self->{arg} = $data[1];

    return $self;
}

sub command_type {
    my Proximo::MySQL::Packet::Command $self = shift;
    return $self->{command};
}

sub argument {
    my Proximo::MySQL::Packet::Command $self = shift;
    return $self->{arg};
}

#############################################################################
#############################################################################
#############################################################################

package Proximo::MySQL::Packet::ResultSetHeader;

use strict;
use Proximo::MySQL::Constants;
use Proximo::MySQL::Packet;
use base 'Proximo::MySQL::Packet';

use fields (
        'field_count',
        'extra',
    );

sub new {
    my Proximo::MySQL::Packet::ResultSetHeader $self = shift;
    $self = fields::new( $self ) unless ref $self;

    my Proximo::MySQL::Connection $conn = shift;
    my $seq = shift() + 0;
    
    # new packet, no type, new sequence
    $self->SUPER::new( undef, $seq );

    # put in arguments
    $self->{field_count} = shift() + 0;
    $self->{extra} = shift() + 0;

    # error checking: this packet is identified by the first LCBIN field having
    # a range of 1-250, so if we're outside of that then bomb...
    Proximo::fatal( 'Attempted to declare result set with %d fields.', $self->{field_count} )
        if $self->{field_count} < 1 ||
           $self->{field_count} > 250;

    # only include extra field if non-zero
    my @extra;
    push @extra, P_LCBIN, $self->{extra}
        if $self->{extra} > 0;

    # now put together the packet itself
    $self->_build(
            P_LCBIN,  $self->{field_count},
            @extra,
        );

    return $self;
}

#############################################################################
#############################################################################
#############################################################################

package Proximo::MySQL::Packet::Field;

use strict;
use Proximo::MySQL::Constants;
use Proximo::MySQL::Packet;
use base 'Proximo::MySQL::Packet';

use fields (
        'catalog',
        'db',
        'table',
        'org_table',
        'name',
        'org_name',
        'charsetnr',
        'length',
        'type',
        'flags',
        'decimals',
        'default',
    );

sub new {
    my Proximo::MySQL::Packet::Field $self = shift;
    $self = fields::new( $self ) unless ref $self;

    my Proximo::MySQL::Connection $conn = shift;
    my $seq = shift() + 0;
    
    # new packet, no type, new sequence
    $self->SUPER::new( undef, $seq );

    # put in arguments
    my %opts = ( @_ );
    $self->{catalog} = 'def';
    $self->{db} = $opts{db} || $opts{database} || '';
    $self->{table} = $opts{table} || '';
    $self->{org_table} = $opts{org_table} || $self->{table} || '';
    $self->{name} = $opts{name} || '';
    $self->{org_name} = $opts{org_name} || $self->{name} || '';
    $self->{charsetnr} = $opts{charset} || 0;
    $self->{length} = $opts{length} || 0;
    $self->{type} = $opts{type} || MYSQL_TYPE_STRING;
    $self->{flags} = $opts{flags} || 0;
    $self->{decimals} = $opts{decimals} || 0;
    $self->{default} = $opts{default};

    # only include extra field if non-zero
    my @extra;
    push @extra, P_LCBIN, $self->{default}
        if defined $self->{default};

    # now put together the packet itself
    $self->_build(
            P_LCSTR,   $self->{catalog},
            P_LCSTR,   $self->{db},
            P_LCSTR,   $self->{table},
            P_LCSTR,   $self->{org_table},
            P_LCSTR,   $self->{name},
            P_LCSTR,   $self->{org_name},
            P_BYTE,    0,
            P_SHORT,   $self->{charsetnr},
            P_LONG,    $self->{length},
            P_BYTE,    $self->{type},
            P_SHORT,   $self->{flags},
            P_BYTE,    $self->{decimals},
            P_SHORT,   0,
            @extra,
        );

    return $self;
}

#############################################################################
#############################################################################
#############################################################################

package Proximo::MySQL::Packet::EOF;

use strict;
use Proximo::MySQL::Constants;
use Proximo::MySQL::Packet;
use base 'Proximo::MySQL::Packet';

use fields (
        'warning_count',
        'status_flags',
    );

sub new {
    my Proximo::MySQL::Packet::EOF $self = shift;
    $self = fields::new( $self ) unless ref $self;

    my Proximo::MySQL::Connection $conn = shift;
    my $seq = shift() + 0;
    
    # new packet, no type, new sequence
    $self->SUPER::new( undef, $seq );

    # put in arguments
    $self->{warning_count} = shift() + 0;
    $self->{status_flags} = shift() + 0;

    # now put together the packet itself
    $self->_build(
            P_BYTE,   0xfe,     # indicate an EOF packet
            P_SHORT,  $self->{warning_count},
            P_SHORT,  $self->{status_flags},
        );

    return $self;
}


#############################################################################
#############################################################################
#############################################################################

package Proximo::MySQL::Packet::RowData;

use strict;
use Proximo::MySQL::Constants;
use Proximo::MySQL::Packet;
use base 'Proximo::MySQL::Packet';

use fields (
        'row_data',
    );

sub new {
    my Proximo::MySQL::Packet::RowData $self = shift;
    $self = fields::new( $self ) unless ref $self;

    my Proximo::MySQL::Connection $conn = shift;
    my $seq = shift() + 0;
    
    # new packet, no type, new sequence
    $self->SUPER::new( undef, $seq );

    # put in arguments
    $self->{row_data} = \@_;

    # now construct output
    my @out;
    push @out, P_LCSTR, $_
        foreach @{ $self->{row_data} };

    # now put together the packet itself
    $self->_build(
            @out
        );

    return $self;
}

1;