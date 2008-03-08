#!/usr/bin/perl
#
# these constants were taken wholesale from DBIx::MyServer, which itself
# took them from the MySQL source code.  I'm going to assume they are all
# correct... :-)

package Proximo::MySQL::Constants;

require Exporter;

use constant CLIENT_LONG_PASSWORD           => 1;
use constant CLIENT_FOUND_ROWS              => 2;
use constant CLIENT_LONG_FLAG               => 4;
use constant CLIENT_CONNECT_WITH_DB         => 8;
use constant CLIENT_NO_SCHEMA               => 16;
use constant CLIENT_COMPRESS                => 32;
use constant CLIENT_ODBC                    => 64;
use constant CLIENT_LOCAL_FILES             => 128;
use constant CLIENT_IGNORE_SPACE            => 256;
use constant CLIENT_PROTOCOL_41             => 512;
use constant CLIENT_INTERACTIVE             => 1024;
use constant CLIENT_SSL                     => 2048;
use constant CLIENT_IGNORE_SIGPIPE          => 4096;
use constant CLIENT_TRANSACTIONS            => 8192;
use constant CLIENT_RESERVED                => 16384;
use constant CLIENT_SECURE_CONNECTION       => 32768;
use constant CLIENT_MULTI_STATEMENTS        => 1 << 16;
use constant CLIENT_MULTI_RESULTS           => 1 << 17;
use constant CLIENT_SSL_VERIFY_SERVER_CERT  => 1 << 30;
use constant CLIENT_REMEMBER_OPTIONS        => 1 << 31;

use constant SERVER_STATUS_IN_TRANS             => 1;
use constant SERVER_STATUS_AUTOCOMMIT           => 2;
use constant SERVER_MORE_RESULTS_EXISTS         => 8;
use constant SERVER_QUERY_NO_GOOD_INDEX_USED    => 16;
use constant SERVER_QUERY_NO_INDEX_USED         => 32;
use constant SERVER_STATUS_CURSOR_EXISTS        => 64;
use constant SERVER_STATUS_LAST_ROW_SENT        => 128;
use constant SERVER_STATUS_DB_DROPPED           => 256;
use constant SERVER_STATUS_NO_BACKSLASH_ESCAPES => 512;

use constant COM_SLEEP                  => 0;
use constant COM_QUIT                   => 1;
use constant COM_INIT_DB                => 2;
use constant COM_QUERY                  => 3;
use constant COM_FIELD_LIST             => 4;
use constant COM_CREATE_DB              => 5;
use constant COM_DROP_DB                => 6;
use constant COM_REFRESH                => 7;
use constant COM_SHUTDOWN               => 8;
use constant COM_STATISTICS             => 9;
use constant COM_PROCESS_INFO           => 10;
use constant COM_CONNECT                => 11;
use constant COM_PROCESS_KILL           => 12;
use constant COM_DEBUG                  => 13;
use constant COM_PING                   => 14;
use constant COM_TIME                   => 15;
use constant COM_DELAYED_INSERT         => 16;
use constant COM_CHANGE_USER            => 17;
use constant COM_BINLOG_DUMP            => 18;
use constant COM_TABLE_DUMP             => 19;
use constant COM_CONNECT_OUT            => 20;
use constant COM_REGISTER_SLAVE         => 21;
use constant COM_STMT_PREPARE           => 22;
use constant COM_STMT_EXECUTE           => 23;
use constant COM_STMT_SEND_LONG_DATA    => 24;
use constant COM_STMT_CLOSE             => 25;
use constant COM_STMT_RESET             => 26;
use constant COM_SET_OPTION             => 27;
use constant COM_STMT_FETCH             => 28;
use constant COM_END                    => 29;

use constant MYSQL_TYPE_DECIMAL     => 0;
use constant MYSQL_TYPE_TINY        => 1;
use constant MYSQL_TYPE_SHORT       => 2;
use constant MYSQL_TYPE_LONG        => 3;
use constant MYSQL_TYPE_FLOAT       => 4;
use constant MYSQL_TYPE_DOUBLE      => 5;
use constant MYSQL_TYPE_NULL        => 6;
use constant MYSQL_TYPE_TIMESTAMP   => 7;
use constant MYSQL_TYPE_LONGLONG    => 8;
use constant MYSQL_TYPE_INT24       => 9;
use constant MYSQL_TYPE_DATE        => 10;
use constant MYSQL_TYPE_TIME        => 11;
use constant MYSQL_TYPE_DATETIME    => 12;
use constant MYSQL_TYPE_YEAR        => 13;
use constant MYSQL_TYPE_NEWDATE     => 14;
use constant MYSQL_TYPE_VARCHAR     => 15;
use constant MYSQL_TYPE_BIT         => 16;
use constant MYSQL_TYPE_NEWDECIMAL  => 246;
use constant MYSQL_TYPE_ENUM        => 247;
use constant MYSQL_TYPE_SET         => 248;
use constant MYSQL_TYPE_TINY_BLOB   => 249;
use constant MYSQL_TYPE_MEDIUM_BLOB => 250;
use constant MYSQL_TYPE_LONG_BLOB   => 251;
use constant MYSQL_TYPE_BLOB        => 252;
use constant MYSQL_TYPE_VAR_STRING  => 253;
use constant MYSQL_TYPE_STRING      => 254;
use constant MYSQL_TYPE_GEOMETRY    => 255;

use constant NOT_NULL_FLAG          => 1;
use constant PRI_KEY_FLAG           => 2;
use constant UNIQUE_KEY_FLAG        => 4;
use constant MULTIPLE_KEY_FLAG      => 8;
use constant BLOB_FLAG              => 16;
use constant UNSIGNED_FLAG          => 32;
use constant ZEROFILL_FLAG          => 64;
use constant BINARY_FLAG            => 128;
use constant ENUM_FLAG              => 256;
use constant AUTO_INCREMENT_FLAG    => 512;
use constant TIMESTAMP_FLAG         => 1024;
use constant SET_FLAG               => 2048;
use constant NO_DEFAULT_VALUE_FLAG  => 4096;
use constant NUM_FLAG               => 32768;

use constant P_BYTE => 1;    # byte
use constant P_SHORT => 2;   # short (2 bytes)
use constant P_LONG => 3;    # long (4 bytes)
use constant P_NULLSTR => 4; # null terminated string
use constant P_RAW => 5;     # raw byte string
use constant P_LCSTR => 6;   # length coded string
use constant P_FILL23 => 7;  # this one is kinda lame
use constant P_LCBIN => 8;   # length coded binary

@ISA = qw( Exporter );

# yes, this is pretty lame to export this all by default, but more than likely
# the modules that use this class are going to need it... so enjoy
@EXPORT = qw(
    CLIENT_LONG_PASSWORD
    CLIENT_FOUND_ROWS
    CLIENT_LONG_FLAG
    CLIENT_CONNECT_WITH_DB
    CLIENT_NO_SCHEMA
    CLIENT_COMPRESS
    CLIENT_ODBC
    CLIENT_LOCAL_FILES
    CLIENT_IGNORE_SPACE
    CLIENT_PROTOCOL_41
    CLIENT_INTERACTIVE
    CLIENT_SSL
    CLIENT_IGNORE_SIGPIPE
    CLIENT_TRANSACTIONS
    CLIENT_RESERVED
    CLIENT_SECURE_CONNECTION
    CLIENT_MULTI_STATEMENTS
    CLIENT_MULTI_RESULTS
    CLIENT_SSL_VERIFY_SERVER_CERT
    CLIENT_REMEMBER_OPTIONS

    SERVER_STATUS_IN_TRANS
    SERVER_STATUS_AUTOCOMMIT
    SERVER_MORE_RESULTS_EXISTS
    SERVER_QUERY_NO_GOOD_INDEX_USED
    SERVER_QUERY_NO_INDEX_USED
    SERVER_STATUS_CURSOR_EXISTS
    SERVER_STATUS_LAST_ROW_SENT
    SERVER_STATUS_DB_DROPPED
    SERVER_STATUS_NO_BACKSLASH_ESCAPES

    COM_SLEEP
    COM_QUIT
    COM_INIT_DB
    COM_QUERY
    COM_FIELD_LIST
    COM_CREATE_DB
    COM_DROP_DB
    COM_REFRESH
    COM_SHUTDOWN
    COM_STATISTICS
    COM_PROCESS_INFO
    COM_CONNECT
    COM_PROCESS_KILL
    COM_DEBUG
    COM_PING
    COM_TIME
    COM_DELAYED_INSERT
    COM_CHANGE_USER
    COM_BINLOG_DUMP
    COM_TABLE_DUMP
    COM_CONNECT_OUT
    COM_REGISTER_SLAVE
    COM_STMT_PREPARE
    COM_STMT_EXECUTE
    COM_STMT_SEND_LONG_DATA
    COM_STMT_CLOSE
    COM_STMT_RESET
    COM_SET_OPTION
    COM_STMT_FETCH
    COM_END

    MYSQL_TYPE_DECIMAL
    MYSQL_TYPE_TINY
    MYSQL_TYPE_SHORT
    MYSQL_TYPE_LONG
    MYSQL_TYPE_FLOAT
    MYSQL_TYPE_DOUBLE
    MYSQL_TYPE_NULL
    MYSQL_TYPE_TIMESTAMP
    MYSQL_TYPE_LONGLONG
    MYSQL_TYPE_INT24
    MYSQL_TYPE_DATE
    MYSQL_TYPE_TIME
    MYSQL_TYPE_DATETIME
    MYSQL_TYPE_YEAR
    MYSQL_TYPE_NEWDATE
    MYSQL_TYPE_VARCHAR
    MYSQL_TYPE_BIT
    MYSQL_TYPE_NEWDECIMAL
    MYSQL_TYPE_ENUM
    MYSQL_TYPE_SET
    MYSQL_TYPE_TINY_BLOB
    MYSQL_TYPE_MEDIUM_BLOB
    MYSQL_TYPE_LONG_BLOB
    MYSQL_TYPE_BLOB
    MYSQL_TYPE_VAR_STRING
    MYSQL_TYPE_STRING
    MYSQL_TYPE_GEOMETRY

    NOT_NULL_FLAG
    PRI_KEY_FLAG
    UNIQUE_KEY_FLAG
    MULTIPLE_KEY_FLAG
    BLOB_FLAG
    UNSIGNED_FLAG
    ZEROFILL_FLAG
    BINARY_FLAG
    ENUM_FLAG
    AUTO_INCREMENT_FLAG
    TIMESTAMP_FLAG
    SET_FLAG
    NO_DEFAULT_VALUE_FLAG
    NUM_FLAG

    P_BYTE
    P_SHORT
    P_LONG
    P_NULLSTR
    P_RAW
    P_LCBIN
    P_FILL23
    P_LCSTR
);

1;