#!/usr/bin/perl
#
# Proximo plugin to do profiling of queries.
#
# At the moment, requires MySQL.
#
# Copyright 2008 by Mark Smith.
#

package Proximo::Plugin::Profiler;

sub register {
    Proximo->add_hook( 'something', sub {
       die "called\n"; 
    });
}

1;