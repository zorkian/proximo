#!/usr/bin/perl

use strict;
use lib "../lib";
use Proximo;

my $prox = Proximo->new(
        # set our configuration file ...
        config_file => 'conf/simple.conf',
    )
    or die "Unable to create Proximo object!\n";

$prox->run;
