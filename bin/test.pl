#!/usr/bin/perl

use strict;
use lib "../lib";
use Proximo;

# load a simple configuration file
Proximo->LoadConfigFile( 'conf/simple.conf' );

# enter main loop, note that this doesn't return
Proximo->Run;