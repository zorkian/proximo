#!/usr/bin/perl

use strict;
use lib "../lib";
use Proximo;
use Proximo::Plugin::Profiler;

# load a simple configuration file
Proximo->LoadConfigFile( 'conf/simple.conf' );
Proximo::Plugin::Profiler::register();

# enter main loop, note that this doesn't return
Proximo->Run;