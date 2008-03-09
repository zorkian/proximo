#!/usr/bin/perl

use strict;
use lib "../lib";
use Proximo;

Proximo->LoadConfigFile( 'conf/simple.conf' );
Proximo->Run;