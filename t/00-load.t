#!perl -T
use 5.006;
use strict;
use warnings;
use Test::More;

plan tests => 3;

BEGIN {
    use_ok( 'InfluxDB::HTTP' ) || print "Bail out!\n";
    use_ok( 'InfluxDB::HTTP::Query' ) || print "Bail out!\n";
    use_ok( 'InfluxDB::HTTP::Write' ) || print "Bail out!\n";
}

diag( "Testing InfluxDB::HTTP $InfluxDB::HTTP::VERSION, Perl $], $^X" );
