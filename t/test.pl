#!/usr/bin/perl

use strict;
use warnings;

use lib 'lib/';

use Const::Fast;
use InfluxDB::HTTP;
use JSON::MaybeXS;
# use Test::More;

const my $TIME     => time();
const my $DATABASE => "test_$TIME";
const my $FIELD    => 'f1';
const my $TAGS     => 'server=zrh01';

const my $M_NAME => 'm_test';
const my $M_BASE => "$M_NAME,$TAGS";

const my $M_OK         => $M_BASE.' '.$FIELD.'=123';
const my $M_BAD_TYPE   => $M_BASE.' '.$FIELD.'="bad"';
const my $M_BAD_FORMAT => $M_BASE.$FIELD.'=bad';

const my $Q_OK => "SELECT LAST($FIELD) FROM $DATABASE.\"autogen\".$M_NAME";

sub main {
    my $influx = InfluxDB::HTTP->new();

    setup($influx);

    test_ping($influx);
    test_write($influx);
    test_query($influx);

    cleanup($influx);

    return;
}

sub setup {
    my $influx = shift;

    my $rv = $influx->query("CREATE DATABASE $DATABASE");

    die "Error setting up database $DATABASE: $rv\n" if (!$rv);

    return;
}

sub cleanup {
    my $influx = shift;

    my $rv = $influx->query("DROP DATABASE $DATABASE");

    die "Error dropping database $DATABASE: $rv\n" if (!$rv);

    return;
}

sub test_ping {
    my $influx = shift;

    my $ping = $influx->ping();
    print "$ping\n";

    die if (!$ping);
    
    return;
}

sub test_write {
    my $influx = shift;

    my $rv;

    $rv = $influx->write($M_OK, database => $DATABASE, precision => 's');
    print "$rv\n";

    $rv = $influx->write($M_BAD_FORMAT, database => $DATABASE, precision => 's');
    print "$rv\n";

    $rv = $influx->write($M_BAD_TYPE, database => $DATABASE, precision => 's');
    print "$rv\n";

    return;
}

sub test_query {
    my $influx = shift;

    my $rv;

    $rv = $influx->query($Q_OK, epoch => 's');
    print encode_json($rv->results)."\n";


    return;
}

main;
