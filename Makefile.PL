use 5.006;
use strict;
use warnings;
use ExtUtils::MakeMaker;

WriteMakefile(
    NAME             => 'InfluxDB::HTTP',
    AUTHOR           => q{Raphael Seebacher <raphael@seebachers.ch>},
    VERSION_FROM     => 'lib/InfluxDB/HTTP.pm',
    ABSTRACT_FROM    => 'lib/InfluxDB/HTTP.pm',
    LICENSE          => 'perl_5',
    PL_FILES         => {},
    MIN_PERL_VERSION => 5.006,
    CONFIGURE_REQUIRES => {
        'ExtUtils::MakeMaker' => 0,
    },
    BUILD_REQUIRES => {
        'Test::More' => 0,
    },
    PREREQ_PM => {
        #'ABC'              => 1.6,
        #'Foo::Bar::Module' => 5.0401,
        'Attribute::Handlers'=>0,
        'Clone'=>0,
        'InfluxDB::LineProtocol'=>0,
        'List::MoreUtils'=>0,
        'JSON::MaybeXS'=>0,
        'LWP::Protocol::http'=>0,
        'LWP::Protocol::https'=>0,
        'LWP::UserAgent'=>0,
        'Object::Result'=>0,
        'Sub::Delete'=>0,
        'Switch'=>0,
        'URI'=>0
    },
    dist  => { COMPRESS => 'gzip -9f', SUFFIX => 'gz', },
    clean => { FILES => 'InfluxDB-HTTP-*' },
);
