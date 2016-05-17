# FIXME authentication support

package InfluxDB::HTTP;

use strict;
use warnings;

require Exporter;

our @ISA = qw(Exporter);

our @EXPORT_OK = ();
our @EXPORT    = ();

use JSON::XS;
use LWP::UserAgent;
use Method::Signatures;
use Object::Result;


our $VERSION = '0.01';


method new ($class: Str :$host = 'localhost', Int :$port = 8086) {
    my $self = {
        host => $host,
        port => $port,
    };

    $self->{lwp_user_agent} = LWP::UserAgent->new();
    $self->{lwp_user_agent}->agent("InfluxDB-HTTP/$VERSION");

    bless $self, $class;

    return $self;
}

method get_lwp_useragent {
    return $self->{lwp_user_agent};
}

method ping {
    my $response = $self->{lwp_user_agent}->head('http://'.$self->{host}.':'.$self->{port}.'/ping');

    if (! $response->is_success()) {
        my $error = $response->message();
        result {
                error  { return $error; }
                <STR>  { return "Error pinging InfluxDB: $error"; }
                <BOOL> { return; }
        }
    }

    my $version = $response->header('X-Influxdb-Version');
    result {
            version { return $version; }
            <STR>   { return "Ping successful: InfluxDB version $version"; }
            <BOOL>  { return 1; }
    }
}

method query (Str|ArrayRef[Str] $query!, Str :$database = '', Int :$chunk_size, Str :$epoch where qr/(h|m|s|ms|u|ns)/) {

    # support chunk_size
    # support query being an arrayref

    my $response = $self->{lwp_user_agent}->post('http://'.$self->{host}.':'.$self->{port}."/query?q=$query&db=$database&epoch=$epoch");

    if (($response->code() != 200) || ($response->header('Content-Type') ne 'application/json')) {
        my $error = $response->message();
        result {
            error  { return $error; }
            <STR>  { return "Error executing query: $error"; }
            <BOOL> { return; }
        }
    }

    my $data = decode_json($response->content);

    result {
        data        { return $data; }
        results     { return $data->{results}; }
        request_id  { return $response->header('Request-Id'); }
        <BOOL>      { return 1; }
    }
}

method write {

}


1;


__END__

=head1 NAME

InfluxDB::HTTP - Perl extension for blah blah blah

=head1 SYNOPSIS

  use InfluxDB::HTTP;
  blah blah blah

=head1 DESCRIPTION

Stub documentation for InfluxDB::HTTP, created by h2xs. It looks like the
author of the extension was negligent enough to leave the stub
unedited.

Blah blah blah.

=head2 EXPORT

None by default.



=head1 SEE ALSO

Mention other useful documentation such as the documentation of
related modules or operating system documentation (such as man pages
in UNIX), or any relevant external documentation such as RFCs or
standards.

If you have a mailing list set up for your module, mention it here.

If you have a web site set up for your module, mention it here.

=head1 AUTHOR

Raphael Seebacher, E<lt>rse@apple.comE<gt>

=head1 COPYRIGHT AND LICENSE

Copyright (C) 2016 by Raphael Seebacher

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself, either Perl version 5.18.2 or,
at your option, any later version of Perl 5 you may have available.


=cut
