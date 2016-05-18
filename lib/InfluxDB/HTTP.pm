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
use URI;


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

method query (Str|ArrayRef[Str] $query!, Str :$database, Int :$chunk_size, Str :$epoch where qr/(h|m|s|ms|u|ns)/ = 'ns') {
    if (ref($query) eq 'ARRAY') {
        $query = join(';', @$query);
    }

    my $uri = URI->new();
    $uri->scheme('http');
    $uri->host($self->{host});
    $uri->port($self->{port});
    $uri->path('query');

    my $uri_query = {'q' => $query, };
    $uri_query->{'db'} = $database if (defined $database);
    $uri_query->{'chunk_size'} = $chunk_size if (defined $chunk_size);
    $uri_query->{'epoch'} = $epoch if (defined $epoch);

    $uri->query_form($uri_query);

    my $response = $self->{lwp_user_agent}->post($uri->canonical());

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

InfluxDB::HTTP - The Perl way to interact with InfluxDB!

=head1 VERSION

Version 0.01

=head1 SYNOPSIS

Quick summary of what the module does. Perhaps a little code snippet.

    use InfluxDB::HTTP;

    my $influx = InfluxDB::HTTP->new();

    my $ping_result = $influx->ping();
    print "$ping_result\n";

    my $query = $influx->query(
        [ 'SELECT Lookups FROM _internal.monitor.runtime WHERE time > '.(time - 60)*1000000000, 'SHOW DATABASES'],
        epoch => 's',
    );

    print "$query\n";


=head1 SUBROUTINES/METHODS

=head2 ping

=head2 query

=head2 write

=head1 AUTHOR

Raphael Seebacher, C<< <raphael at seebachers.ch> >>

=head1 BUGS

Please report any bugs or feature requests to C<bug-influxdb-http at rt.cpan.org>, or through
the web interface at L<http://rt.cpan.org/NoAuth/ReportBug.html?Queue=InfluxDB-HTTP>.  I will be notified, and then you'll
automatically be notified of progress on your bug as I make changes.

=head1 SUPPORT

You can find documentation for this module with the perldoc command.

    perldoc InfluxDB::HTTP::Write


    You can also look for information at:

=over 4

=item * RT: CPAN's request tracker (report bugs here)

L<http://rt.cpan.org/NoAuth/Bugs.html?Dist=InfluxDB-HTTP>

=item * AnnoCPAN: Annotated CPAN documentation

L<http://annocpan.org/dist/InfluxDB-HTTP>

=item * CPAN Ratings

L<http://cpanratings.perl.org/d/InfluxDB-HTTP>

=item * Search CPAN

L<http://search.cpan.org/dist/InfluxDB-HTTP/>

=back


=head1 ACKNOWLEDGEMENTS


=head1 LICENSE AND COPYRIGHT

Copyright 2016 Raphael Seebacher.

This program is free software; you can redistribute it and/or modify it
under the terms of either: the GNU General Public License as published
by the Free Software Foundation; or the Artistic License.

See L<http://dev.perl.org/licenses/> for more information.

=cut
