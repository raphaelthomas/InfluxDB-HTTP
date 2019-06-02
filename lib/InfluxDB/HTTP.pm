package InfluxDB::HTTP;

use strict;
use utf8;
use warnings;

require Exporter;

our @ISA = qw(Exporter);

my %constants;
BEGIN { # http://www.perlmonks.org/?node_id=1072731
    %constants = (
        PRECISION_NANOSECONDS => 'ns',
        PRECISION_MICROSECONDS_U => "µ",
        PRECISION_MICROSECONDS => 'u',
        PRECISION_MILLISECONDS => 'ms',
        PRECISION_SECONDS => 's',
        PRECISION_MINUTES => 'm',
        PRECISION_HOURS => 'h',
        PRECISION_RFC3339 => "rfc3339",

        ENDPOINT_QUERY => "query",
        ENDPOINT_WRITE => "write",
        ENDPOINT_PING => "ping"
    );
} # end BEGIN
use constant \%constants;
our @EXPORT_OK = (keys(%constants), parseResults, toValidQueryPrecision, validatePrecision);
our @EXPORT    = ();

use Attribute::Handlers;
use Clone 'clone';
use InfluxDB::LineProtocol qw();
use List::MoreUtils qw();
use JSON::MaybeXS;
use LWP::Protocol::http;
use LWP::Protocol::https;
use LWP::UserAgent;
use Object::Result;
use Sub::Delete;
use Switch;
use URI;

our $VERSION = '0.04';

sub new {
    my $class = shift;
    my %args = (
        host => 'localhost',
        port => 8086,
        username=>undef,
        password=>undef,
        ssl=>1,
        ssl_verify=>0,
        ssl_opts=>{},
        timeout => 180,
        _prev_write_precision=>undef, # no va a cargar 'InfluxDB::LineProtocol::data2line' tan frecuentemente
        @_,
    );
    my ($host, $port, $username, $password, $ssl, $ssl_verify, $ssl_opts, $timeout) = @args{'host', 'port', "username", "password", "ssl", "ssl_verify", "ssl_opts", 'timeout'};

    my $self = {
        host => $host,
        port => $port,
        username=>$username,
        password=>$password,
        ssl=>$ssl,
        ssl_verify=>$ssl_verify,
        ssl_opts=>$ssl_opts
    };

    my $ua= LWP::UserAgent->new();
    $ua->agent("InfluxDB-HTTP/$VERSION");
    $ua->timeout($timeout);
    if ($ssl) {
        my $opts0 = {};
        if (!$ssl_verify) {
            $opts0 = {verify_hostname=>0 ,SSL_verify_mode=>0x00}; # https://stackoverflow.com/a/338550
        } # fin if
        my $opts = ({%$opts0, %$ssl_opts}, );
        $self->{ssl_opts} = $opts;
        $ua->ssl_opts(%$opts);
    } # fin if
    $self->{lwp_user_agent} = $ua;

    bless $self, $class;

    return $self;
}

sub get_lwp_useragent {
    my ($self) = @_;
    return $self->{lwp_user_agent};
}

sub ping {
    my ($self) = @_;
    my $uri = $self->_get_influxdb_http_api_uri(ENDPOINT_PING);
    $uri->query_form(
        ($self->{username} ? ('u'=>$self->{username}) : ()),
        ($self->{password} ? ('p'=>$self->{password}) : ())
    );
    my $response = $self->get_lwp_useragent()->head($uri->canonical());

    if (! $response->is_success()) {
        my $error = $response->message();
        result {
                raw    { return $response; }
                error  { return $error; }
                <STR>  { return "Error pinging InfluxDB: $error"; }
                <BOOL> { return; }
        };
    }

    my $version = $response->header('X-Influxdb-Version');
    result {
            raw     { return $response; }
            version { return $version; }
            <STR>   { return "Ping successful: InfluxDB version $version"; }
            <BOOL>  { return 1; }
    };
}

sub query {
    my $self = shift;
    my $query = shift;
    my $argEpoch = "epoch";
    my %args = ($argEpoch => PRECISION_RFC3339, @_);
    my ($database, $chunk_size, $epoch) = @args{'database', 'chunk_size', $argEpoch};

    die("Missing 1st argument 'query'.") if (!$query);
    die(sprintf("Argument ${epoch} '%s' is not one of (%s, %s, %s, %s, %s, %s, %s, %s).", $epoch, PRECISION_HOURS, PRECISION_MINUTES, PRECISION_SECONDS, PRECISION_MILLISECONDS, PRECISION_MICROSECONDS, PRECISION_MICROSECONDS_U, PRECISION_NANOSECONDS, PRECISION_RFC3339)) if (!&validatePrecision($epoch));

    if (ref($query) eq 'ARRAY') {
        $query = join(';', @$query);
    }

    my $uri = $self->_get_influxdb_http_api_uri(ENDPOINT_QUERY);
    my $param_epoch = &toValidQueryPrecision($epoch, ENDPOINT_QUERY);
    $uri->query_form(
        q => $query,
        ($database ? (db => $database) : ()),
        ($self->{username} ? ('u'=>$self->{username}) : ()),
        ($self->{password} ? ('p'=>$self->{password}) : ()),
        ($chunk_size ? (chunk_size => $chunk_size) : ()),
        ($param_epoch ? (epoch => $param_epoch) : ())
    );

    my $response = $self->get_lwp_useragent()->post($uri->canonical());

    chomp(my $content = $response->content());

    my $error;
    if ($response->is_success()) {
        local $@;
        my $data = eval { decode_json($content) };
        $error = $@;

        if ($data) {
            $error = $data->{error};
        }

        if (!$error) {
            result {
                raw         { return $response; }
                data        { return $data; }
                results     { return $data->{results}; }
                request_id  { return $response->header('Request-Id'); }
                <STR>       { return "Returned data: $content"; }
                <BOOL>      { return 1; }
            };
        }
    }
    else {
        $error = $content;
    }

    result {
        raw    { return $response; }
        error  { return $error; }
        <STR>  { return "Error executing query: $error"; }
        <BOOL> { return; }
    };
}

sub write {
    my $self = shift;
    my $points = shift;
    my $argDatabase = 'database';
    my $argMeasurement = "measurement";
    my $argPrecision = 'precision';
    my $argRetentionPolicy = "retention_policy";
    my $argTagCols = "tag_fields";
    my %args = (
        $argDatabase=>undef,
        $argMeasurement=>undef,
        $argPrecision=>PRECISION_NANOSECONDS,
        $argRetentionPolicy=>undef,
        $argTagCols=>undef,
        @_
    );
    my ($database, $measurement, $precision, $retention_policy, $tagCols) = @args{$argDatabase, $argMeasurement, $argPrecision, $argRetentionPolicy, $argTagCols};

    die("Missing argument 1st argument (points|line protocols).") if (!$points);
    die("Missing argument '${argDatabase}'.") if (!$database);
    die(sprintf("Argument ${argPrecision} '%s' is not one of (%s, %s, %s, %s, %s, %s, %s, %s).", $precision, PRECISION_HOURS, PRECISION_MINUTES, PRECISION_SECONDS, PRECISION_MILLISECONDS, PRECISION_MICROSECONDS, PRECISION_MICROSECONDS_U, PRECISION_NANOSECONDS, PRECISION_RFC3339)) if (defined($precision) && !&validatePrecision($precision));

    my $strLineProtocols = $points;
    my $param_precision = &toValidQueryPrecision($precision, ENDPOINT_WRITE);
    if (ref($points) eq 'ARRAY') {
        if (!defined($self->{_prev_write_precision}) || $self->{_prev_write_precision} ne $precision) {
            delete_sub "get_ts" if (exists(&get_ts));
            $self->{_prev_write_precision} = $precision;
            InfluxDB::LineProtocol->import(("data2line", "precision=".($param_precision eq PRECISION_MICROSECONDS ? "us" : $param_precision)));
        } # fin if
        $strLineProtocols = join("\n", (map {
            my $isString = ($_ & ~$_); # https://www.perlmonks.org/?node_id=791677
            if (!$isString && !defined($measurement)) { die("Parámetro «${argMeasurement}» no encontrado."); } # fin if
            my $copied = $_;
            my $tags = {};
            if (defined($tagCols) && ref($tagCols) eq 'ARRAY') {
                $copied = clone($_);
                my @aTagCols = @$tagCols;
                for my $i (0 .. $#aTagCols) {
                    if (!defined($copied->{$aTagCols[$i]})) { next; } # fin if
                    $tags->{$aTagCols[$i]} = $copied->{$aTagCols[$i]};
                    delete $copied->{$aTagCols[$i]};
                } # fin for
            } # fin if
            ($isString ? $_ : data2line($measurement, $copied, $tags));
        } @$points));
    }

    my $uri = $self->_get_influxdb_http_api_uri(ENDPOINT_WRITE);

    $uri->query_form(
        db => $database,
        ($self->{username} ? ('u'=>$self->{username}) : ()),
        ($self->{password} ? ('p'=>$self->{password}) : ()),
        ($param_precision ? (precision => $param_precision) : ()),
        ($retention_policy ? (rp => $retention_policy) : ())
    );

    my $response = $self->get_lwp_useragent()->post($uri->canonical(), Content => $strLineProtocols);

    chomp(my $content = $response->content());

    if ($response->code() != 204) {
        local $@;
        my $data = eval { decode_json($content) };
        my $error = $@;
        $error = $data->{error} if (!$error && $data);

        result {
            raw    { return $response; }
            error  { return $error; }
            <STR>  { return "Error executing write: $error"; }
            <BOOL> { return; }
        }
    }

    result {
        raw    { return $response; }
        <STR>  { return "Write successful"; }
        <BOOL> { return 1; }
    }
}

sub _get_influxdb_http_api_uri {
    my ($self, $endpoint) = @_;

    die "Missing argument 'endpoint'" if !$endpoint;

    my $uri = URI->new();

    $uri->scheme(($self->{ssl} ? LWP::Protocol::https::socket_type() : LWP::Protocol::http::socket_type()));
    $uri->host($self->{host});
    $uri->port($self->{port});
    $uri->path($endpoint);

    return $uri;
}

sub UNIVERSAL::MétodoEstático :ATTR(CODE) { # https://metacpan.org/release/Attribute-Static/source/lib/Attribute/Static.pm
    my ($package, $symbol, $referent, $attr, $data, $phase) = @_;
    my $meth = *{$symbol}{NAME};
    no warnings 'redefine';
    *{$symbol} = sub {
        my $check_invocant = sub { # https://stackoverflow.com/a/35283547
            my $thing = shift;
            my $caller = caller();
            return (!defined($thing) || !ref($thing) || !blessed($thing) || !$thing->isa($caller) ? 0 : $thing);
        };
        if ($check_invocant->($_[0])) { shift @_; } # fin if
        goto &$referent;
    };
} # fin UNIVERSAL::MétodoEstático()

sub validatePrecision :MétodoEstático {
    my ($strPrecision) = @_;
    if (!defined($strPrecision)) { return 0; } # fin if
    my $pattern = sprintf("^(%s|%s|%s|%s|%s|%s|%s|%s)\$", PRECISION_HOURS, PRECISION_MINUTES, PRECISION_SECONDS, PRECISION_MILLISECONDS, PRECISION_MICROSECONDS, PRECISION_MICROSECONDS_U, PRECISION_NANOSECONDS, PRECISION_RFC3339);
    return ($strPrecision !~ /$pattern/ ? 0 : 1);
} # fin validatePrecision()

sub toValidQueryPrecision :MétodoEstático {
    my ($strPrecision, $endpoint) = @_;
    $endpoint //= ENDPOINT_QUERY;
    switch ($endpoint) {
        case ENDPOINT_QUERY {
            switch ($strPrecision) {
                case PRECISION_RFC3339 { return undef; } # fin case
                case PRECISION_MICROSECONDS_U { return PRECISION_MICROSECONDS; } # fin case
            } # fin switch
        } # fin case
        case ENDPOINT_WRITE {
            switch ($strPrecision) {
                case PRECISION_RFC3339 { return PRECISION_NANOSECONDS; } # fin case
                case PRECISION_MICROSECONDS_U { return PRECISION_MICROSECONDS; } # fin case
            } # fin switch
        } # fin case
    } # fin switch
    return $strPrecision;
} # fin toValidQueryPrecision()

sub parseResults :MétodoEstático { # https://github.com/ajgb/anyevent-influxdb/blob/master/lib/AnyEvent/InfluxDB.pm#L591
    my ($data) = @_;
    return [
        map {
            my $res = $_;
            my $cols = $res->{columns};
            my $values = $res->{values};
            +{
                name => $res->{name},
                values => [
                    map {
                        +{
                            List::MoreUtils::zip(@$cols, @$_)
                        };
                    } @{$values || []}
                ]
            };
        } @{$data->[0]->{series} || []}
    ];
} # fin parseResults()

1;

__END__

=head1 NAME

InfluxDB::HTTP - The Perl way to interact with InfluxDB!

=head1 VERSION

Version 0.04

=head1 SYNOPSIS

InfluxDB::HTTP allows you to interact with the InfluxDB HTTP API. The module essentially provides
one method per InfluxDB HTTP API endpoint, that is C<ping>, C<write> and C<query>.

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

=head2 RETURN VALUES AND ERROR HANDLING

C<Object::Result> is relied upon for returning data from subroutines. The respective result
object can always be used as string and evaluated on a boolean basis. A result object
evaluating to false indicates an error and a corresponding error message is provided in the
attribute C<error>:

    my $ping = $influx->ping();
    print $ping->error unless ($ping);

Furthermore, all result objects provide access to the C<HTTP::Response> object that is returned
by InfluxDB in the attribute C<raw>.

=head2 new(host=>'localhost', port=>8086, username=>'zutana', password=>'se¢retØ', ssl=>1, ssl_verify=>0, ssl_opts=>{}, timeout=>600)

Passing C<host>, C<port>, C<username>, C<password>, C<ssl>, C<ssl_verify>, C<ssl_opts>, and/or 
C<timeout> is optional, defaulting to the InfluxDB defaults or to 3 minutes for the timeout. 
The timeout is in seconds.

Returns an instance of InfluxDB::HTTP.

=head2 ping()

Pings the InfluxDB instance configured in the constructor (i.e. by C<host> and C<port>).

Returned object evaluates to true or false depending on whether the ping was successful or not.
If true, then it contains a C<version> attribute that indicates the InfluxDB version running on
the pinged server.

The C<version> attribute is extracted from the C<X-Influxdb-Version> HTTP response header, which
is part of the HTTP response from the pinged InfluxDB instance.

    my $ping = $influx->ping();
    print $ping->version if ($ping);

=head2 query(query, database=>"DATABASE", chunk_size=>CHUNK_SIZE, epoch=>"ns")

Used to query the InfluxDB instance. All parameters but the first one are optional. The
C<query> parameter can either be a String or a Perl ArrayRef of Strings, where every String
contains a valid InfluxDB query.

If the returned object evaluates to true, indicating that the query was successful, then
the returned object's C<data> attribute contains the entire response from InfluxDB as Perl
hash. Additionally the attribute C<request_id> provides the request identifier as set in
the HTTP reponse headers by InfluxDB. This can for example be useful for correlating
requests with log files.

=head2 write(data, database=>"DATABASE", precision=>"ns", retention_policy=>"RP", measurement=>"foo", tag_fields=>("tag1", ...))

Writes data into InfluxDB. The 1ˢᵗ parameter C<data> can either be a String or an
ArrayRef of Strings or Hashes, where each String contains one valid InfluxDB LineProtocol
statement and each Hash represents a point. All of those mesaurements are then sent to 
InfluxDB and the specified database. The returned object evaluates to true if the 
write was successful, and otherwise to false.

The optional argument "C<precision>" can be given if a precsion different than "ns" is used in
the line protocol. InfluxDB docs suggest that using a coarser precision than ns can save
space and processing. In many cases "s" or "m" might do.

The optional argument "C<retention_policy>" can be used to specify a retention policy other than
the default retention policy of the selected database.

The optional argument "C<measurement>" is required if the 1ˢᵗ parameter C<data> is an ArrayRef 
and contains elements other than Strings.

The optional argument "C<tag_fields>" specifies which field(s) will be stored as tag(s).

=head2 get_lwp_useragent()

Returns the internally used LWP::UserAgent instance for possible modifications
(e.g. to configure an HTTP proxy).

=head1 AUTHOR

Raphael Seebacher, C<< <raphael@seebachers.ch> >>, modificada por sam80180 C<< <https://github.com/sam80180/InfluxDB-HTTP> >>

=head1 BUGS

Please report any bugs or feature requests to
L<https://github.com/raphaelthomas/InfluxDB-HTTP/issues>.

=head1 LICENSE AND COPYRIGHT

MIT License

Copyright (c) 2016 Raphael Seebacher

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.

=cut
