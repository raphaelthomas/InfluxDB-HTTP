package InfluxDB::HTTP;

use strict;
use warnings;

require Exporter;

our @ISA = qw(Exporter);

our @EXPORT_OK = ();
our @EXPORT    = ();

use LWP::UserAgent;
use Method::Signatures;


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

    return unless ($response->is_success());
    return $response->header('X-Influxdb-Version');
}

method query {

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
