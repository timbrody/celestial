=head1 NAME

Celestial::FullText - Retrieve the full-text for an OAI record

=head1 SYNOPSIS

  use Celestial::FullText;

  my $ft = Celestial::FullText->new( $dbh, harvestAgent => $ha );

  foreach my $file ($ft->formats)
  {
		my $mt = $file->mime_type;
		my @ext = $mt->extensions;
		my $url = $file->url;
		print -s $file;
  }

=head1 DESCRIPTION

=head2 EXPORT

None by default.

=head1 METHODS

=over 4

=cut

package Celestial::FullText;

use strict;

use URI;
use HTTP::OAI;
use HTTP::OAI::Metadata::OAI_DC;
use HTTP::OAI::Metadata::METS;
use Socket qw();

use vars qw( $MAX_FILE_SIZE );

our $VERSION = '0.01';
our $DEBUG = 0;

our %SERVER_TYPES = (
	eprints => "GNU EPrints",
	dspace => "DSpace",
	mets => "METS",
);

# Preloaded methods go here.

=item $ft = Celestial::FullText->new( %options )

Create a new FullText retrieval object using RECORD to determine what the type of repository is.

=cut

sub new
{
	my( $class, %self ) = @_;

	my $self = bless \%self, $class;

	return $self;
}

=item $st = $ft->guess_repository_type()

Guess the repository type using HARVEST_AGENT and RECORD.

=cut

sub guess_repository_type
{
	my( $self ) = @_;
	
	return $self->{ server_type }
		if exists $self->{ server_type };

	my $repo = $self->{repository};
	my $ha = $self->{harvestAgent};

	{
		my $uri = URI->new($ha->baseURL);
		if( $uri->path eq '/perl/oai2' )
		{
			return $self->{ server_type } = "eprints";
		}
		$uri->path( '/cgi/oai2' );
		$uri->query( 'verb=Identify' );
		my $r = $ha->get( $uri );
		return $self->{ server_type } = "eprints" if $r->content =~ /EPrints/;
	}

	foreach my $format ($repo->formats)
	{
		if( $format->{namespace} eq 'http://www.loc.gov/METS/' )
		{
			return $self->{ server_type } = "mets";
		}
	}

	if( defined(my $host = $repo->parent) )
	{
		if( $host->is_set( "software" ) )
		{
			my $version = $host->value( "software" );
			if( $self->can( "run_$version" ) )
			{
				return $self->{ server_type } = $host->value( "software" );
			}
		}
	}

	return $self->{ server_type } = undef;
}

=item @formats = $ft->formats( RECORDID )

Return a list of formats for the given record.

=cut

sub formats
{
	my( $self, $id ) = @_;

	my $repo = $self->{repository};
	my $oai_dc = $repo->format( "oai_dc" );

	my $st = $self->guess_repository_type();
	return if !defined $st;

	my $f = "run_$st";
	return $self->$f( $id );
}

sub get_oai_dc
{
	my( $self, $id ) = @_;

	my $dataset = $self->{repository}->repository->dataset( "celestial_dc" );
	my( $dc ) = $self->{dbh}->_get( $dataset, 0, $id );

	return undef if !defined $dc;

	return $dc->get_data();
}

sub get_mets
{
	my( $self, $id ) = @_;

	my $dataset = $self->{repository}->repository->dataset( "celestial_dc" );
	my( $dc ) = $self->{dbh}->_get( $dataset, 0, $id );

	return undef if !defined $dc;

	my $identifier = $dc->value( "header" )->[0]->{identifier};
	return if !defined $identifier;

	my $prefix;
	for($self->{repository}->formats)
	{
		if( $_->{namespace} eq 'http://www.loc.gov/METS/' )
		{
			$prefix = $_->{prefix};
			last;
		}
	}
	return if !$prefix;

	my $r = $self->{harvestAgent}->GetRecord(
		metadataPrefix => $prefix,
		identifier => $identifier,
		handlers => {
			metadata => 'HTTP::OAI::Metadata::METS',
		},
	);
	return if !$r->is_success;

	return $r->next;
}

sub run_dspace
{
	my( $self, $id ) = @_;

	my $ha = $self->{harvestAgent};

	my $dc = $self->get_oai_dc( $id );
	return if !$dc;

	my @fmts;
	my( $jo_url ) = grep { /^https?:\/\// } @{$dc->{ identifier }};
	if( !defined $jo_url )
	{
		return ();
	}
warn "GET $jo_url\n" if $DEBUG;
	my $jo = $ha->get( $jo_url );
	unless( $jo->is_success ) {
		warn sprintf("Error requesting [%s]: %s\n",
			$jo_url,
			$jo->message,
		);
		return ();
	}
	my $ct = $jo->content;
	my $bu = $jo->request->uri;
	$bu->path('');

	my %urls;

	while( $ct =~ m/\"([^\"]+?bitstream[^\"]+?)\"/sg )
	{
		my $uri = URI->new_abs( $1, $bu );
		$urls{$uri} = $uri;
	}

	while(my( $u, $uri ) = each %urls )
	{
		if( my $fmt = Celestial::FullText::Format->new( $self, $uri ) ) {
			push @fmts, $fmt;
		} else {
			warn sprintf("Error requesting [%s], ignored\n", $uri);
		}
	}

	return @fmts;
}

sub run_eprints
{
	my( $self, $id ) = @_;

	my @fmts;

	my $ha = $self->{harvestAgent};

	my $dc = $self->get_oai_dc( $id );
	warn "Failed to retrieve oai_dc" if !$dc && $DEBUG;
	return if !$dc;

	my $bu = URI->new($ha->baseURL)->canonical;
	$bu->path('');

	# we only want to download files from the same host as the PMH interface,
	# otherwise we could end up retrieving publisher versions etc.
	my @addresses = gethostbyname( $bu->host );
	my %samehost = map { $_ => 1 }
		(
			split(/ /, $addresses[1]),
			map { Socket::inet_ntoa($_) } @addresses[4..$#addresses]
		);

	my @urls;
	for(
		@{$dc->{identifier}||[]},
		@{$dc->{format}||[]},
		@{$dc->{relation}||[]},
		)
	{
		my ($fmt,$url) = split / /, $_;
		$url = $fmt if !$url;
		next if $url !~ /^https?:/;
		my $u = URI->new($url);
		next if !$u->host;
		if( !exists $samehost{$u->host} )
		{
			if( my $packed_ip = gethostbyname($u->host) )
			{
				my $ip = Socket::inet_ntoa($packed_ip);
				$samehost{$u->host} = $samehost{$ip};
			}
		}
		next unless
			$samehost{$u->host} and
			$u->path =~ m#^/\d+/\d+/|/archive/\d+/\d+/#;
		push @urls, $url;
	}

	return
		grep { defined $_ }
		map { Celestial::FullText::Format->new( $self, $_ ) } @urls;
}

sub run_mets
{
	my( $self, $id ) = @_;

	my $ha = $self->{ harvestAgent };

	my $bu = URI->new($ha->baseURL)->canonical;
	$bu->path('');

	my $rec = $self->get_mets( $id );
	return if !$rec || !$rec->metadata;

	my @urls;
	for($rec->metadata->files)
	{
		push @urls, $_->{ url } if $_->{ url };
	}

	unless( @urls )
	{
		warn $rec->identifier . " - METS - didn't contain any URLs" if $DEBUG;
	}

	return
		grep { defined $_ }
		map { Celestial::FullText::Format->new( $self, $_ ) } @urls;
}

package Celestial::FullText::Format;

use overload '""' => \&to_string;
use File::Temp;
use MIME::Types;

sub new
{
	my( $class, $ft, $url ) = @_;

	my $self = bless {
		ft => $ft,
		url => $url,
		harvestAgent => $ft->{harvestAgent},
		fh => undef,
		filesize => 0,
	}, $class;

warn "HEAD $url\n" if $DEBUG;
	my $r = $self->{harvestAgent}->head( $url );
	if( !$r->is_success ) {
		warn "Error requesting [$url]: " . $r->message . "\n"
			if $DEBUG;
		return undef;
	}
	$self->{mt} = MIME::Types->new->type( $r->header( 'Content-Type' )) || $r->header( 'Content-Type' );
	$self->{date} = $r->headers->header( 'Last-Modified' );
	$self->{size} = $r->headers->header("Content-Length");

	return $self;
}

sub _get
{
	my $self = shift;
	my $ext = '';
	if( $self->url =~ m# [^/](\.\w{2,5})$ #x )
	{
		$ext = $1;
	}
	elsif( $self->mime_type and ref($self->mime_type) )
	{
		my @exts = $self->mime_type->extensions;
		$ext = ".".$exts[0] if @exts;
	}
	$self->{ fh } = File::Temp->new( CLEANUP => 1, SUFFIX => $ext );
	binmode($self->{ fh });
warn "GET ".$self->url."\n" if $DEBUG;

	return $self->{ harvestAgent }->get( $self->url,
		':content_cb' => sub { $self->_lwpcallback( @_ ) }
	);
}

sub _lwpcallback
{
	my( $self, $data, $r, $proto ) = @_;
	if( !defined(syswrite($self->{ fh }, $data)) ) {
		die $!;
	}
	$self->{filesize} += length($data);
	if( defined($Celestial::FullText::MAX_FILE_SIZE) and
		$self->{filesize} > $Celestial::FullText::MAX_FILE_SIZE )
	{
		die "toobig\n";
	}
}

sub url
{
	return shift->{ url };
}

sub date
{
	return shift->{ date };
}

sub size
{
	return shift->{ size };
}

sub to_string
{
	my $self = shift;
	$self->_get() unless $self->{ fh };
	return $self->{ fh };
}

sub mime_type
{
	return shift->{ mt };
}

sub file
{
	my $self = shift;
	unless( defined($self->{ fh }) ) {
		my $r = $self->_get();
		unless( $r->is_success ) {
			if( $r->header('X-Died') ) {
				if( $r->header('X-Died') =~ /^toobig/ ) {
					warn "GET for " . $r->request->uri . " resulted in a toobig file\n";
				} else {
					Carp::confess $r->header('X-Died');
				}
			} else {
				warn "GET for " . $r->request->uri . " failed, even though HEAD succeeded";
				return undef;
			}
		}
	}
	return $self->{ fh };
}

1;

__END__

=back

=head1 SEE ALSO

=head1 AUTHOR

Timothy D Brody, E<lt>tdb01r@localdomainE<gt>

=head1 COPYRIGHT AND LICENSE

Copyright (C) 2006 by Timothy D Brody

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself, either Perl version 5.8.5 or,
at your option, any later version of Perl 5 you may have available.

=cut
