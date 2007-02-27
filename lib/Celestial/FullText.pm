package Celestial::FullText;

use strict;
use warnings;

use Carp;
use URI;
use HTTP::OAI;
use HTTP::OAI::Metadata::OAI_DC;
use HTTP::OAI::Metadata::METS;

use vars qw( $MAX_FILE_SIZE );

our $VERSION = '0.01';

our %SERVER_TYPES = (
	eprints => "GNU EPrints",
	dspace => "DSpace",
	mets => "METS",
);

# Preloaded methods go here.

sub new
{
	my( $class, $ha, %opts ) = @_;
	unless( $opts{ identifier } || $opts{ record } )
	{
		croak("Requires identifier or record arguments");
	}
	unless( $opts{ record } )
	{
		my $r = $ha->GetRecord(
			identifier => $opts{ identifier },
			metadataPrefix => 'oai_dc',
			handlers => { metadata => 'HTTP::OAI::Metadata::OAI_DC' }
		);
		$opts{ record } = $r->next;
		unless( $r->is_success )
		{
			warn "Error getting record ($opts{identifier}): " . $r->message;
			return;
		}
	}
	unless( $opts{ record } )
	{
		warn("Failed to get oai_dc record for $opts{identifier}");
		return;
	}
	unless( $opts{ record }->metadata )
	{
		warn("Record doesn't contain metadata (".$opts{record}->identifier."): $opts{record}");
		return;
	}
	bless {%opts, ha => $ha}, $class;
}

sub formats
{
	my $self = shift;
	my $st = $self->server_type or return ();

	my $f = "_$st";
	no strict "refs";
	return $self->$f();
}

sub server_type
{
	my $self = shift;
	return $self->{ server_type } if $self->{ server_type };
	my $ha = $self->{ ha };
	my $rec = $self->{ record };

	if( $rec->metadata->isa( 'HTTP::OAI::Metadata::METS' ))
	{
		return $self->{ server_type } = "mets";
	}

	my $ids = $rec->metadata->dc->{ identifier };

	{
		my $uri = URI->new($ha->baseURL);
		if( $uri->path eq '/perl/oai2' )
		{
			return $self->{ server_type } = "eprints";
		}
	}

	foreach my $url (grep { /^https?:/ } @$ids)
	{
		my $r = $ha->head( $url );
		unless( $r->is_success ) {
			warn sprintf("Error requesting [%s]: %s\n",
				$url,
				$r->message,
			);
			return;
		}
		next unless $r->content_type eq 'text/html' or $r->content_type eq 'text/xml';
		$r = $ha->get( $url );
		unless( $r->is_success ) {
			warn sprintf("Error requesting [%s]: %s\n",
				$url,
				$r->message,
			);
			return;
		}

		my $ct = $r->content;
		if( $ct =~ /\"metadataFieldLabel\"/ and $ct =~ /\"metadataFieldValue\"/ )
		{
			$self->{ jumpoff } = $url;
			return $self->{ server_type } = "dspace";
		}
	}

	return;
}

sub _dspace
{
	my $self = shift;
	my $ha = $self->{ ha };
	my $rec = $self->{ record };
	my $jo = $self->{ jumpoff };
	my @fmts;
	unless( $self->{ jumpoff } )
	{
		($jo) = @{$rec->metadata->dc->{ identifier }};
	}
	$jo = $ha->get( $jo );
	unless( $jo->is_success ) {
		warn sprintf("Error requesting [%s]: %s\n",
			$jo->request->uri,
			$jo->message,
		);
		return ();
	}
	my $ct = $jo->content;
	my $bu = $jo->request->uri;
	$bu->path('');

	while( $ct =~ m/\"([^\"]+?bitstream[^\"]+?)\"/sg )
	{
		my $uri = URI->new_abs( $1, $bu );
		if( my $fmt = Celestial::FullText::Format->new( $ha, $uri ) ) {
			push @fmts, $fmt;
		} else {
			warn sprintf("Error requesting [%s], ignoring!\n", $uri);
		}
	}

	return @fmts;
}

sub _eprints
{
	my $self = shift;
	my $ha = $self->{ ha };
	my $rec = $self->{ record };
	my @fmts;
	my @urls;
	push @urls, @{$rec->metadata->dc->{ identifier }};
	push @urls, @{$rec->metadata->dc->{ format }};
	push @urls, @{$rec->metadata->dc->{ relation }};
	my $bu = URI->new($ha->baseURL)->canonical;
	$bu->path('');
	for(@urls)
	{
		my ($fmt,$url) = split / /, $_;
		$_ = $url ? $url : $fmt;
		next unless $_ =~ /^https?:/;
		my $u = URI->new($_);
		$_ = '' unless $u->host eq $bu->host and $u->path =~ m#^/\d+/\d+/|/archive/\d+/\d+/#;
	}
	@urls = grep { /^https?:/ } @urls;
	return grep { defined $_ } map { Celestial::FullText::Format->new( $ha, $_ ) } @urls;
}

sub _mets
{
	my $self = shift;
	my $ha = $self->{ ha };
	my $rec = $self->{ record };

	my $bu = URI->new($ha->baseURL)->canonical;
	$bu->path('');

	my @urls;
	for($rec->metadata->files)
	{
warn "Got url: " . $_->{ url };
		push @urls, $_->{ url } if $_->{ url };
	}

	return grep { defined $_ } map { Celestial::FullText::Format->new( $ha, $_ ) } @urls;
}

package Celestial::FullText::Format;

use overload '""' => \&to_string;
use File::Temp;
use MIME::Types;

our $TMPFILE;
our $TMPFILE_SIZE = 0;

sub new
{
	my( $class, $ua, $url ) = @_;
	my $r = $ua->head( $url );
	unless( $r->is_success ) {
		warn "Error requesting [$url]: " . $r->message . "\n";
		return undef;
	}
	my $mt = MIME::Types->new->type( $r->header( 'Content-Type' )) || $r->header( 'Content-Type' );
	my $date = $r->headers->header( 'Last-Modified' );
	my $cl = $r->headers->header("Content-Length");
	return bless {
		ha => $ua,
		url => $url,
		date => $date,
		mt => $mt,
		size => $cl,
	}, ref($class) || $class;
}

sub _get
{
	my $self = shift;
	my $ext = '';
	if( $self->url =~ m#[^/](\.\w{2,5})$# )
	{
		$ext = $1;
	}
	elsif( $self->mime_type and ref($self->mime_type) )
	{
		my @exts = $self->mime_type->extensions;
		$ext = ".".$exts[0] if @exts;
	}
	$self->{ fh } = $TMPFILE = File::Temp->new( CLEANUP => 1, SUFFIX => $ext );
	binmode($TMPFILE);
	$TMPFILE_SIZE = 0;
	return $self->{ ha }->get( $self->url, ':content_cb' => \&Celestial::FullText::Format::_lwpcallback );
}

sub _lwpcallback
{
	my( $data, $r, $proto ) = @_;
	if( !defined(syswrite($TMPFILE, $data)) ) {
		die $!;
	}
	$TMPFILE_SIZE += length($data);
	if( defined($Celestial::FullText::MAX_FILE_SIZE) and
		$TMPFILE_SIZE > $Celestial::FullText::MAX_FILE_SIZE )
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
		my $r = self->_get();
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
# Below is stub documentation for your module. You'd better edit it!

=head1 NAME

Celestial::FullText - Retrieve the full-text for an OAI record

=head1 SYNOPSIS

  use Celestial::FullText;

  my $ft = Celestial::FullText->new( $ha, identifier => $id );
  my $ft = Celestial::FullText->new( $ha, record => $rec );

	my $ft = Celestial::FullText->new(
		$ha,
		server_type => 'dspace',
		record => $rec
	);

  unless( $ft )
  {
  	warn "Unsupported repository type";
  }

  print $ft->server_type;

  foreach my $file ($ft->formats)
  {
		my $mt = $file->mime_type;
		my @ext = $mt->extensions;
		my $url = $file->url;
		print -s $file;
  }

=head1 DESCRIPTION

Stub documentation for Celestial::FullText, created by h2xs. It looks like the
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

Timothy D Brody, E<lt>tdb01r@localdomainE<gt>

=head1 COPYRIGHT AND LICENSE

Copyright (C) 2006 by Timothy D Brody

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself, either Perl version 5.8.5 or,
at your option, any later version of Perl 5 you may have available.


=cut
