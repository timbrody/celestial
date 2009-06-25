package Celestial::CGI;

use strict;
use warnings;

use YAML;
use vars qw( $AUTOLOAD );
use Apache2::Const qw( REDIRECT NOT_FOUND SERVER_ERROR );
use URI;
use URI::Escape qw();
use Number::Bytes::Human qw();

use vars qw( @ISA @EXPORT @EXPORT_OK );
use Exporter;
@ISA = qw( Exporter );
@EXPORT = qw( uri_escape uri_unescape );

sub new {
	my( $class, %opts ) = @_;
	$opts{phrases} = load_phrases($Celestial::Config::SETTINGS->{ languages }->{ en_GB });
	bless \%opts, $class;
}

sub AUTOLOAD
{
	$AUTOLOAD =~ s/^.*:://;
	return if $AUTOLOAD eq 'DESTROY';
	die if $AUTOLOAD =~ /^[A-Z]/;
	my $self = shift;
	unless( exists($self->{ $AUTOLOAD }) ) {
		die "Can't call method $AUTOLOAD on $self (never initialised)";
	}
	return @_ == 1 ?
		$self->{ $AUTOLOAD } = shift :
		$self->{ $AUTOLOAD };
}

sub load_phrases
{
	my $fn = shift;
	unless( -e $fn ) {
		Carp::confess( "Can't load language file [$fn]: does not exist" );
	}
	return YAML::LoadFile( $fn );
}

sub form_action {
	my $self = shift;
	return $self->as_link($self->section);
}

sub as_link {
	my( $self, @args ) = @_;
	my $section = @args % 2 == 1 ? shift @args : $self->section;
	my $url = URI->new( $self->script_path . '/' . $section, 'http' );
	$url->query_form( @args );
	return $url;
}

sub absolute_link {
	my $self = shift;
	return URI->new_abs(shift, $self->url);
}

sub msg {
	my( $self, $term, @vars ) = @_;
	my $phrase = $self->phrases->{ $term };
	return sprintf("[%s('%s')]", $term, join("','",@vars)) unless defined($phrase);
	$phrase =~ s/\$(\d+)/$vars[$1-1]||"[invalid var index $1]"/seg;
	return $phrase;
}

sub tick { chr(0x2713) }
sub cross { chr(0x2717) }
sub unknown { '?' }

sub date {
	my( $self, $ds ) = @_;
	return '?' unless $ds;
	return $self->datestamp( substr($ds,0,8) );
}

sub datestamp {
	my( $self, $ds ) = @_;
	return '?' unless $ds;
	if( $ds =~ /^(\d{4})(\d\d)(\d\d)$/ ) {
		return "$1-$2-$3";
	} elsif( $ds =~ /^(\d{4})(\d\d)(\d\d)(\d\d)(\d\d)(\d\d)$/ ) {
		return "$1-$2-$3T$4:$5:$6Z";
	} else {
		return "['$ds']";
	}
}

sub humansize {
	my( $self, $bytes ) = @_;
	return Number::Bytes::Human::format_bytes( $bytes );
}

sub uri_escape {
	shift if ref($_[0]) and $_[0]->isa('Celestial::CGI');
	return URI::Escape::uri_escape_utf8(@_);
}

sub uri_unescape {
	my $str = @_ == 1 ? shift : $_[1];
	$str =~ s/%([0-9A-Fa-f]{2})/chr(hex($1))/eg;
	utf8::decode($str); # utf8 bytes => utf8 chars
	return $str;
}

sub param {
	shift->cgi->param( @_ );
}

sub redirect {
	my $self = shift;
	my $r = $self->request;
	$r->err_headers_out->add( 'Location' => shift );
	$r->status( REDIRECT );
}

sub not_found {
	my $self = shift;
	$self->request->status( NOT_FOUND );
}

sub internal_error {
	my $self = shift;
	$self->request->status( SERVER_ERROR );
}

sub content_type {
	my $self = shift;
	$self->request->content_type( shift );
}

sub header {
	my $self = shift;
	$self->request->headers_out->{$_[0]} = $_[1];
}

sub valid_email {
	my( $self, $email ) = @_;
	return $email =~ /^[A-Za-z0-9_\.\-]+\@(?:[A-Za-z0-9_\-]+\.)+[A-Za-z]{2,4}$/;
}

sub set_cookie {
	my( $self, $name, $value, %opts ) = @_;
	$opts{ -name } ||= $name;
	$opts{ -value } ||= $value;
	$opts{ -expires} ||= 0;
	$opts{ -path } ||= $self->script_path;
	$opts{ -domain } ||= $self->hostname;

	my $cookie = CGI::cookie(%opts);

	return $self->request->err_headers_out->add('Set-Cookie' => $cookie);
}

sub get_cookie {
	my( $self, $name ) = @_;
	
	my $hdr = $self->request->headers_in->{ 'Cookie' } or return;
	my @jar = map { split /=/, $_, 2 } split /;\s*/, $hdr;
	return unless @jar % 2 == 0;
	my %cookies = @jar;

	return exists($cookies{ $name }) ?
		uri_unescape($cookies{ $name }) :
		undef;
}

1;
