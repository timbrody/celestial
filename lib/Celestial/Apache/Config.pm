package Celestial::Apache::Config;

use strict;
use warnings;

use Celestial::Config; # Exports $SETTINGS

use CGI qw/-oldstyle_urls/;
use URI;
use HTML::Entities;

use HTTP::Request;
use LWP::UserAgent;
use HTTP::OAI::Harvester;
use XML::LibXML;
use Celestial::DBI;

use Apache2;
use Apache::Access;
use Apache::Connection;
use Apache::Const qw( OK );

#$SIG{__DIE__} = sub {
#	print CGI::header( 'text/html' );
#	print CGI::start_html("Internal Error"),
#		CGI::h1("Internal Error");
#	for(@_) {
#		print CGI::p(CGI::pre(encode_entities($_)));
#	}
#	print CGI::p(CGI::pre(Carp::longmess));
#	print CGI::end_html();
#	exit(0);
#};

use Celestial::Handler;
use Celestial::Handler::login;
use Celestial::Handler::logout;
use Celestial::Handler::status;
use Celestial::Handler::settings;
use Celestial::Handler::import;
use Celestial::Handler::repository;

sub handler
{
	my $r = shift;

	binmode(STDOUT,":utf8");

	$r->status( OK );
	$r->content_type( "text/html; charset: utf-8" );

###########################################################

	my $dbh = Celestial::DBI->connect()
		or die("Unable to connect to database: $!");
	my $dom = XML::LibXML::Document->new('1.0','UTF-8');

###########################################################

	my $cgi;
	{
		my $section = URI->new(CGI::self_url())->path;
		$section = $section =~ /\/(\w+)\/?$/ ? $1 : '';

		my $action = CGI::param('action') || '';

		$cgi = Celestial::CGI->new(
				request => $r,
				section => $section,
				action => $action,
				user => $r->user,
				authorised => $r->user,
				base_url => URI->new(CGI::url()),
				form_action => URI->new(CGI::url()),
				url => URI->new(CGI::self_url()),
				script_path => URI->new(CGI::url())->path,
				hostname => $r->hostname,
				remote_ip => $r->connection->remote_ip,
				referer => $r->headers_in->{ 'Referer' },
				auth => undef,
				);
	}

###########################################################

	my $auth = Celestial::Auth->new(
			dbh => $dbh
	);
	$auth->authenticate( $cgi );
	$cgi->auth( $auth );

###########################################################

# Set up the Handler::ORDER and Handler::DEFAULT
	@Handler::NAVBAR = @Handler::ORDER = ();
	for(qw( login logout status import settings repository )) {
		my $h = "Celestial::Handler::".$_;
		$h->init( $cgi );
	}

###########################################################

	$dom->createInternalSubset( "html", "-//W3C//DTD XHTML 1.0 Strict//EN", "http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd" );

	$Handler::dom = $dom; # for dataElement

	$dom->setDocumentElement(my $doc = $dom->createElement('html'));

	$doc->appendChild(my $head = $dom->createElement('head'));
	$doc->appendChild(my $body = $dom->createElement('body'));

	$head->appendChild(dataElement( 'style', "\@import url(/generic.css);", { type => 'text/css', media => 'screen' }));

# Window title
	my $wtitle = $head->appendChild(dataElement( 'title', 'Celestial - ' ))->getFirstChild;

# Body title
	my $ptitle = $body->appendChild(dataElement( 'h1', 'Celestial - '))->getFirstChild;

# Top navigation bar
	$body->appendChild(my $topbar = dataElement( 'div', undef, {class=>'topbar'} ));
	$topbar->appendChild(my $navbar = dataElement( 'ul', undef, {class=>'navbar'} ));

	my $section = $Handler::DEFAULT;

# Find the handler
	for(@Handler::ORDER)
	{
		if( $_ eq $cgi->section )
		{
			$section = $_;
			last;
		}
	}

	$cgi->section( $section );

	my $h = "Celestial::Handler::$section";
	$h = $h->new( $dbh, $dom );

	for(@Handler::NAVBAR) {
		my $link = dataElement( 'a', $cgi->msg( "navbar.$_", $cgi->user ), { href=>$cgi->as_link( $_ ), class=>'navbar' });
		my $li = dataElement( 'li', $link, {class=>$_ eq $section ? 'navbar hilite' : 'navbar'} );
		$navbar->appendChild( $li ) if defined($li);
	}

	$wtitle->appendData( $h->title($cgi) );
	$ptitle->appendData( $h->title($cgi) );

	$body->appendChild( $h->body($cgi) );

	$dbh->disconnect;

	if( $r->status == OK ) {
		$dom->toFH(\*STDOUT,1);
	}

	return $r->status;
}

package Celestial::CGI;

use Celestial::Config; # Exports $SETTINGS

use YAML;
use vars qw( $AUTOLOAD );
use Apache::Const qw( REDIRECT );

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
	return $self->base_url($self->section);
}

sub as_link {
	my( $self, @args ) = @_;
	my $section = @args % 2 == 1 ? shift @args : $self->section;
	my $url = URI->new( $self->base_url->path . '/' . $section, 'http' );
	$url->query_form( @args );
	return $url;
}

sub msg {
	my( $self, $term, @vars ) = @_;
	my $phrase = $self->phrases->{ $term };
	return "['$term']" unless defined($phrase);
	$phrase =~ s/\$(\d+)/$vars[$1-1]||"[invalid var index $1]"/seg;
	return $phrase;
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

sub param {
	shift;
	CGI::param( @_ );
}

sub redirect {
	my $self = shift;
	my $r = $self->request;
	$r->err_headers_out->add( 'Location' => shift );
	$r->status( REDIRECT );
}

package Celestial::Auth;

use CGI::Cookie;
use vars qw( $dbh );

sub new {
	my( $class, %opts ) = @_;
	$dbh = $opts{ dbh };
	bless \%opts, $class;
}

sub _cname {
	'celestial';
}

sub _key {
	my $c = shift;
	my $str = '';
	for(1..$c) {
		$str .= chr(ord('a')+int(rand(26)));
	}
	return $str;
}

sub _clearup {
	my( $self ) = @_;
	
	$dbh->do("DELETE FROM Sessions WHERE `datestamp` + INTERVAL 1 DAY < NOW()");
}

sub get_session {
	my( $self, $key, $ip ) = @_;

	$self->_clearup;
	my $sth = $dbh->prepare("SELECT `user` FROM Sessions WHERE `key`=? AND `ip`=?");
	$sth->execute( $key, $ip );
	my ($ds) = $sth->fetchrow_array or return;
	return $ds;
}

sub add_session {
	my( $self, $user, $key, $ip ) = @_;

	$self->_clearup;
	$dbh->do("INSERT IGNORE INTO Sessions (`user`,`key`,`ip`,`datestamp`) VALUES (?,?,?,NOW())",{},$user,$key,$ip);
}

sub remove_session {
	my( $self, $key, $ip ) = @_;

	$self->_clearup;
	$dbh->do("DELETE FROM Sessions WHERE `key`=? AND `ip`=?",{},$key,$ip);
}

sub authenticate {
	my( $self, $CGI ) = @_;

	my $ip = $CGI->remote_ip;

	if( $CGI->authorised and $CGI->user ) {
		my $key = _key(60);
		$self->add_session( $CGI->user, $key, $ip );
		$self->set_cookie( $CGI, $key );
		return;
	}

	my $key = $self->get_cookie( $CGI ) or return;

	if( defined(my $user = $self->get_session( $key, $ip )) ) {
		$CGI->user( $user );
		$CGI->authorised( 1 );
	} else {
		$self->logout( $CGI );
	}
}

sub logout($$) {
	my( $self, $CGI ) = @_;

	if( my $key = $self->get_cookie( $CGI ) ) {
		my $ip = $CGI->remote_ip;
		$self->remove_session( $key, $ip );
	}
	
	$CGI->authorised( undef );
	$CGI->user( undef );
	$self->set_cookie( $CGI, '' );
}

sub set_cookie {
	my( $self, $CGI, $value ) = @_;

	my $cookie = CGI::cookie(
			-name => $self->_cname,
			-value => $value,
			-expires => 0,
			-path => $CGI->script_path,
			-domain => $CGI->hostname,
	);

	$CGI->request->err_headers_out->add('Set-Cookie' => $cookie);
}

sub get_cookie {
	my( $self, $CGI ) = @_;

	my $hdr = $CGI->request->headers_in->{ 'Cookie' } or return;
	my @jar = map { split /=/, $_, 2 } split /;/, $hdr;
	return unless @jar % 2 == 0;
	my %cookies = @jar;

	return $cookies{ $self->_cname };
}

1;
