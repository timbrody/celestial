package Celestial::Apache;

use strict;
use warnings;
use encoding 'utf8'; # Byte strings are also utf8

use Celestial;

use CGI qw/-oldstyle_urls/;
use URI;
use HTML::Entities;

use HTTP::Request;
use LWP::UserAgent;
use HTTP::OAI::Harvester;
use XML::LibXML;

use mod_perl;
use APR::Table;
use Apache2::RequestIO; # Supplies $r->read
use Apache2::Connection;
use Apache2::RequestRec;
#use Apache2::Access;
use Apache2::Const qw( OK );
#use Apache2::Filter;

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

#$SIG{__DIE__} = $SIG{__WARN__} = sub {
#	print STDERR join "\n",
#		@_,
#		Carp::longmess;
#};

use Celestial::Handler;
use Celestial::Handler::login;
use Celestial::Handler::logout;
use Celestial::Handler::status;
use Celestial::Handler::subscription;
use Celestial::Handler::import;
use Celestial::Handler::settings;

use Celestial::Handler::repository;
use Celestial::Handler::static;
use Celestial::Handler::oai;
use Celestial::Handler::listfriends;
use Celestial::Handler::identifiers;

sub handler
{
	my $r = shift;

	binmode(STDOUT,":utf8");

###########################################################

	my $cfg = Celestial::Config->new;

	my $dbh = Celestial::DBI->connect()
		or die("Unable to connect to database: " . $Celestial::DBI::errstr);
	my $dom = XML::LibXML::Document->new('1.0','UTF-8');

	$Handler::dom = $dom; # for dataElement

###########################################################

	my $cgi;
	{
		my $q = CGI->new;

		my $url = URI->new($q->self_url());
			$url->query(undef);
			$url = $url->path;
		my $script_path = $Celestial::Config::SETTINGS->{ paths }->{ script };
		die "Script path undefined in the configuration"
			unless defined $script_path;
# || URI->new(CGI::url())->path;
		my $section = substr($url,length($script_path));
		$section = $section =~ /^\/?([^\/]+)/ ? $1 : '';
		my $section_path = length($section) > 0 ?
			substr($url,length($script_path)+length($section)+1) :
			substr($url,length($script_path));
#warn "url = $url, script = $script_path, section = $section, path = $section_path";

		my $action = $q->param('action') || '';
		my $referer = $r->headers_in->{ 'Referer' };

		$cgi = Celestial::CGI->new(
				cgi => $q,
				status => OK,
				request => $r,
				section => $section,
				action => $action,
				user => $r->user,
				authorised => $r->user,
				base_url => URI->new($q->url()),
				form_action => URI->new($q->url()),
				url => URI->new($q->self_url()),
				script_path => $script_path,
				section_path => $section_path,
				hostname => $r->hostname,
				remote_ip => $r->connection->remote_ip,
				referer => $referer,
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
#	@Handler::NAVBAR = @Handler::ORDER = ();
#	for(qw( login logout status import settings repository )) {
#		my $h = "Celestial::Handler::".$_;
#		$h->init( $cgi );
#	}

###########################################################

	# Get a handler
	my $section = $DEFAULT;
	for(@ORDER)
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

	my $page;
	if( defined($page = $h->page( $cgi ))) {
		$dom->setDocumentElement( $page );
	}

	$dbh->disconnect;

	if( $page ) {
		$r->content_type( "text/html; charset: utf-8" );
		binmode(STDOUT,":raw"); # toFH already converts utf8 to bytes
		$dom->toFH(\*STDOUT,1);
	}

	return $cgi->status;
}

package Celestial::Apache::Error;

use strict;
use warnings;

use CGI qw();
use mod_perl;
use Apache2::RequestIO;
use HTML::Entities;

sub handler
{
	my $r = shift;

	my $q = CGI->new;
	my $status = $ENV{ REDIRECT_STATUS } || 200;
	my $msg = $ENV{ REDIRECT_ERROR_NOTES } || '[No error message available]';

	$r->status($status);
	$r->content_type( 'text/html' );
	
	print $q->start_html(
			-title=>"Internal Server Error",
			-style=>{-code=>qq(
body {
	font-family: sans-serif;
	color: #000;
	background-color: #fff;
	padding: 0px;
	margin: 0px;
}
div.header {
	color: #fff;
	background-color: #009;
	padding-left: 10px;
	padding-right: 10px;
}
div.body {
	padding-left: 10px;
	width: 40em;
}
div.footer {
	padding-left: 10px;
	padding-right: 10px;
}
div.error {
	font-family: monospace;
	padding-left: 20px;
}
.tagline {
	font-size: 80%;
	font-weight: bold;
}
h1 {
	margin: 0px;
}
hr {
	border: 1px inset #99f;
}
)},
		),
		$q->div({class=>'header'}, $q->h1("Internal Server Error")),
		$q->div({class=>'body'},
			$q->p("An internal server error ($status) has occurred. The following message was received:"),
			$q->div({class=>'error'}, encode_entities($msg)),
			$q->p("Please try again later."),
		),
		$q->div({class=>'footer'},
			$q->hr,
			$q->p({class=>'tagline'}, "This page has been generated by the Celestial internal error handler."),
		),
		$q->end_html();

	return 0;
}

package Celestial::Auth;

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

	return $CGI->set_cookie( $self->_cname, $value );
}

sub get_cookie {
	my( $self, $CGI ) = @_;

	return $CGI->get_cookie( $self->_cname );
}

1;
