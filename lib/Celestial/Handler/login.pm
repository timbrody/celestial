package Celestial::Handler::login;

use strict;
use warnings;

use Celestial::Handler;
use vars qw( @ISA );
@ISA = qw( Celestial::Handler );

push @ORDER, 'login';

sub navbar {
	my( $class, $CGI ) = @_;
	return !$CGI->authorised;
}

sub page {
	my( $self, $CGI ) = @_;
	if( $CGI->referer ) {
		$CGI->redirect( $CGI->referer );
		return;
	}
	return $self->SUPER::page( $CGI );
}

sub title {
	my( $self, $CGI ) = @_;
	return $CGI->msg( 'login.title' );
}

sub body {
	my( $self, $CGI ) = @_;
	my $dbh = $self->dbh;
	my $dom = $self->dom;

	my $body = $dom->createElement( 'div' );

	$body->appendChild(dataElement( 'p', "This is a virtual URL to support authentication. If you see this, you probably haven't configured your web server to authenticate /login." ));

	if( $CGI->referer ) {
		$body->appendChild(my $p = dataElement( 'p', "Redirecting you back to " ));
		$p->appendChild( dataElement( 'tt', dataElement( 'a', $CGI->referer, {href=>$CGI->referer} )));
	}

	return $body;
}

1;
