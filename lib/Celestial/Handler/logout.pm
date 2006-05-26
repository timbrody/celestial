package Celestial::Handler::logout;

use strict;
use warnings;

use Celestial::Handler;
use vars qw( @ISA );
@ISA = qw( Celestial::Handler );

push @ORDER, 'logout';

sub navbar {
	my( $class, $CGI ) = @_;
	return $CGI->authorised;
}

sub title {
	my( $self, $CGI ) = @_;
	return $CGI->msg( 'logout.title' );
}

sub page {
	my( $self, $CGI ) = @_;
	
	$CGI->auth->logout( $CGI );

	if( $CGI->referer ) {
		$CGI->redirect( $CGI->referer );
		return;
	}
	$self->SUPER::page( $CGI );
}

sub body {
	my( $self, $CGI ) = @_;
	my $dbh = $self->dbh;
	my $dom = $self->dom;

	my $body = $dom->createElement( 'div' );

	$body->appendChild(dataElement( 'p', "This is a virtual URL to support authentication."));

	if( $CGI->referer ) {
		$body->appendChild(my $p = dataElement( 'p', "Redirecting you back to " ));
		$p->appendChild( dataElement( 'tt', dataElement( 'a', $CGI->referer, {href=>$CGI->referer} )));
	}

	return $body;
}

1;
