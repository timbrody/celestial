package Celestial::Handler::logout;

use Celestial::Handler;
use vars qw( @ISA );
@ISA = qw( Celestial::Handler );

sub init {
	my( $class, $CGI ) = @_;
	push @Handler::ORDER, 'logout';
	if( $CGI->authorised ) {
		push @Handler::NAVBAR, 'logout';
	}
}

sub title {
	my( $self, $CGI ) = @_;
	return $CGI->msg( 'logout.title' );
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

	$CGI->auth->logout( $CGI );
	$CGI->redirect( $CGI->referer );

	return $body;
}

1;
