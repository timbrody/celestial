package Celestial::Handler::login;

use Celestial::Handler;
use vars qw( @ISA );
@ISA = qw( Celestial::Handler );

sub init {
	my( $class, $CGI ) = @_;
	push @Handler::ORDER, 'login';
	unless( $CGI->authorised ) {
		push @Handler::NAVBAR, 'login';
	}
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

	$CGI->redirect($CGI->referer);

	return $body;
}

1;
