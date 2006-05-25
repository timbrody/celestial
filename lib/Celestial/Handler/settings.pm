package Celestial::Handler::settings;

use Celestial::Handler;
use vars qw( @ISA );
@ISA = qw( Celestial::Handler );

sub init {
	my( $class, $CGI ) = @_;
	push @Handler::ORDER, 'settings';
	if( $CGI->authorised ) {
		push @Handler::NAVBAR, 'settings';
	}
}

sub title {
	my( $self, $CGI ) = @_;
	return $CGI->msg( 'settings.title' );
}

sub body {
	my( $self, $CGI ) = @_;
	my $dbh = $self->dbh;
	my $dom = $self->dom;

	return $self->no_auth( $CGI ) unless $CGI->authorised;

	my $body = $dom->createElement( 'div' );

	my @fields;
	for($dbh->listConfigs) {
		if( defined($CGI->param( $_ )) and length($CGI->param( $_ ))) {
			$dbh->_config( $_, $CGI->param( $_ ));
		}
		push @fields, {
			name => $_,
			value => $dbh->_config( $_ ),
		};
	}

	$body->appendChild( formElement($CGI,
		legend => $CGI->msg( 'settings.legend' ),
		submit => {
			value => $CGI->msg( 'settings.submit' ),
		},
		fields => \@fields,
	));

	return $body;
}

1;
