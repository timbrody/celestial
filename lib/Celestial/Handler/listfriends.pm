package Celestial::Handler::listfriends;

use strict;
use warnings;

use Celestial::Handler;
use vars qw( @ISA );
@ISA = qw( Celestial::Handler );

push @ORDER, 'listfriends';

sub page
{
	my( $self, $CGI ) = @_;
	my $dbh = $self->dbh;
	my $dom = $self->dom;

	$dom->setDocumentElement(my $root = $dom->createElement('BaseURLs'));

	my $c = 0;
	my $mirror = $CGI->url->clone;
	foreach my $repo ($dbh->listRepositories) {
		$c++;
		$mirror->path($CGI->as_link( 'oai' ) . '/' . $CGI->uri_escape($repo->identifier));
		$root->appendChild( dataElement( 'baseURL', $repo->baseURL, {
			id => $repo->identifier,
			mirror => $mirror,
		}));
	}
	$root->setAttribute( 'number', $c );

	$CGI->content_type( 'text/xml; charset=utf-8' );
	$dom->toFH(\*STDOUT,1);

	return undef;
}

1;
