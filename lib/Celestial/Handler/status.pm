package Celestial::Handler::status;

use Celestial::Handler;
use vars qw( @ISA );
@ISA = qw( Celestial::Handler );

sub init {
	push @Handler::ORDER, 'status';
	push @Handler::NAVBAR, 'status';
	# We'll claim the default
	$Handler::DEFAULT = 'status';
}

sub title {
	my( $self, $CGI ) = @_;
	return $CGI->msg( 'status.title' );
}

sub body {
	my( $self, $CGI ) = @_;
	my $dbh = $self->dbh;
	my $dom = $self->dom;

	my $body = $dom->createElement( 'div' );

	my $table = $body->appendChild( dataElement( 'table' ));
	$table->appendChild( my $caption = dataElement( 'caption' ));

#	my $row = $table->appendChild( dataElement( 'tr' ));
#	if( $CGI->authorised ) {
#		$row->appendChild( dataElement( 'th' ));
#	}
#	for(qw(  BaseURL Schema LastHarvest LastError )) {
#		$row->appendChild( dataElement( 'th', $_ ));
#	}

	my $c = 0;
	$dbh->do("LOCK TABLES Repositories READ, MetadataFormats READ");
	foreach my $repo ($dbh->listRepositories) {
		$c++;
		my @mdfs = $repo->listMetadataFormats;
		my $edit_url = $CGI->as_link('repository',repository=>$repo->id);
		foreach my $mdf (@mdfs) {
			my $ds = $mdf->lastHarvest ?
				$CGI->datestamp($mdf->lastHarvest) :
				$CGI->msg( 'status.noharvestyet' );
			$row = $table->appendChild( dataElement( 'tr', undef, {class=>$c%2 ? 'oddrow' : 'evenrow'} ));
			$row->appendChild( dataElement( 'td', dataElement( 'a', $repo->identifier, {href=>$edit_url}) ));
			$row->appendChild( dataElement( 'td', urlElement( $repo->baseURL )));
			$row->appendChild( dataElement( 'td', $mdf->metadataPrefix ));
			$row->appendChild( dataElement( 'td', $ds ));
		}
		unless(@mdfs) {
			$row = $table->appendChild( dataElement( 'tr', undef, {class=>$c%2 ? 'oddrow' : 'evenrow'} ));
			$row->appendChild( dataElement( 'td', dataElement( 'a', $repo->identifier, {href=>$edit_url}) ));
			$row->appendChild( dataElement( 'td', urlElement( $repo->baseURL )));
			$row->appendChild( dataElement( 'td', 'No metadata formats found', {colspan=>2, align=>'center'}));
		}
	}
	$dbh->do("UNLOCK TABLES");

	$caption->appendText( $CGI->msg( 'status.caption', $c ) );

	return $body;
}

1;
