package Celestial::Handler::status;

use strict;
use warnings;

use Celestial::Handler;
use vars qw( @ISA );
@ISA = qw( Celestial::Handler );

# We'll claim the default
push @ORDER, 'status';
$DEFAULT = 'status';

sub navbar { 1 }

sub title {
	my( $self, $CGI ) = @_;
	return $CGI->msg( 'status.title' );
}

sub body {
	my( $self, $CGI ) = @_;
	my $dbh = $self->dbh;
	my $dom = $self->dom;

	my $body = $dom->createElement( 'div' );

	my $table = $body->appendChild( dataElement( 'table', undef, {class=>'status'} ));
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
		my $row = $table->appendChild( dataElement( 'tr', undef, {
			($c % 2) ? (class=>'oddrow') : (),
		}));
		my $a = dataElement( 'a', $repo->identifier, {href=>$edit_url, class=>'status'});
		$row->appendChild( @mdfs > 1 ?
			dataElement( 'td', $a, {rowspan=>scalar(@mdfs)} ) :
			dataElement( 'td', $a ) );
		if( @mdfs ) {
			foreach my $mdf (@mdfs) {
				my $ds = $mdf->lastHarvest ?
					$CGI->date($mdf->lastHarvest) :
					$CGI->msg( 'status.noharvestyet' );
				$row->appendChild( dataElement( 'td', $mdf->metadataPrefix ));
				$row->appendChild( dataElement( 'td', $ds ));
				$table->appendChild( $row = dataElement( 'tr', undef, {
					($c % 2) ? (class=>'oddrow') : (),
				}));
			}
			$table->removeChild($row);
		} else {
			$row->appendChild( dataElement( 'td', 'No metadata formats found', {colspan=>2, align=>'center'}));
		}
	}
	$dbh->do("UNLOCK TABLES");

	$caption->appendText( $CGI->msg( 'status.caption', $c ) );

	return $body;
}

1;
