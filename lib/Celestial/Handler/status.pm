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

sub page
{
	my( $self, $CGI ) = @_;

	return $self->_ajax($CGI) if $CGI->param( 'ajax' );
	
	return $self->SUPER::page($CGI);
}

sub title {
	my( $self, $CGI ) = @_;
	return $CGI->msg( 'status.title' );
}

sub head {
	my( $self, $CGI ) = @_;

	my $head = $self->SUPER::head( $CGI );

#	$head->appendChild( dataElement( 'script', undef, {
#		type => 'text/javascript',
#		src => $CGI->as_link( 'static/ajax/sorttable.js' )
#	}));
	$head->appendChild( dataElement( 'script', undef, {
		type => 'text/javascript',
		src => $CGI->as_link( 'static/ajax/handler/static.js' )
	}));
	$head->appendChild( dataElement( 'script', undef, {
		type => 'text/javascript',
		src => $CGI->as_link( 'static/ajax/handler/status.js' )
	}));

	return $head;
}

sub body
{
	my( $self, $CGI ) = @_;
	my $dbh = $self->dbh;
	my $dom = $self->dom;

	my $body = $dom->createElement( 'div' );
	$body->setAttribute( id => 'status_table' );

	$body->appendChild( my $caption = dataElement( 'p' ));

	$body->appendChild( dataElement( 'p', undef, {
		id => 'show_all_details'
	}));

	my $c = 0;
	foreach my $repo ($dbh->listRepositories)
	{
		$c++;
		$body->appendChild( my $div = dataElement( 'div' ));
		
		# Title
		$div->appendChild( my $titlediv = dataElement( 'div', undef, {class=>'title'} ));
		my $edit_url = $CGI->as_link('repository',repository=>$repo->id);
		$titlediv->appendChild( dataElement( 'a', $repo->id . " - " . $repo->identifier, {
			href => $edit_url
		} ));

		# State
		$div->appendChild( my $statediv = dataElement( 'div', undef, {class=>'state'} ));
		$statediv->appendChild( dataElement( 'span', undef, {
			class => 'detail_link',
			target => $repo->id,
		} ));
		$statediv->appendChild( urlElement( $repo->baseURL ) );
		my $ds = defined($repo->getLock) ? $repo->getLock : undef;
		if( !defined( $ds ) ) {
			$statediv->appendChild( dataElement( 'span', $CGI->tick, {class=>'state passed'}));
		} elsif( $ds > 0 ) {
			$statediv->appendChild( dataElement( 'span', $CGI->unknown, {class=>'state unknown'}));
		} else {
			$statediv->appendChild( dataElement( 'span', $CGI->cross, {class=>'state failed'}));
		}

		# Detail (via AJAX)
		$div->appendChild( dataElement( 'div', undef, {
			id => $repo->id,
			class => 'detail',
		} ));
#		last;
	}
	
	$caption->appendText( $CGI->msg( 'status.caption', $c ) );

	return $body;
}

sub _ajax
{
	my( $self, $CGI ) = @_;
	my $dbh = $self->dbh;
	my $dom = $self->dom;

	$CGI->content_type("text/html; charset=UTF-8");

	my $id = $CGI->param( 'repository' ) || '';
	$id =~ s/\D//sg;
	my $repo = $dbh->getRepository( $id );
	if( !$repo )
	{
		my $div = dataElement( 'div' );
		$div->appendText( 'No metadata formats found.' );
		print $div->toString;
		return;
	}

	my $table = $dom->createElement( 'table' );

	foreach my $mdf ($repo->listMetadataFormats)
	{
		my $i = 0;
		$table->appendChild(my $row = dataElement( 'tr' ));
		my $ds = $mdf->lastHarvest ?
			$CGI->date($mdf->lastHarvest) :
			$CGI->msg( 'status.noharvestyet' );
		$row->appendChild( dataElement( 'td', dataElement( 'a', $mdf->metadataPrefix, { href => $mdf->metadataNamespace }) ));
		$row->appendChild( dataElement( 'td', $ds ));
		$row->appendChild( dataElement( 'td', $mdf->cardinality ));
		$row->appendChild( dataElement( 'td', $CGI->humansize($mdf->storage) ));
		$ds = defined($repo->getLock) ? $repo->getLock : $mdf->locked;
		my $state = $row->appendChild( dataElement( 'span' ));
		my @class = qw( state );
		if( !defined( $ds ) ) {
			$state->appendText( $CGI->tick );
			push @class, qw( passed );
		} elsif( $ds > 0 ) {
			$state->appendText( $CGI->unknown );
			push @class, qw( unknown );
		} else {
			$state->appendText( $CGI->cross );
			push @class, qw( failed );
		}
		$state->setAttribute( class => join(' ', @class) );
	}

	print $table->toString;

	return undef;
}

sub _body_dep {
	my( $self, $CGI ) = @_;
	my $dbh = $self->dbh;
	my $dom = $self->dom;

	my $body = $dom->createElement( 'div' );

	my $table = $body->appendChild( dataElement( 'table', undef, {
		class => 'status sortable',
		id => 'status_table'
	} ));
	$table->appendChild( my $caption = dataElement( 'caption' ));

	my $row = $table->appendChild( dataElement( 'tr' ));
	for(qw( identifier metadataPrefix lastHarvest cardinality storage locked )) {
		my $cid = "sortCol$_";
		$row->appendChild( dataElement( 'th', $CGI->msg( 'input.'.$_ )));
	}

	my $c = 0;
	$dbh->do("LOCK TABLES Repositories READ, MetadataFormats READ, Locks READ");
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
				$row->appendChild( dataElement( 'td', $mdf->cardinality, {align=>'right'} ));
				$row->appendChild( dataElement( 'td', $CGI->humansize($mdf->storage), {align=>'right'} ));
				$ds = defined($repo->getLock) ? $repo->getLock : $mdf->locked;
				if( !defined( $ds ) ) {
					$row->appendChild( dataElement( 'td', $CGI->tick, {class=>'state passed'}));
				} elsif( $ds > 0 ) {
					$row->appendChild( dataElement( 'td', $CGI->unknown, {class=>'state unknown'}));
				} else {
					$row->appendChild( dataElement( 'td', $CGI->cross, {class=>'state failed'}));
				}
				$table->appendChild( $row = dataElement( 'tr', undef, {
					($c % 2) ? (class=>'oddrow') : (),
				}));
			}
			$table->removeChild($row);
		} else {
			$row->appendChild( dataElement( 'td', 'No metadata formats found', {colspan=>4, align=>'center'}));
			my $ds = $repo->getLock;
			if( !defined( $ds ) ) {
				$row->appendChild( dataElement( 'td', $CGI->tick, {class=>'state passed'}));
			} elsif( $ds > 0 ) {
				$row->appendChild( dataElement( 'td', $CGI->unknown, {class=>'state unknown'}));
			} else {
				$row->appendChild( dataElement( 'td', $CGI->cross, {class=>'state failed'}));
			}
		}
	}
	$dbh->do("UNLOCK TABLES");

	$caption->appendText( $CGI->msg( 'status.caption', $c ) );

	return $body;
}

1;
