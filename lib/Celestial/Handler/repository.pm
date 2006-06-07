package Celestial::Handler::repository;

use strict;
use warnings;

use Celestial::Handler;
use vars qw( @ISA );
@ISA = qw( Celestial::Handler );
use URI::Escape qw(uri_escape_utf8);

push @ORDER, 'repository';

sub title {
	my( $self, $CGI ) = @_;
	return $CGI->msg( 'repository.title' );
}

sub body {
	my( $self, $CGI ) = @_;
	my $dbh = $self->dbh;
	my $dom = $self->dom;

	my $repoid = $CGI->param( 'repository' );
	return $self->error( $CGI, $CGI->msg( 'error.norepository' )) unless defined($repoid);
	my $repo = $dbh->getRepository( $repoid );
	return $self->error( $CGI, $CGI->msg( 'error.nosuchrepository', $repoid )) unless defined($repo);

	# We need to do this first, to generate the nosuchrepository error
	if( $CGI->authorised and
		!defined($CGI->param('metadataFormat')) and
		$CGI->action eq 'remove' and
		$CGI->param('confirm') and
		$CGI->param('confirm') eq 'yes'
	)
	{
		$repo->remove;
		return $self->error( $CGI, $CGI->msg( 'error.nosuchrepository', $repoid ));
	}

	my $body = $dom->createElement( 'div' );

	$body->appendChild( dataElement( 'h2', $repo->identifier ));

	$self->_oai_links($body, $CGI, $repo);

	$body->appendChild( dataElement( 'h3', $CGI->msg( 'repository.subtitle.status' ) ));

	$self->_status($body, $CGI, $repo);

	$self->_subscribe($body, $CGI, $repo);

	if( $CGI->authorised ) {
		$self->_can_edit( $body, $CGI, $repo );
	} else {
		$self->_display( $body, $CGI, $repo );
	}

	return $body;
}

sub _oai_links {
	my( $self, $body, $CGI, $repo ) = @_;

	my $baseurl = URI->new($CGI->as_link('oai') . '/' . uri_escape_utf8($repo->identifier), 'http');

	$body->appendChild( my $table = dataElement( 'table' ));
	$table->appendChild( dataElement( 'caption', $CGI->msg( 'repository.oai' )));

	foreach my $verb (qw(Identify ListMetadataFormats ListSets)) {
		$baseurl->query_form(verb => $verb);
		$table->appendChild( my $tr = dataElement( 'tr' ));
		$tr->appendChild( dataElement( 'td', $verb ));
		$tr->appendChild( dataElement( 'td', urlElement( $baseurl )));
	}
}

sub _status {
	my( $self, $body, $CGI, $repo ) = @_;

	my $ds = $repo->getLock;

	$body->appendChild( my $table = dataElement( 'table' ));
	$table->appendChild( my $tr = dataElement( 'tr' ));
	$tr->appendChild( dataElement( 'td', $CGI->msg( 'repository.status.state' )));
	if( !defined( $ds ) ) {
		$tr->appendChild( dataElement( 'td', $CGI->tick, {class=>'state passed'}));
	} elsif( $ds > 0 ) {
		$tr->appendChild( dataElement( 'td', $CGI->unknown, {class=>'state unknown'}));
		$table->appendChild( $tr = dataElement( 'tr' ));
		$tr->appendChild( dataElement( 'td', $CGI->msg( 'repository.locked.since' )));
		$tr->appendChild( dataElement( 'td', $CGI->datestamp( $ds )));
	} else {
		$tr->appendChild( dataElement( 'td', $CGI->cross, {class=>'state failed'}));
	}
}

sub _subscribe {
	my( $self, $body, $CGI, $repo ) = @_;
	my $dbh = $self->dbh;

	my $email = $CGI->param( 'email' ) || $CGI->get_cookie( 'email' ) || '';
	my $freq = $CGI->param( 'frequency' );
	$CGI->set_cookie( 'email', $email ) if $email;

	if( $email and $freq ) {
		my $rep;
		if( !$CGI->valid_email( $email ) ) {
			$body->appendChild( $self->error( $CGI, $CGI->msg( 'error.email', $email) ));
		} elsif( !($freq >= 7 and $freq <= 90) ) {
			$body->appendChild( $self->error( $CGI, $CGI->msg( 'error.frequency', $freq )));
#		} elsif( defined($rep = $repo->getReport( $email )) ) {
#			$repo->removeReport( $email );
#			$body->appendChild( $self->notice( $CGI, $CGI->msg( 'subscribe.unsubscribed', $email, $freq )));
		} else {
			$repo->addReport({
				email => $email,
				frequency => $freq,
				include => 0,
				confirmed => '',
			});
			$body->appendChild( $self->notice( $CGI, $CGI->msg( 'subscribe.subscribed', $email, $freq )));
		}
	}

	$body->appendChild( formElement($CGI,
		hidden => {
			repository => $repo->id,
		},
		legend => $CGI->msg( 'repository.subscribe.legend' ),
		fields => [{
			name => 'email',
			value => $email,
		},{
			name => 'frequency',
			value => 14,
			size => 2,
			maxlength => 2,
		}],
		submit => {
			value => $CGI->msg( 'repository.subscribe' ),
		},
	));
}


sub _display {
	my( $self, $body, $CGI, $repo ) = @_;

	$body->appendChild( my $fs = dataElement( 'fieldset' ));
	$fs->appendChild( my $table = dataElement( 'table' ));
	$table->appendChild( tableRowElement( $CGI->msg( 'input.identifier' ), $repo->identifier ));
	$table->appendChild( tableRowElement( $CGI->msg( 'input.baseURL' ), urlElement( $repo->baseURL ) ));

	$self->_list_mdfs( $body, $CGI, $repo );
}

sub _can_edit {
	my( $self, $body, $CGI, $repo ) = @_;

	return unless $CGI->authorised;

	$body->appendChild( dataElement( 'h3', $CGI->msg( 'repository.subtitle.canedit' )));

	$body->appendChild( formElement($CGI,
		legend => $CGI->msg( 'repository.remove.legend' ),
		hidden => {
			repository => $repo->id,
			action => 'remove',
		},
		fields => [{
#			label => $CGI->msg( 'repository.remove.confirm' ),
			type => 'checkbox',
			name => 'confirm',
			value => 'yes',
		}],
		submit => {
			value => $CGI->msg( 'repository.remove.submit' )
		}
	));

	my $update;
	my @fields = ();
	foreach my $name (qw( identifier baseURL )) {
		if( defined($CGI->param($name)) and length($CGI->param($name)) and $CGI->param($name) ne $repo->$name ) {
			$repo->$name( $CGI->param($name) );
			$update = 1;
		}
		push @fields, {
			name => $name,
			value => $repo->$name,
		};
	}
	$repo->commit if $update;

	$body->appendChild( formElement($CGI,
		method => 'post',
		legend => $CGI->msg( 'repository.legend' ),
		hidden => {
			repository => $repo->id,
		},
		fields => \@fields,
		submit => {
			value => $CGI->msg( 'repository.submit' ),
		},
	));

	$self->_list_mdfs( $body, $CGI, $repo );

	return $body;
}

sub _list_mdfs {
	my( $self, $body, $CGI, $repo ) = @_;

	$body->appendChild( dataElement( 'h3', $CGI->msg( 'repository.subtitle.mdfs' )));

	foreach my $mdf ($repo->listMetadataFormats) {
		my $tr;
		$body->appendChild( dataElement( 'a', undef, {name=>$mdf->metadataPrefix} ));
		$body->appendChild( my $fs = dataElement( 'fieldset' ));
		$fs->appendChild( dataElement( 'legend', $mdf->metadataPrefix ));

		if( $CGI->authorised ) {
			$fs->appendChild( my $p = dataElement( 'p' ));
			$self->_mdf_actions( $p, $CGI, $repo, $mdf );
		}

		$fs->appendChild( my $table = dataElement( 'table' ));
		foreach my $field (qw( metadataNamespace schema )) {
			$table->appendChild( tableRowElement( $CGI->msg( 'mdf.'.lc($field) ), urlElement( $mdf->$field )));
		}
		$table->appendChild( tableRowElement( $CGI->msg( 'mdf.lastharvest' ), $CGI->datestamp($mdf->lastHarvest) ));
		$table->appendChild( tableRowElement( $CGI->msg( 'mdf.cardinality' ), $mdf->cardinality ));
		if( $mdf->locked ) {
			$table->appendChild( $tr = dataElement( 'tr' ));
			$tr->appendChild( dataElement( 'td', $CGI->msg( 'mdf.locked' )));
			$tr->appendChild( dataElement( 'td', $CGI->datestamp($mdf->locked), {class=>'state failed'} ));
		}

		$table = dataElement( 'table' );
		my $c = 0;
		my $errs = $mdf->listErrors();
		while( my $err = <$errs> ) {
			last if ++$c == 5;
			$table->appendChild( $tr = dataElement( 'tr' ));
			$tr->appendChild( dataElement( 'td', $CGI->datestamp($err->datestamp) ));
			$tr->appendChild( dataElement( 'td', dataElement( 'tt', dataElement( 'a', $err->url, { href=>$err->url } ))));
			$table->appendChild( $tr = dataElement( 'tr' ));
			$tr->appendChild( dataElement( 'td', dataElement( 'tt', $err->error), {colspan=>2} ));
		}
		if( $c > 0 ) {
			$table->appendChild( dataElement( 'caption', $CGI->msg( 'mdf.error.title' )));
			$fs->appendChild( $table );
		}
	}

}

sub _mdf_actions {
	my( $self, $body, $CGI, $repo, $mdf ) = @_;

	return unless $CGI->authorised;

	if( defined($CGI->param('metadataFormat')) and
		$CGI->param('metadataFormat') == $mdf->id )
	{
		if( $CGI->action eq 'reharvest' ) {
			$mdf->reset;
		}
		elsif( $CGI->action eq 'delete' ) {
			$mdf->removeAllRecords;
		}
		elsif( $CGI->action eq 'lock' ) {
			$mdf->lock;
		}
		elsif( $CGI->action eq 'unlock' ) {
			$mdf->unlock;
		}
	}

	my @actions = qw(reharvest delete);
	push @actions, $mdf->locked ? 'unlock' : 'lock';
	foreach(@actions) {
		$body->appendChild( dataElement( 'a', $CGI->msg('repository.mdfs.'.$_), {href=>$CGI->as_link('repository', repository=>$repo->id, metadataFormat=>$mdf->id, action=>$_) . '#' . $mdf->metadataPrefix}));
		$body->appendText( ' ' );
	}
}

1;
