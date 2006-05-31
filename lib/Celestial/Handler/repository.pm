package Celestial::Handler::repository;

use strict;
use warnings;

use Celestial::Handler;
use vars qw( @ISA );
@ISA = qw( Celestial::Handler );

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
	return $self->error( $CGI->msg( 'error.norepository' )) unless defined($repoid);
	my $repo = $dbh->getRepository( $repoid );
	return $self->error( $CGI->msg( 'error.nosuchrepository' )) unless defined($repo);

	my $body = $dom->createElement( 'div' );

	$body->appendChild( dataElement( 'h2', $repo->identifier ));

	$self->_oai_links($body, $CGI, $repo);

	$body->appendChild( dataElement( 'p' ));

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

	my $baseurl = $CGI->as_link('oai/' . $repo->identifier);

	$body->appendChild( my $table = dataElement( 'table' ));
	$table->appendChild( dataElement( 'caption', $CGI->msg( 'repository.oai' )));

	foreach my $verb (qw(Identify ListMetadataFormats ListSets)) {
		$baseurl->query_form(verb => $verb);
		$table->appendChild( my $tr = dataElement( 'tr' ));
		$tr->appendChild( dataElement( 'td', $verb ));
		$tr->appendChild( dataElement( 'td', urlElement( $baseurl )));
	}
#	foreach my $mdf ($repo->listMetadataFormats) {
#		foreach my $verb (qw(ListIdentifiers ListRecords)) {
#			$baseurl->query_form(
#				verb => $verb,
#				metadataPrefix => $mdf->metadataPrefix
#			);
#			$table->appendChild( my $tr = dataElement( 'tr' ));
#			$tr->appendChild( dataElement( 'td', $verb . '/' . $mdf->metadataPrefix ));
#			$tr->appendChild( dataElement( 'td', urlElement( $baseurl )));
#		}
#	}
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

	$body->appendChild( dataElement( 'h4', $CGI->msg( 'repository.subtitle.canedit' )));

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

	$body->appendChild( dataElement( 'h4', $CGI->msg( 'repository.subtitle.mdfs' )));

	foreach my $mdf ($repo->listMetadataFormats) {
		my $tr;
		$body->appendChild( my $fs = dataElement( 'fieldset' ));
		$fs->appendChild( dataElement( 'legend', $mdf->metadataPrefix ));

		$fs->appendChild( my $table = dataElement( 'table' ));
		foreach my $field (qw( metadataNamespace schema )) {
			$table->appendChild( tableRowElement( $CGI->msg( 'mdf.'.lc($field) ), urlElement( $mdf->$field )));
		}
		$table->appendChild( tableRowElement( $CGI->msg( 'mdf.lastharvest' ), $CGI->datestamp($mdf->lastHarvest) ));
		$table->appendChild( tableRowElement( $CGI->msg( 'mdf.cardinality' ), $mdf->cardinality ));

		$table = dataElement( 'table' );
		my $c = 0;
		my $errs = $mdf->listErrors();
		while( my $err = <$errs> ) {
			last if ++$c == 5;
			$table->appendChild( $tr = dataElement( 'tr' ));
			$tr->appendChild( dataElement( 'td', $err->datestamp ));
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

1;
