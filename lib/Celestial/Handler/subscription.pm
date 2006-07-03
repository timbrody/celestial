package Celestial::Handler::subscription;

use strict;
use warnings;

use Celestial::Handler;
use vars qw( @ISA );
@ISA = qw( Celestial::Handler );

push @ORDER, 'subscription';

sub navbar { 1 }

sub title {
	my( $self, $CGI ) = @_;
	return $CGI->msg( 'subscription.title' );
}

sub body {
	my( $self, $CGI ) = @_;
	my $dbh = $self->dbh;
	my $dom = $self->dom;

	my $email = $CGI->param( 'email' ) || $CGI->get_cookie( 'email' ) || '';
	$CGI->set_cookie( 'email', $email ) if $email;

	my $body = $dom->createElement( 'div' );

	$body->appendChild( formElement($CGI,
		fields => [{
			name => 'email',
			value => $email,
		}],
		submit => {
			value => $CGI->msg( 'subscription.submit' ),
		},
	));

	if( $email ) {
		$self->_edit_reports( $body, $CGI, $email );
	}

	return $body;
}

sub _edit_reports {
	my( $self, $body, $CGI, $email ) = @_;
	my $dbh = $self->dbh;

	my @reps = $dbh->listReportsByEmail( $email );
	unless( @reps ) {
		$body->appendChild( $self->error( $CGI, $CGI->msg( 'error.nosubscriptions', $email )));
		return;
	}

	if( $CGI->action eq 'remove' ) {
		my( $id, $repo );
		if( defined($id = $CGI->param( 'repository' )) and
			defined($repo = $dbh->getRepository( $id )) ) {
			$repo->removeReport( $email );
			$body->appendChild( $self->notice( $CGI, $CGI->msg( 'subscription.removed', $repo->identifier )));
			@reps = $dbh->listReportsByEmail( $email );
			return if scalar(@reps) == 0;
		}
	} elsif( $CGI->action eq 'removeall' and $CGI->param( 'confirm' ) ) {
		foreach my $rep (@reps) {
			$rep->repository->removeReport( $email );
		}
		$body->appendChild( $self->notice( $CGI, $CGI->msg( 'subscription.removedall' )));
		return;
	} elsif( $CGI->action eq 'combine' and my $freq = $CGI->param( 'frequency' )) {
		if( !($freq >= 7 and $freq <= 90) ) {
			$body->appendChild( $self->error( $CGI, $CGI->msg( 'error.frequency' )));
		} else {
			foreach my $rep (@reps) {
				$rep->previous( undef );
				$rep->frequency( $freq );
				$rep->commit;
			}
		}
	} elsif( $CGI->action eq 'confirm' ) {
		foreach my $rep (@reps) {
			$rep->confirmed( 1 );
			$rep->commit;
		}
		$body->appendChild( $self->notice( $CGI, $CGI->msg( 'subscription.confirmed' )));
	}

	$body->appendChild( dataElement( 'h4', $CGI->msg( 'subscription.subtitle', $email )));

	$body->appendChild( formElement($CGI,
		hidden => {
			email => $email,
			action => 'removeall',
		},
		legend => $CGI->msg( 'subscription.removeall.legend' ),
		fields => [{
			type => 'checkbox',
			name => 'confirm',
			value => 'yes',
		}],
		submit => {
			name => 'submit',
			value => $CGI->msg( 'subscription.removeall.submit' )
		},
	));

	$body->appendChild( formElement($CGI,
		note => $CGI->msg( 'subscription.combine.warning' ),
		hidden => {
			email => $email,
			action => 'combine',
		},
		legend => $CGI->msg( 'subscription.combine.legend' ),
		fields => [{
			name => 'frequency',
			value => 14,
			size => 2,
			maxlength => 2,
		}],
		submit => {
			name => 'submit',
			value => $CGI->msg( 'subscription.combine.submit' )
		},
	));

	$body->appendChild( my $table = dataElement( 'table' ));
	$table->appendChild( dataElement( 'caption', $CGI->msg( 'subscription.caption' )));
	$table->appendChild( my $tr = dataElement( 'tr', undef, {class => 'heading'} ));
	$tr->appendChild( dataElement( 'th', $CGI->msg( 'input.identifier' )));
	$tr->appendChild( dataElement( 'th', $CGI->msg( 'input.frequency' )));
	$tr->appendChild( dataElement( 'th', $CGI->msg( 'input.previous' )));
	$tr->appendChild( dataElement( 'th' ));
	foreach my $rep (@reps) {
		my $repo = $rep->repository;
		my( $tr, $td );
		$table->appendChild( $tr = dataElement( 'tr' ));
		$tr->appendChild( $td = dataElement( 'td', dataElement( 'a', $repo->identifier, {href=>$CGI->as_link('repository', repository => $repo->id)} )));
		$tr->appendChild( $td = dataElement( 'td', $rep->frequency, {'align'=>'center'} ));
		$tr->appendChild( $td = dataElement( 'td', $CGI->datestamp($rep->previous), {'align'=>'center'} ));
		$tr->appendChild( $td = dataElement( 'td', dataElement( 'a', $CGI->msg( 'subscription.remove' ), { href=>$CGI->as_link($CGI->section, repository => $repo->id, email => $email, action => 'remove') } )));
	}
}

1;
