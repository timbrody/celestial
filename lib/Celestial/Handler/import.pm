package Celestial::Handler::import;

use strict;
use warnings;

use utf8;

use Celestial::Handler;
use vars qw( @ISA );
@ISA = qw( Celestial::Handler );

push @ORDER, 'import';

use URI;
require URI::Find;
use HTTP::OAI;
require LWP::UserAgent;

use constant {
	UNKNOWN => 0,
	FAILED => 1,
	PASSED => 2,
	ADDED => 3,
};

use vars qw( @FOUND_URLS );

our @IGNORE_URL_REGEXP = (
	qr/www\.openarchives\.org/,
	qr/www\.w3\.org/,
);

sub navbar {
	my( $class, $CGI ) = @_;
	return $CGI->authorised;
}

sub title {
	my( $self, $CGI ) = @_;
	return $CGI->msg( 'import.title' );
}

sub page {
	my( $self, $CGI ) = @_;

	if( $CGI->param( 'ajax' )) {
		return $self->_ajax( $CGI );
	}

	return $self->SUPER::page( $CGI );
}

sub head {
	my( $self, $CGI ) = @_;

	my $head = $self->SUPER::head( $CGI );
	
	$head->appendChild( dataElement( 'script', undef, {type=>'text/javascript', src=>$CGI->as_link('static/ajax/utils.js')}));
	$head->appendChild( dataElement( 'script', undef, {type=>'text/javascript', src=>$CGI->as_link('static/ajax/handler/'.$CGI->section.'.js')}));

	return $head;
}

sub body {
	my( $self, $CGI ) = @_;
	my $dbh = $self->dbh;
	my $dom = $self->dom;

	return $self->no_auth( $CGI ) unless $CGI->authorised;

	my $body = $dom->createElement( 'div' );

	if( $CGI->param( 'base_url_count' ) or my @qurls = $CGI->param( 'base_url' ) ) {
		$self->process_urls( $body, $CGI, @qurls );
	} elsif( my( $qurl ) = $CGI->param( 'query_url' ) ) {
		$self->extract_urls( $body, $CGI, $qurl );
	} else {
		$self->default( $body, $CGI );
	}

	return $body;
}

sub default {
	my( $self, $body, $CGI ) = @_;
	my $dbh = $self->dbh;
	my $dom = $self->dom;
	
	$body->appendChild( formElement($CGI,
		legend => $CGI->msg( 'import.query.legend' ),
		fields => [{
			name => 'query_url',
		}],
		submit => {
			name => 'action',
			value => $CGI->msg( 'import.query.submit' ),
		},
	));
}

sub extract_urls {
	my( $self, $body, $CGI, $url ) = @_;
	my $dbh = $self->dbh;

	$url = URI->new($url);

	unless(
		($url->scheme eq 'http' or $url->scheme eq 'https') and
		$url->host and $url->path
	) {
		return $body->appendChild( $self->error( $CGI->msg( 'error.import.invalid_url', $url )));
	}

	$body->appendChild( dataElement( 'p', dataElement( 'tt', $url )));

	my @base_urls;
	my @unknown;
	my %state;

	{
		my $u = $url->clone;
		$u->query(undef); # Clear any query component
		push @base_urls, $u;
		if( defined(my $id = $dbh->getRepositoryBaseURL( $u )) ) {
			$state{$u} = ADDED;
		} else {
			$state{$u} = UNKNOWN;
			push @unknown, $u;
		}
	}

	my $ua = LWP::UserAgent->new();
	$ua->from( $dbh->adminEmail );
	$ua->timeout( 30 );

	my $r = $ua->get( $url );
	if( $r->code != 200 ) {
		return $body->appendChild( $self->error( $r->message ));
	}

	@FOUND_URLS = ();

	URI::Find->new(\&_find_cb)->find( $r->content_ref );

	foreach my $url (@FOUND_URLS)
	{
		$url = $url->canonical;
		next if exists($state{$url});
		my $ignore = 0;
		for(@IGNORE_URL_REGEXP)
		{
			($url =~ $_) && ($ignore = 1) && last;
		}
		next if $ignore;
		push @base_urls, $url;
		if( defined(my $id = $dbh->getRepositoryBaseURL( $url )) ) {
			$state{$url} = ADDED;
		} else {
			$state{$url} = UNKNOWN;
			push @unknown, $url;
		}
	}

	$body->appendChild( my $form = dataElement( 'form', undef, {method=>'post',action=>$CGI->form_action},id=>'process_form' ));
	$form->appendChild( my $fs = dataElement( 'fieldset', undef, {class=>'input'} ));
	$fs->appendChild( dataElement( 'legend', $CGI->msg( 'import.queryall.legend' )));
	$fs->appendChild( my $table = dataElement( 'table', undef, {class=>'input'} ));
	#$table->appendChild( my $tr = dataElement( 'tr', undef, {class=>'input'} ));

	my $i = 0;
	foreach my $url (@base_urls) {
		my $id = "base_url".$i++;
		$table->appendChild( my $tr = dataElement( 'tr', undef, {class=>'input'} ));
		$tr->appendChild( dataElement( 'td', dataElement( 'label', dataElement( 'tt', $url), {'for'=>$id} ), {class=>'input'} ));
		if( $state{$url} == ADDED ) {
			$tr->appendChild( dataElement( 'td', dataElement( 'input', undef, {type=>'checkbox', name=>'_base_url', id=>$id, value=>$url, checked=>'yes', disabled=>'yes'} ), {class=>'input'} ));
			$tr->appendChild( dataElement( 'td', $CGI->tick, {class=>'state passed'} ));
		} else {
			$tr->appendChild( dataElement( 'td', dataElement( 'input', undef, {type=>'checkbox', name=>$id, id=>$id, value=>$url, checked=>'yes'} ), {class=>'input'} ));
			$tr->appendChild( dataElement( 'td', $CGI->unknown, {class=>'state unknown'} ));
		}
	}
	$fs->appendChild( dataElement( 'input', undef, {type=>'hidden', name=>'base_url_count', value=>$i} ));

	if( @unknown ) {
		$fs->appendChild( dataElement( 'input', undef, {type=>'submit', name=>'submit', value=>$CGI->msg( 'import.add.submit' )} ));
	} else {
		$fs->appendChild( dataElement( 'p', $CGI->msg('import.done') ));
	}

}

sub _find_cb
{
	push @FOUND_URLS, shift;
}

sub process_urls
{
	my( $self, $body, $CGI, @base_urls ) = @_;
	my $dbh = $self->dbh;

	my @ignore = $CGI->param( 'ignore' );

	$self->onload( 'processUrls();' );

	# First request has checkboxes!
	if( defined(my $c = $CGI->param('base_url_count')) ) {
		for(0..($c-1)) {
			push @base_urls, $CGI->param( 'base_url' . $_ )
				if defined($CGI->param( 'base_url' . $_ ) and $CGI->param( 'base_url' . $_ ) eq 'yes' );
		}
	}

	$body->appendChild( dataElement( 'pre', undef, {id=>'debug'}));

	$body->appendChild( $self->help( $CGI->msg( 'import.check.help' )));

	# Supremely inefficient ...
	foreach my $url (@ignore) {
		@base_urls = grep { $_ ne $url } @base_urls;
	}

	$body->appendChild( my $form = dataElement( 'form', undef, {method=>'post', action=>$CGI->form_action, id=>'base_url_form'} ));
	$form->appendChild( my $fs = dataElement( 'fieldset', undef, {class=>'input'} ));
	foreach(qw(tick cross unknown)) {
		$fs->appendChild( dataElement( 'input', undef, {type=>'hidden',name=>"language.$_",id=>"language.$_",value=>$CGI->$_} ));
	}
	$fs->appendChild( dataElement( 'legend', $CGI->msg( 'import.check.legend' )));
	$fs->appendChild( my $table = dataElement( 'table', undef, {class=>'input'} ));
	foreach my $url (@base_urls) {
		$table->appendChild( my $tr = dataElement( 'tr', undef, {class=>'input'} ));
		$tr->appendChild( dataElement( 'td', urlElement( $url ), {class=>'input'}));
		$tr->appendChild( dataElement( 'input', undef, {type=>'hidden',name=>'base_url',value=>$url} ));
		$tr->appendChild( dataElement( 'td', $CGI->unknown, {id=>$url,class=>'state unknown'} ));
	}

	$fs->appendChild( dataElement( 'p', $CGI->msg('import.done'), {id=>'done_message', style=>'display: none;'} ));
}

sub _add_repository
{
	my( $self, $url, $identify ) = @_;
	my $dbh = $self->dbh;

	my $name = $url;
	if( $identify->repositoryName ) {
		$name = $identify->repositoryName;
	}

	return if defined($dbh->getRepositoryBaseURL( $url ));
	my $repo = Celestial::DBI::Repository->new(
		dbh=>$dbh,
		id => undef,
		identifier => $name,
		baseURL => $url,
		Identify => $identify->toDOM->toString,
	);

	return $dbh->addRepository( $repo );
}

sub _ajax
{
	my( $self, $CGI ) = @_;
	my $dbh = $self->dbh;

	unless( $CGI->authorised ) {
		warn "Error: Unauthorised AJAX request";
		return;
	}

	$CGI->content_type( 'text/plain' );

	my $url = $CGI->param( 'base_url' ) or return;
	my $base_url = URI->new($url);
	$base_url->query( undef );

	my $ha = HTTP::OAI::Harvester->new( baseURL => $base_url );
	$ha->agent( $dbh->adminEmail );
	$ha->timeout( 30 );
	my $id = $ha->Identify();
	
	my $success = 0;
	if( $id->is_success ) {
		$success = 1;
		$self->_add_repository($url, $id);
	}

	print join(" ", $url, $success);

	return undef;
}

1;
