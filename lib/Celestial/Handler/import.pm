package Celestial::Handler::import;

use strict;
use warnings;

use Celestial::Handler;
use vars qw( @ISA );
@ISA = qw( Celestial::Handler );

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

sub init {
	my( $class, $CGI ) = @_;
	push @Handler::ORDER, 'import';
	if( $CGI->authorised ) {
		push @Handler::NAVBAR, 'import';
	}
}

sub title {
	my( $self, $CGI ) = @_;
	return $CGI->msg( 'import.title' );
}

sub body {
	my( $self, $CGI ) = @_;
	my $dbh = $self->dbh;
	my $dom = $self->dom;

	return $self->no_auth( $CGI ) unless $CGI->authorised;

	my $body = $dom->createElement( 'div' );

	my $action = lc($CGI->param( 'action' ) || '');

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
		submit => {
			name => 'action',
			value => $CGI->msg( 'import.query.submit' ),
		},
		fields => [{name => 'query_url', size => 100}],
	));
}

sub extract_urls {
	my( $self, $body, $CGI, $url ) = @_;
	my $dbh = $self->dbh;

	$url = URI->new($url);

	unless(
		($url->scheme eq 'http' or $url->scheme eq 'http') and
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
	if( $r->is_error ) {
		return $body->appendChild( $self->error( $r->message ));
	}

	@FOUND_URLS = ();

	URI::Find->new(\&_find_cb)->find( $r->content_ref );

	for(@FOUND_URLS) {
		$_ = $_->canonical;
		next if exists($state{$_});
		push @base_urls, $_;
		if( defined(my $id = $dbh->getRepositoryBaseURL( $_ )) ) {
			$state{$_} = ADDED;
		} else {
			$state{$_} = UNKNOWN;
			push @unknown, $_;
		}
	}

	$body->appendChild( my $form = dataElement( 'form', undef, {method=>'post',action=>$CGI->form_action} ));
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
			$tr->appendChild( dataElement( 'td', chr(0x2713), {class=>'state passed'} ));
		} else {
			$tr->appendChild( dataElement( 'td', dataElement( 'input', undef, {type=>'checkbox', name=>$id, id=>$id, value=>$url, checked=>'yes'} ), {class=>'input'} ));
			$tr->appendChild( dataElement( 'td', '?', {class=>'state unknown'} ));
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

	my $action = lc($CGI->param( 'action' ) || '');

	my @ignore = $CGI->param( 'ignore' );
	my @failed = $CGI->param( 'failed' );
	my @added = $CGI->param( 'added' );
	my @unknown;

	# First request has checkboxes!
	if( defined(my $c = $CGI->param('base_url_count')) ) {
		for(0..($c-1)) {
			push @base_urls, $CGI->param( 'base_url' . $_ )
				if defined($CGI->param( 'base_url' . $_ ) and $CGI->param( 'base_url' . $_ ) eq 'yes' );
		}
	}

	$body->appendChild( $self->help( $CGI->msg( 'import.check.help' )));

	# Supremely inefficient ...
	foreach my $url (@ignore) {
		@base_urls = grep { $_ ne $url } @base_urls;
	}

	my %repos;
	foreach my $url (@base_urls)
	{
		if( grep { $_ eq $url } @added ) {
			$repos{$url} = ADDED;
		} elsif( grep { $_ eq $url } @failed ) {
			$repos{$url} = FAILED;
		} else {
			$repos{$url} = UNKNOWN;
			push @unknown, $url;
		}
	}

	my $stime = time();
	while( my $url = shift @unknown )
	{
		my $ha = HTTP::OAI::Harvester->new( baseURL => $url );
		$ha->agent( $dbh->adminEmail );
		$ha->timeout( 30 );
		my $id = $ha->Identify();
		
		if( $id->is_success ) {
			push @added, $url;
			$repos{$url} = ADDED;
			$self->_add_repository($url, $id);
		} else {
			push @failed, $url;
			$repos{$url} = FAILED;
		}

		# Stop if we took more than 60 seconds
		last if (time() - $stime) > 60;
	}

	$body->appendChild( my $form = dataElement( 'form', undef, {method=>'post', action=>$CGI->form_action} ));
	$form->appendChild( my $fs = dataElement( 'fieldset', undef, {class=>'input'} ));
	$fs->appendChild( dataElement( 'legend', $CGI->msg( 'import.check.legend' )));
	$fs->appendChild( my $table = dataElement( 'table', undef, {class=>'input'} ));
	foreach my $url (@unknown,@added,@failed) {
		$table->appendChild( my $tr = dataElement( 'tr', undef, {class=>'input'} ));
		$tr->appendChild( dataElement( 'td', dataElement( 'tt', $url ), {class=>'input'}));
		$tr->appendChild( dataElement( 'input', undef, {type=>'hidden',name=>'base_url',value=>$url} ));
		if( $repos{$url} == ADDED ) {
			$tr->appendChild( dataElement( 'input', undef, {type=>'hidden',name=>'added',value=>$url} ));
			$tr->appendChild( dataElement( 'td', chr(0x2713), {class=>'state passed'} ));
		} elsif( $repos{$url} == FAILED ) {
			$tr->appendChild( dataElement( 'input', undef, {type=>'hidden',name=>'failed',value=>$url} ));
			$tr->appendChild( dataElement( 'td', chr(0x2717), {class=>'state failed'} ));
		} else {
			$tr->appendChild( dataElement( 'td', '?', {class=>'state unknown'} ));
		}
	}

	if( @unknown > 0 ) {
		$fs->appendChild( dataElement( 'input', undef, {type=>'submit',name=>'submit',value=>$CGI->msg( 'import.add.submit' )} ));
	} else {
		$fs->appendChild( dataElement( 'p', $CGI->msg('import.done') ));
	}
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
	my $repo = Celestial::Repository->new({
		id => undef,
		identifier => $name,
		baseURL => $url,
		Identify => $identify->toDOM->toString,
		dbh=>$dbh,
	});

	return $dbh->addRepository( $repo );
}

1;
