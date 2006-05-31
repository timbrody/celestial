package Celestial::Handler;

use strict;
use warnings;

use Carp;

use vars qw( @ORDER @NAVBAR $DEFAULT $dbh $dom );

use vars qw( @ISA @EXPORT @EXPORT_OK );
use Exporter;
@ISA = qw(Exporter);
@EXPORT = qw( @ORDER $DEFAULT &dataElement &abbr_url &formElement &tableRowElement &urlElement );
@EXPORT_OK = qw( @ORDER $DEFAULT &dataElement &abbr_url &formElement &tableRowElement &urlElement );

sub new {
	my $class = shift;
	$dbh = shift or die "Requires dbh";
	$dom = shift or die "Requires dom";
	bless {
		dbh => $dbh,
		dom => $dom,
	}, $class;
}

sub dbh { shift->{dbh} }
sub dom { shift->{dom} }

sub page {
	my( $self, $CGI ) = @_;

	$dom->createInternalSubset( "html", "-//W3C//DTD XHTML 1.0 Strict//EN", "http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd" );

	$dom->setDocumentElement(my $doc = $dom->createElement('html'));

	$doc->appendChild(my $head = $dom->createElement('head'));
	$doc->appendChild(my $body = $dom->createElement('body'));

	$head->appendChild(dataElement( 'style', "\@import url('".$CGI->as_link('static/generic.css')."');", { type => 'text/css', media => 'screen' }));

	# Window title
	my $wtitle = $head->appendChild(dataElement( 'title', 'Celestial - ' ))->getFirstChild;

	# Body title
	my $ptitle = $body->appendChild(dataElement( 'h1', 'Celestial - '))->getFirstChild;

	# Top navigation bar
	$body->appendChild(my $topbar = dataElement( 'div', undef, {class=>'topbar'} ));
	$topbar->appendChild(my $navbar = dataElement( 'ul', undef, {class=>'navbar'} ));

	for (@ORDER) {
		my $c = "Celestial::Handler::$_";
		next unless $c->navbar($CGI);
		my $link = dataElement( 'a', $CGI->msg( "navbar.$_", $CGI->user ), { href=>$CGI->as_link( $_ ), class=>'navbar' });
		my $li = dataElement( 'li', $link, {class=>$_ eq $CGI->section ? 'navbar hilite' : 'navbar'} );
		$navbar->appendChild( $li ) if defined($li);
	}

	$wtitle->appendData( $self->title($CGI) );
	$ptitle->appendData( $self->title($CGI) );

	$body->appendChild( $self->body($CGI) );

	return $doc;
}

sub title {
	shift->msg( 'error.404' );
}

sub body {
	my( $self, $CGI ) = @_;

	return dataElement( 'div' );
}

sub navbar { 0 }

sub dataElement {
	my( $name, $value, $attr ) = @_;
	Carp::confess( "dom not defined" ) unless $dom;
	$attr ||= {};
	my $node = $dom->createElement($name);
	if( defined($value) ) {
		if( ref($value) =~ /^XML::/ ) {
			$node->appendChild( $value);
		} elsif( ref($value) or length($value) > 0 ) {
			$node->appendText($value);
		}
	}
	while(my( $key, $value ) = each %$attr ) {
		$node->setAttribute( $key, $value );
	}
	return $node;
}

sub abbr_url {
	my $uri = URI->new( shift );
	my $maxlen = shift || 50;
	if( length($uri) > $maxlen ) {
		my $path = $uri->path;
		my $m = length($uri)-$maxlen;
		$m = 4 if $m < 4;
		$path =~ s/^.{4,$m}(.*\/[^\/]+)$/...$1/;
		$uri->path($path);
	}
	return $uri;
}

sub urlElement
{
	my $url = shift;
	return dataElement( 'a', dataElement( 'tt', abbr_url($url,shift) ), {href=>$url});
}

sub tableRowElement
{
	my( $key, $value, $attr ) = @_;

	my $tr = dataElement( 'tr', undef, $attr );
	$tr->appendChild( dataElement( 'td', $key, $attr ));
	$tr->appendChild( dataElement( 'td', $value, $attr ));
	return $tr;
}

sub formElement
{
	my( $CGI, %opts ) = @_;
	my @fields = @{$opts{fields}};
	$opts{ method } ||= 'get';
	$opts{ action } ||= $CGI->form_action;
	$opts{ hidden } ||= {};

	my $form = dataElement('form',undef,{method=>'get',action=>$CGI->form_action});
	$form->appendChild(my $fs = dataElement('fieldset',undef,{class => $opts{ legend } ? 'input' : 'nolegend'} ));
	if( $opts{ legend } ) {
		$fs->appendChild(dataElement('legend', $opts{ legend }));
	}
	if( $opts{ note } ) {
		$fs->appendChild( dataElement( 'p', $opts{ note }, {class => 'input'}));
	}

	while(my ($name, $value) = each %{$opts{hidden}}) {
		$fs->appendChild(dataElement('input', undef, {type=>'hidden',name=>$name,value=>$value}));
	}

	$fs->appendChild(my $table = dataElement('table',undef,{class=>'input'}));

	foreach my $field (@fields) {
		my $name = $field->{ name };
		$field->{ type } ||= 'text';
		$field->{ size } ||= 50;
		my $label = delete($field->{ label }) || $CGI->msg( 'input.'.$name );
		$table->appendChild(my $tr = dataElement('tr',undef,{class=>'input'}));
		$tr->appendChild(my $td = dataElement('td',undef,{class=>'label'}));
		$td->appendChild(dataElement('label',$label,{'for'=>$name}));
		$tr->appendChild($td = dataElement('td',undef,{class=>'input'}));
		$td->appendChild(dataElement('input',undef,{%$field,id=>$name}));
	}

	if( my $submit = $opts{ submit } ) {
		$submit->{ 'type' } ||= 'submit';
		$fs->appendChild(dataElement( 'input', undef, $submit ));
	}

	return $form;
}

sub error {
	my( $self, $CGI, $msg ) = @_;

	return dataElement( 'div', $msg, {class=>'error'});
}

sub notice {
	my( $self, $CGI, $msg ) = @_;

	return dataElement( 'div', $msg, {class=>'error'});
}

sub help {
	my( $self, $CGI, $msg ) = @_;

	return dataElement( 'div', $msg, {class=>'help'});
}

sub no_auth {
	my( $self, $CGI ) = @_;

	return $self->error( $CGI, $CGI->msg( 'error.no_auth' ));
}

1;
