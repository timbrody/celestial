package Celestial::Handler;

use vars qw( @ISA @EXPORT @EXPORT_OK );

use vars qw( @ORDER @NAVBAR $DEFAULT $dbh $dom );

use Exporter;
@ISA = qw(Exporter);
@EXPORT = qw( &dataElement &abbr_url &formElement &tableRowElement &urlElement );
@EXPORT_OK = qw( &dataElement &abbr_url &formElement &tableRowElement &urlElement );

@NAVBAR = @ORDER = qw();
$DEFAULT = undef;

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

sub dataElement {
	my( $name, $value, $attr ) = @_;
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
		$path =~ s/^(.{0,10}).*(\/[^\/]+)$/$1...$2/;
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
	$form->appendChild(my $fs = dataElement('fieldset',undef,{class => $opts{ legend } ? 'normal' : 'input'} ));
	if( $opts{ legend } ) {
		$fs->appendChild(dataElement('legend', $opts{ legend }));
	}

	while(my ($name, $value) = each %{$opts{hidden}}) {
		$fs->appendChild(dataElement('input', undef, {type=>'hidden',name=>$name,value=>$value}));
	}

	$fs->appendChild(my $table = dataElement('table',undef,{class=>'input'}));

	foreach my $field (@fields) {
		my $name = $field->{ name };
		$field->{ type } ||= 'text';
		$field->{ size } ||= 50;
		my $label = delete($field{ label }) || $CGI->msg( 'input.'.$name );
		$table->appendChild(my $tr = dataElement('tr',undef,{class=>'input'}));
		$tr->appendChild(my $td = dataElement('td',undef,{class=>'input'}));
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

sub help {
	my( $self, $CGI, $msg ) = @_;

	return dataElement( 'div', $msg, {class=>'help'});
}

sub no_auth {
	my( $self, $CGI ) = @_;

	return $self->error( $CGI, $CGI->msg( 'error.no_auth' ));
}

sub navbar {
	undef;
}

sub title {
	'';
}

sub body {
	undef;
}

1;
