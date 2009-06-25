package Celestial::DBI::Encapsulation;

use encoding 'utf8';
use vars qw($AUTOLOAD);

sub new {
	my $class = shift;
	if( @_ == 1 and ref($_[0]) eq 'HASH' ) {
		foreach(values %{$_[0]}) {
			utf8::decode($_); # Decode utf8 from the databsae
		}
		return bless({_elem => $_[0]}, $class);
	} else {
		return bless({_elem => {@_}}, $class);
	}
}

sub require {
	my $self = shift;
	for(@_) {
		unless(defined($self->{_elem}->{$_})) {
			Carp::confess("Requires argument: $_");
		}
	}
}

sub AUTOLOAD {
	my $self = shift;
	Carp::confess("Attempt to call object method [$AUTOLOAD] on class") unless ref($self);
	$AUTOLOAD =~ s/^.*:://;
	return if $AUTOLOAD eq 'DESTROY';
	$self->_elem($AUTOLOAD,@_);
}

sub prepare {
	Carp::confess( "prepare not allowed on Encapsulation" );
}
sub do {
	Carp::confess( "do not allowed on Encapsulation" );
}

sub asHash {
	my $self = shift;
	return %{$self->asHashRef};
}

sub asHashRef {
	my $self = shift;
	return $self->{_elem};
}

sub _elem {
	my $self = shift;
	my $name = shift;
	return @_ ? $self->{_elem}->{$name} = shift : $self->{_elem}->{$name};
}

1;
