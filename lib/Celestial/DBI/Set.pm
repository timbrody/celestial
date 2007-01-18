package Celestial::DBI::Set;

use vars qw(@ISA);
@ISA = qw(Celestial::DBI::Encapsulation);

sub new {
	my $self = shift->SUPER::new(@_);
	$self->require(qw( dbh id ));
	$self;
}

1;
