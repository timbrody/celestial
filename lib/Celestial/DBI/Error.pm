package Celestial::DBI::Error;

use overload "<>" => \&_next;

use vars qw(@ISA @FIELDS);
@ISA = qw(Celestial::DBI::Encapsulation);

@FIELDS = qw(metadataFormat datestamp url error errorResponse);

sub new
{
	my $self = shift->SUPER::new(@_);
	$self->require(qw( dbh metadataFormat ));
	$self;
}

sub DESTROY
{
	my $self = shift;
	$self->_sth->finish if defined($self->_sth);
}

sub _next
{
	my $self = shift;
	my $row = $self->_sth->fetchrow_hashref or return;
	return Celestial::Error->new({
		%$row,
		dbh=>$self->dbh,
		metadataFormat=>$self->metadataFormat
	});
}

1;
