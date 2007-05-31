=head1 NAME

Celestial::DBI::Fulltext

=head1 DESCRIPTION

Represents a Fulltext table that contains all of the full-text URLs and formats for a repository. The id of the Fulltext table is the id of the oai_dc metadata format (although the full-text may not actually be linked from DC).

=head1 METHODS

=over 4

=cut

package Celestial::DBI::Fulltext;

use vars qw(@ISA $TABLE_SCHEMA @COLUMNS @DATE_COLUMNS);
@ISA = qw(Celestial::DBI::Encapsulation);

$TABLE_SCHEMA = "
(
`record` INT UNSIGNED NOT NULL,
`datestamp` DATETIME NOT NULL,
`url` VARCHAR(255) NOT NULL,
`mimetype` VARCHAR(64) NOT NULL,
`puid` VARCHAR(64),
`format` VARCHAR(255) NOT NULL,
PRIMARY KEY(`record`,`format`,`url`)
)
";

@COLUMNS = qw( record url mimetype puid format );
@DATE_COLUMNS = qw( datestamp );

sub new {
	my $self = shift->SUPER::new(@_);
	$self->require(qw( dbh id ));
	$self->table("Fulltext_" . $self->id);
	$self->create_tables;
	$self;
}

sub create_tables
{
	my $self = shift;
	$self->table("Fulltext_" . $self->id);
	if( !$self->dbh->table_exists($self->table) ) {
		$self->dbh->do(sprintf('CREATE TABLE %s %s',
			$self->table,
			$TABLE_SCHEMA
		)) or Carp::confess "Error creating table ".$self->table.": $!";
	}
}

=item addFulltext($rec)

Add a Fulltext record using following fields ($rec is a hash ref):

	record	Record id
	datestamp	Last update to the full-text
	url	Full-text URL
	mimetype	Server defined mime-type
	puid	Pronom unique id
	format	English-language format description

=cut

sub addFulltext {
	my( $self, $rec ) = @_;
	my $dbh = $self->dbh;

	my @fields = qw( record datestamp url mimetype puid format );
	$dbh->do("REPLACE ".$self->table." (".join(',',@fields).") VALUES(".join(',',map{'?'}@fields).")",{},
		@$rec{@fields})
		or die "Error writing to ".$self->table.": $!";
}

sub hasFulltext {
	my( $self, $id ) = @_;
	my $dbh = $self->dbh;

	my $sth = $dbh->prepare("SELECT 1 FROM ".$self->table." WHERE `record`=$id LIMIT 1");
	$sth->execute or die $!;
	return $sth->fetch;
}

sub removeFulltext {
	my( $self, $id ) = @_;

	$self->dbh->do("DELETE FROM ".$self->table." WHERE `record`=?", {}, $id)
}

sub lastHarvest {
	my $self = shift;
	return $self->dbh->lastFulltextHarvest($self, @_);
}

sub reset {
	my $self = shift;
	$self->dbh->do("DELETE FROM ".$self->table);
}

=item $ftt->synchronize( $mdf )

Syrchronize our records with $mdf (i.e. remove any records that don't have an equivalent id in $mdf).

=cut

sub synchronize($$) {
	my( $self, $mdf ) = @_;

	my $table = $self->table;
	my $ref = $mdf->table;
	my $dbh = $self->dbh;

	$dbh->do("DROP TEMPORARY TABLE IF EXISTS _orphans");
	$dbh->do("CREATE TEMPORARY TABLE _orphans (`record` int unsigned not null, PRIMARY KEY(`record`))")
		or die("Unable to create temp table for Fulltext synchronization: $!");
	$dbh->do("INSERT INTO _orphans SELECT DISTINCT `record` FROM `$table` LEFT JOIN `$ref` ON `record`=`id` WHERE `id` is Null");
	$dbh->do("DELETE FROM `$table` WHERE `record`=(SELECT O.`record` FROM _orphans AS O WHERE `$table`.`record`=O.`record`)");
	$dbh->do("DROP TEMPORARY TABLE _orphans");
}

1;
