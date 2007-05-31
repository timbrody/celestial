
=head1 NAME

Celestial::DBI::MetadataFormat - Encapsulates a metadata format

=head1 SYNOPSIS

	$mdf = $repo->getMetadataFormat('oai_dc');

	$rec = new HTTP::OAI::Record(...);

	$mdf->addProvenance($rec);
	$mdf->addRecord($rec);
	$rec = $mdf->getRecord($mdf->getRecordId('oai:smurf:0001'));

=head1 METHODS

=over 4

=item $mdf->metadataPrefix

=item $mdf->metadataNamespace

=item $mdf->schema

=cut

package Celestial::DBI::MetadataFormat;

use vars qw(@ISA $TABLE_SCHEMA @COLUMNS @DATE_COLUMNS);
@ISA = qw(Celestial::DBI::Encapsulation);

@COLUMNS = qw( id repository metadataPrefix schema metadataNamespace lastToken cardinality storage );
@DATE_COLUMNS = qw( locked lastHarvest lastFullHarvest lastAttempt lastFulltextHarvest );

$TABLE_SCHEMA = <<EOS;
(
  `id` INT(10) UNSIGNED NOT NULL AUTO_INCREMENT,
  `datestamp` DATETIME,
  `accession` DATETIME,
  `cursor` BIGINT UNSIGNED,
  `identifier` BLOB NOT NULL,
  `status` ENUM('deleted'),
  `header` LONGBLOB NOT NULL,
  `metadata` LONGBLOB,
  `about` LONGBLOB,
  PRIMARY KEY  (`id`),
  KEY (`cursor`),
  KEY `identifier` (`identifier`(128)),
  KEY (`accession`,`status`)
) TYPE=MyISAM;
EOS

sub new
{
	my $self = shift->SUPER::new(@_);
	$self->require(qw( dbh repository ));
	$self->table(sprintf('Records_%d_%d', $self->repository->id, $self->id))
		if defined($self->id);
	$self;
}

sub create_tables
{
	my $self = shift;
	Carp::confess("id must be set") unless defined($self->id);
	$self->table(sprintf('Records_%d_%d', $self->repository->id, $self->id));
	if( !$self->dbh->table_exists($self->table) ) {
		$self->dbh->do(sprintf('CREATE TABLE `%s` %s',$self->table,$TABLE_SCHEMA))
			or Carp::confess('Error creating table [' . $self->table . ']');
	}
}

sub commit
{
	my( $self ) = @_;
	$self->dbh->updateMetadataFormat($self);
}

sub remove
{
	my( $self ) = @_;
	my $dbh = $self->dbh;
	my $id = $self->id;

	$dbh->do("DELETE FROM MetadataFormats WHERE `id`=?",{},$id);
	$dbh->do("DELETE FROM harvestLog WHERE `metadataFormat`=?",{},$id);
	$dbh->do("DROP TABLE IF EXISTS ".$self->table);
}

sub lock
{
	my( $self ) = @_;
	$self->locked( $self->dbh->now );
	$self->commit;
}

sub unlock
{
	my( $self ) = @_;
	$self->locked( undef );
	$self->commit;
}

=item $mdf->reset()

Reset the harvest date (doesn't effect data).

=cut

sub reset
{
	my( $self ) = @_;
	my $dbh = $self->dbh;
	$self->removeAllErrors;
	$self->lastHarvest(undef);
	$self->lastFullHarvest(undef);
	$self->lastToken(undef);
	$self->commit;
}

=item $mdf->removeAllRecords()

Remove all data and reset the harvest date.

=cut

sub removeAllRecords
{
	my( $self ) = @_;
	$self->cardinality(0);
	$self->reset; # Force re-harvest of everything
	$self->dbh->do("DELETE FROM ".$self->table)
		or die $!;
}

sub removeAllErrors
{
	my( $self ) = @_;
	my $dbh = $self->dbh;
	$dbh->do("DELETE FROM harvestLog WHERE `metadataFormat`=?",{},$self->id);
}

=item $name = table([$name])

Get the table name for this metadata format.

=cut

sub addProvenance {
	my $self = shift;
	$self->dbh->addProvenance(
		repository=>$self->repository,
		metadataFormat=>$self,
		record=>shift,
	);
}

sub addRecord($$) {
	my $self = shift;
	$self->dbh->addRecord($self, shift);
}

sub updateRecord($$) {
	my $self = shift;
	$self->dbh->updateRecord($self, shift);
}

sub getRecord {
	my $self = shift;
	$self->dbh->getRecord($self,@_);
}

sub getRecordId {
	my $self = shift;
	$self->dbh->getRecordId($self,shift);
}

sub getRecordIdentifier {
	my $self = shift;
	$self->dbh->getRecordIdentifier($self,shift);
}

sub getRecordAccession {
	my $self = shift;
	$self->dbh->getRecordAccession($self,shift);
}

sub getFulltext {
	my $self = shift;
	return $self->dbh->getFulltext($self);
}

sub listErrors {
	my $self = shift;
	return $self->dbh->listErrors($self);
}

=back

=cut

1;
