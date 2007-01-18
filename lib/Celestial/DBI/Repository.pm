=head1 NAME

Celestial::DBI::Repository - Encapsulates a repository

=head1 SYNOPSIS

	my $repo = $dbh->getRepository($id);

	$mdf = new HTTP::OAI::MetadataFormat(...);
	$mdf = $repo->addMetadataFormat($mdf);

	$mdf = $repo->getMetadataFormat('oai_dc');
	@mdfs = $repo->listMetadataFormats();

=head1 METHODS

=over 4

=item $repo->identifier([identifier])

Return and optionally set the given field.

=cut

package Celestial::DBI::Repository;

use vars qw(@ISA @COLUMNS %SETS_SCHEMA $MDF_SCHEMA );
@ISA = qw(Celestial::DBI::Encapsulation);

@COLUMNS = qw( id identifier baseURL harvestMethod harvestSets harvestFrequency fullHarvestFrequency Identify );

$SETS_SCHEMA{'Sets'} = <<EOS;
(
`id` INT(10) UNSIGNED NOT NULL AUTO_INCREMENT,
`setSpec` TINYTEXT NOT NULL,
`setName` TEXT,
PRIMARY KEY(`setSpec`(255)),
UNIQUE(`id`)
)
EOS
$SETS_SCHEMA{'SetDescriptions'} = <<EOS;
(
`set` INT(10) UNSIGNED NOT NULL,
`description` LONGBLOB NOT NULL,
KEY(`set`)
)
EOS
$SETS_SCHEMA{'SetMemberships'} = <<EOS;
(
`set` INT(10) UNSIGNED NOT NULL,
`record` INT(10) UNSIGNED NOT NULL,
PRIMARY KEY(`set`,`record`),
KEY(`record`,`set`)
)
EOS

$MDF_SCHEMA = <<EOS;
CREATE TABLE MetadataFormats (
	`id` INT UNSIGNED NOT NULL,
	`repository` INT UNSIGNED NOT NULL,
	`metadataPrefix` VARCHAR(64) NOT NULL,
	`schema` TINYTEXT,
	`metadataNamespace` TINYTEXT,
	`lastHarvest` DATETIME,
	`lastFullHarvest` DATETIME,
	`lastAttempt` DATETIME,
	`lastToken` TEXT,
	`lastFulltextHarvest` DATETIME,
	`cardinality` INT UNSIGNED,
	PRIMARY KEY(`repository`,`metadataPrefix`),
	UNIQUE(`id`)
);
EOS

sub new {
	my $self = shift->SUPER::new(@_);
	$self->require(qw( dbh identifier baseURL ));
	if( defined($self->id) ) {
		foreach my $type (keys %SETS_SCHEMA) {
			my $fn = lc($type) . '_table';
			my $tblname = $type . "_" . $self->id;
			$self->$fn($tblname);
		}
	}
	$self;
}

sub create_tables {
	my $self = shift;
	my $dbh = $self->dbh;
	while(my( $type, $schema ) = each %SETS_SCHEMA) {
		my $tblname = $type . "_" . $self->id;
		unless( $dbh->table_exists( $tblname )) {
			$dbh->do(sprintf('CREATE TABLE `%s` %s',
				$tblname,
				$schema
			)) or Carp::confess( "Error creating table [$tblname]: $!" );
		}
	}
}

sub commit {
	my $self = shift;
	$self->dbh->updateRepository( $self );
}

sub remove
{
	my( $self ) = @_;
	my $dbh = $self->dbh;
	my $id = $self->id;

	$dbh->lock($id);
	$_->remove for $self->listMetadataFormats;
	while(my( $type, $schema ) = each %SETS_SCHEMA) {
		my $tblname = $type . "_" . $self->id;
		$dbh->do("DROP TABLE IF EXISTS $tblname");
	}
	$dbh->do("DELETE FROM Repositories WHERE `id`=?",{},$id);
	$dbh->unlock($id);
}

sub lock {
	shift->dbh->lock(@_);
}

sub unlock {
	shift->dbh->unlock(@_);
}

sub getLock {
	my $self = shift;
	$self->dbh->getLock($self,@_);
}

sub addReport {
	my $self = shift;
	my $rec = shift;
	$rec->{repository} = $self->id;
	return Celestial::DBI::Report->addReport( $self->dbh, $rec );
}

sub getReport {
	my $self = shift;
	return Celestial::DBI::Report->getReport( $self->dbh, $self, @_ );
}

sub listReports {
	my $self = shift;
	return Celestial::DBI::Report->listReports( $self->dbh, $self );
}

sub removeReport {
	my $self = shift;
	return Celestial::DBI::Report->removeReport( $self->dbh, $self, @_ );
}

sub addMetadataFormat {
	my $self = shift;
	return $self->dbh->addMetadataFormat($self,@_);
}

sub getMetadataFormat {
	my $self = shift;
	return $self->dbh->getMetadataFormat($self,@_);
}

sub listMetadataFormats {
	my $self = shift;
	return $self->dbh->listMetadataFormats($self);
}

sub resetFormat {
	my ($self,$mdf) = @_;
	$self->dbh->resetFormat($mdf);
}

sub disableFormat {
	my ($self,$mdf) = @_;
	$self->dbh->disableFormat($mdf);
}

sub enableFormat {
	my ($self,$mdf) = @_;
	$self->dbh->enableFormat($self,$mdf);
}

sub addSet($$) {
	my( $self, $set ) = @_;
	my $dbh = $self->dbh;

	my $id;
	my $tblname = $self->sets_table;

	my $sth = $dbh->prepare("SELECT `id` FROM $tblname WHERE `setSpec`=?");
	$sth->execute($set->setSpec) or Carp::confess( "Error adding set: $!" );
	($id) = $sth->fetchrow_array;
	$sth->finish;

	if( defined($id) ) {
		$sth = $dbh->prepare("UPDATE $tblname SET `setName`=? WHERE `id`=?");
		$sth->execute($set->setName, $id)
			or Carp::confess( "Error updating set: $!" );
	} else {
		$sth = $dbh->prepare("INSERT $tblname (`setSpec`,`setName`) VALUES (?,?)");
		$sth->execute($set->setSpec,$set->setName)
			or Carp::confess( "Error adding set: $!" );
		$id = $sth->{mysql_insertid};
		unless( defined($id) ) {
			Carp::confess( "Error getting id for new set: $!" );
		}
	}
	$sth->finish;

	$self->addSetDescriptions($id,$set->setDescription);

	return $id;
}

sub addSetDescriptions($$@) {
	my ($self, $id, @descriptions) = @_;
	my $dbh = $self->dbh;

	my $tblname = $self->setdescriptions_table;

	$dbh->do("DELETE FROM $tblname WHERE `set`=?",{},$id)
		or die $!;
	my $sth = $dbh->prepare("INSERT INTO $tblname (`set`,`description`) VALUES (?,?)");
	for( @descriptions ) {
		$sth->execute($id,$_->dom->toString) or die $!;
	}
}

sub addSetMembership($$$)
{
	my ($self, $setid, $recid) = @_;

	my $tblname = $self->setmemberships_table;

	$self->dbh->do("REPLACE $tblname (`set`,`record`) VALUES (?,?)",{},
		$setid,
		$recid,
	) or die $!;
}

sub getSetId($$) {
	my( $self, $setSpec ) = @_;

	my $tblname = $self->sets_table;

	my $sth = $self->dbh->prepare("SELECT `id` FROM $tblname WHERE `setSpec`=?");
	$sth->execute($setSpec) or die $!;
	my ($id) = $sth->fetchrow_array or return undef;
	$sth->finish;
	return $id;
}

sub listSetIds($$) {
	my ($self, $set) = @_;
	$set =~ s/\%/\_/sg; # Make sure we don't get a %...% query
	
	my $tblname = $self->sets_table or die "Sets table name isn't defined";
	my @ids;
	
	my $sth = $self->dbh->prepare("SELECT `id` FROM $tblname WHERE setSpec=? OR setSpec like ?");
	$sth->execute($set,$set . ':%') or die $!;
	while( my ($id) = $sth->fetchrow_array ) {
		push @ids, $id;
	}
	$sth->finish;
	
	return @ids;
}

1;
