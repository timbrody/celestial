package Celestial::DBI;

=pod

=head1 NAME

Celestial::DBI - Abstract interface to database

=head1 DESCRIPTION

This module provides an object-based interface to the Celestial database (MySQL).

=head2 Deleting Records

There is no facility for removing single records, instead you should addRecord with record with the "deleted" flag set with the same OAI identifier.

=head1 SYNOPSIS

	use HTTP::OAI::MetadataFormat;
	use HTTP::OAI::Record;
	use Celestial::DBI;

	$dbh = Celestial::DBI->connect("/etc/celestial.conf")
		or die $!;

	# Creating/getting a new repository handle
	$repo = $dbh->addRepository(
	    $identifier,$baseURL,
	    $harvestMethod,$harvestSets,$harvestFrequency,$fullHarvestFrequency,
	    $Identify);
	$repo = $dbh->getRepository($dbh->getRepositoryId($identifier));

	# Creating/getting a new metadata format handle
	$mdf = new HTTP::OAI::MetadataFormat(...);
	    $mdf->metadataPrefix('oai_dc');
	    $mdf->schema('http://www.openarchives.org/OAI/2.0/oai_dc.xsd');
	    $mdf->metadataNamespace('http://www.openarchives.org/OAI/2.0/oai_dc/');
	$mdf = $repo->addMetadataFormat($mdf);
	$mdf = $repo->getMetadataFormat('oai_dc');

	# Adding a new record
	$rec = new HTTP::OAI::Record(...);
	$mdf->addRecord($rec);

	# Removing a repository
	$repo->remove;

=over 4

=cut

use strict;
use warnings;

use Celestial::Config;

use POSIX qw/strftime/;
use DBI;
use XML::LibXML;
use Encode;

use HTTP::OAI::Record;
use HTTP::OAI::Set;

use Carp;

use vars qw($AUTOLOAD $DB_MAX_ERROR_SIZE $DATE_FORMAT);

$DB_MAX_ERROR_SIZE = 2**15; # 32k

$DATE_FORMAT = '%Y%m%d%H%i%S';

=pod

=item $dbh = Celestial::DBI->new()

Returns a handle to a new Celestial::DBI object (use connect instead)

=cut

sub new {
	my $class = shift;
	return bless {
		_parser=>XML::LibXML->new(),
	}, $class;
}

=pod

=item $dbh = Celestial::DBI->connect([$cfg_file],[DBI connect args])

Given a single argument it is treated as the name of a Celestial config file, multiple arguments are passed to DBI to make a connection to a database. Returns a new Celestial::DBI object.

=cut

sub connect {
	my $class = shift;
	my $self = $class->new();
	my $db = $Celestial::Config::SETTINGS
		or die "Unable to get database settings";
	my $user = $db->{ username };
	my $pw = $db->{ password };
	my @opts;
	for(qw( database host port )) {
		next unless exists( $db->{ $_ });
		push @opts, join( '=', $_ => $db->{ $_ });
	}
	my $dsn = "dbi:mysql:" . join(';', @opts);
	$self->dbh(DBI->connect($dsn, $user, $pw))
		or return undef;
	return $self;
}

=pod

=item $int_dbh = $dbh->dbh([$int_dbh])

Return and optionally set the DBI object.

=cut

sub dbh {
	my $self = shift;
	return @_ ? $self->{_dbh} = shift : $self->{_dbh};
}

sub parser { $_[0]->{_parser} }

# Cache SQL queries
#sub prepare {
#	my ($self, $SQL) = @_;
#	return $self->dbh->prepare_cached($SQL);
#}

sub now {
	return strftime("%Y%m%d%H%M%S",gmtime);
}

=pod

=item ds = $dbh->datestamp($ds)

Formats a timestamp yyyymmddHHMMSS to yyyy-mm-ddTHH:MM:SSZ.

=cut

sub datestamp {
	return
		shift =~ /(\d{4})(\d\d)(\d\d)(\d\d)(\d\d)(\d\d)/ ? 
		"$1-$2-$3T$4:$5:$6Z" :
		undef;
}

=pod

=item $dbh->ping()

Perform a null query to the database, to either keep the connection alive or to re-start the connection.

=cut

sub ping {
	my $self = shift;
	eval { $self->do("SELECT 1") };
	return $@;
}

=pod

=item $oai = $dbh->timestamp($date)

Returns an OAI datestamp from a timestamp in format yyyymmddHHMMSS.

=cut

sub timestamp {
	my $ts = shift || return undef;
	if( $ts =~ /^(\d{4})\D?(\d{2})\D?(\d{2})\D?(\d{2})\D?(\d{2})\D?(\d{2})/ ) {
		return "$1-$2-$3T$4:$5:$6Z";
	} else {
		die "Cannot extract timestamp from $ts";
	}
}

# !!!This may stop DBI from closing its connections!!!
#sub DESTROY {
#	my $self = shift;
#	$self->dbh->DESTROY(@_) if $self->dbh;
#}

sub AUTOLOAD {
	my $self = shift;
	$AUTOLOAD =~ s/^.*:://;
#	warn "${self}::$AUTOLOAD(".join(',',@_).")\n";
	return if $AUTOLOAD eq 'DESTROY';
	my @r;
	eval { @r = ($self->dbh->$AUTOLOAD(@_)) };
	Carp::confess $self->dbh->errstr if $self->dbh->{RaiseError} and $self->dbh->err;
	return wantarray ? @r : $r[0];
}

=pod

=item $dbh->lock($repo_id)

Attempt to lock a repository $repo_id, returns 1 on success or undef on failure (already locked).

=cut

sub lock {
	my ($self, $id, $timestamp) = @_;
	$self->do("LOCK TABLES Locks WRITE");
	my $sth = $self->prepare("SELECT `repository` FROM Locks WHERE `repository`=?");
	$sth->execute($id) or croak $!;
	if( $sth->fetch ) {
		$sth->finish;
		$self->do("UNLOCK TABLES");
		return undef;
	}
   	$sth = $self->prepare("INSERT INTO Locks (`repository`,`timestamp`) VALUES(?,?)");
   	$sth->execute($id,$timestamp) or croak $!;
	$self->do("UNLOCK TABLES");
   	return 1;
}

=pod

=item $dbh->unlock($repo_id)

Unlocks a repository $repo_id, regardless of whether it was already locked.

=cut

sub unlock {
	my ($self, $id) = @_;
   	$self->do("DELETE FROM Locks WHERE `repository`=?",{},$id)
   		or Carp::confess $!;
}

=item $dbh->getLock($repo)

Returns the timestamp of the lock (if locked).

=cut

sub getLock($$)
{
	my( $dbh, $repo ) = @_;
	my $sth = $dbh->prepare("SELECT DATE_FORMAT(`timestamp`,$DATE_FORMAT) FROM Locks WHERE `repository`=?");
	$sth->execute($repo->id) or Carp::confess $!;
	my ($ts) = $sth->fetchrow_array;
	return $ts;
}

=pod

=item $dbh->table_exists($name)

Returns true if a table $name exists in the database.

=cut

sub table_exists {
	my ($self,$name) = @_;
	Carp::confess "table_exists: Table names must contain only [a-zA-Z0-9_]" if $name =~ /[^a-zA-Z0-9_]/;
	local $self->dbh->{PrintError} = 0;
	local $self->dbh->{RaiseError} = 0;
	my $rc = $self->dbh->do("DESCRIBE `$name`");
	return defined($rc) ? 1 : 0;
}

=pod

=item $dbh->cardinality($name)

Returns the size of table $name

=cut

sub cardinality {
	my ($self,$name) = @_;
	my $sth = $self->prepare("SELECT COUNT(*) FROM `$name`");
	$sth->execute() or Carp::confess $!;
	my ($c) = $sth->fetchrow_array();
	$sth->finish;
	return $c;
}

sub listConfigs {
	return qw( adminEmail repositoryName maxHarvesters );
}

sub _config {
	my ($self,$name) = (shift,shift);
	my $value;
	if( @_ ) {
		$value = shift;
		$self->do("DELETE FROM `Configuration` WHERE `type`=?",{},$name);
		my $sth = $self->prepare("INSERT INTO `Configuration` (`type`,`value`) VALUES (?,?)");
		$sth->execute($name, $value);
	} else {
		my $sth = $self->prepare("SELECT value FROM `Configuration` WHERE `type`=? LIMIT 1");
		$sth->execute($name);
		($value) = $sth->fetchrow_array;
		$sth->finish;
		utf8::decode($value) if defined($value);
	}
	return $value;
}

=pod

=item $adminEmail = $dbh->adminEmail([$adminEmail])
=item $name = $dbh->repositoryName([$name])
=item $max = $dbh->maxHarvesters([$max])

Return and optionally set the configuration options.

=cut

sub adminEmail { shift->_config('adminEmail',@_) }
sub repositoryName { shift->_config('repositoryName',@_) }
sub maxHarvesters { shift->_config('maxHarvesters',@_) }
sub mailHost { shift->_config('mailHost',@_) }

=pod

=item $id = $dbh->getRecordId($mdf,$identifier)

Gets the record id of the $identifier for metadata format $mdf.

=cut

sub getRecordId {
	my ($self,$mdf,$identifier) = @_;
	my $sth = $self->prepare("SELECT `id` FROM ".$mdf->table." WHERE `identifier`=?");
	$sth->execute($identifier);
	my ($id) = $sth->fetchrow_array;
	$sth->finish;
	return $id;
}

sub getRecordIds {
	my ($self,%args) = @_;
	my $repo = $args{-repository};
	my $identifier = $args{-identifier} or die "Requires -identifier";
	my $mdf = $args{-metadataFormat};
	my @ids;
	if( $repo ) {
		my @mdfs = $self->listMetadataFormats($repo);
		for (@mdfs) {
			my $sth = $self->prepare("SELECT id FROM ".$_->table." WHERE identifier=? LIMIT 1");
			$sth->execute($identifier);
			if( my ($id) = $sth->fetchrow_array ) {
				push @ids, $id;
			}
		}
	} elsif( $mdf ) {
		my $sth = $self->prepare("SELECT id FROM ".$mdf->table." WHERE identifier=? LIMIT 1");
		$sth->execute($identifier);
		if( my ($id) = $sth->fetchrow_array ) {
			push @ids, $id;
		}
	} else {
		die "Requires either -repository or -metadataFormat";
	}
	return @ids;
}

=item $repo = $dbh->getRepository($id)

Get a new repository object using its id.

=cut

sub getRepository($$) {
	my( $self, $id ) = @_;
	my $sth = $self->prepare("SELECT * FROM Repositories WHERE `id`=?");
	$sth->execute( $id );
	my $row = $sth->fetchrow_hashref or return;
	return Celestial::Repository->new({
		%$row,
		dbh => $self,
	});
}

=item $id = $dbh->getRepositoryId($identifier)

Get a repository id using its identifier.

=cut

sub getRepositoryId($$) {
	my ($self,$identifier) = @_;
	my $sth = $self->prepare("SELECT id FROM Repositories WHERE `identifier`=?");
	$sth->execute($identifier);
	my ($id) = $sth->fetchrow_array() or return undef;
	$sth->finish;
	return $id;
}

=item $id = $dbh->getRepositoryBaseURL($baseURL)

Get a repository id using its baseURL.

=cut

sub getRepositoryBaseURL($$) {
	my ($self,$baseURL) = @_;
	my $sth = $self->prepare("SELECT id FROM Repositories WHERE `baseURL`=?");
	$sth->execute($baseURL);
	my ($id) = $sth->fetchrow_array() or return undef;
	$sth->finish;
	return $id;
}

=item @repos = $dbh->listRepositories()

Return a list of all the repositories.

=cut

sub listRepositories($) {
	my $self = shift;
	my $sth = $self->prepare("SELECT * FROM Repositories ORDER BY identifier");
	$sth->execute;
	my @REPOS;
	while( my $row = $sth->fetchrow_hashref ) {
		push @REPOS, Celestial::Repository->new({
			%$row,
			dbh=>$self,
		});
	}
	$sth->finish;
	return @REPOS;
}

=item $repo = $dbh->updateRepository($repo)

Update the repository $repo.

=cut

sub updateRepository($$) {
	my( $self, $repo ) = @_;
	$self->do("LOCK TABLES Repositories WRITE");
	$self->do("DELETE FROM Repositories WHERE `id`=" . $repo->id );
	$self->addRepository( $repo );
	$self->do("UNLOCK TABLES");

	return $self->getRepository($repo->id);
}

=item $repo = $dbh->addRepository($repo)

Add a new repository $repo to the database.

=cut

sub addRepository($$) {
	my( $self, $repo ) = @_;

	my $sth = $self->prepare(my $sql = sprintf('INSERT INTO Repositories (%s) VALUES (%s)',
		join(',',map{"`$_`"} @Celestial::Repository::COLUMNS),
		join(',',map{'?'} @Celestial::Repository::COLUMNS)
	));
	$sth->execute(map { $repo->$_ } @Celestial::Repository::COLUMNS)
		or die "$sql: $!";
	if( !defined($repo->id) ) {
		$repo = $self->getRepository($self->dbh->{mysql_insertid});
	}
	$sth->finish;

	# Auxillary tables
	$repo->create_tables;
	
	return $repo;
}

sub updateIdentify_old {
	my ($self,$repo,$Identify) = @_;
	my $sth = $self->prepare("UPDATE Repositories SET Identify=? WHERE id=?");
	$sth->execute($Identify,$repo->id);
	$sth->finish;
}

sub listMetadataFormats {
	my ($self, $repo) = @_;
	my $cols = join(',',
		map({ "`$_`" } @Celestial::MetadataFormat::COLUMNS),
		map({ "DATE_FORMAT(`$_`,'$DATE_FORMAT') as `$_`" } @Celestial::MetadataFormat::DATE_COLUMNS));
	my $sth = $self->prepare("SELECT $cols FROM MetadataFormats WHERE `repository`=?");
	$sth->execute($repo->id);
	my @mdfs;
	while( my $row = $sth->fetchrow_hashref ) {
		push(@mdfs, Celestial::MetadataFormat->new({
			%$row,
			dbh=>$self,
			repository=>$repo
		}));
	}
	$sth->finish;
	return @mdfs;
}

sub getMetadataFormat {
	my ($self, $repo, $mdp) = @_;
	my $sth;
	my $cols = join(',',
		map({ "`$_`" } @Celestial::MetadataFormat::COLUMNS),
		map({ "DATE_FORMAT(`$_`,'$DATE_FORMAT') as `$_`" } @Celestial::MetadataFormat::DATE_COLUMNS));
	if( defined($mdp) and $repo->isa('Celestial::Repository') ) {
		$sth = $self->prepare("SELECT $cols FROM MetadataFormats WHERE `repository`=? AND `metadataPrefix`=?");
		$sth->execute($repo->id, $mdp) or Carp::confess("Error getting metadata table for $mdp: $!");
	} elsif( $repo !~ /\D/ ) {
		$sth = $self->prepare("SELECT $cols FROM MetadataFormats WHERE `id`=$repo");
		$sth->execute or Carp::confess("Error getting metadata table for $mdp: $!");
	} else {
		Carp::confess("Invalid arguments: Requires either repository and prefix or metadata format id\n");
	}
	my $row = $sth->fetchrow_hashref or return;
	# Get the repository object
	if( !defined($mdp) or !ref($repo) ) {
		$repo = $self->getRepository( $row->{ 'repository' } );
	}
	my $mdf = Celestial::MetadataFormat->new({
		%$row,
		dbh=>$self,
		repository=>$repo,
	});
	$sth->finish;
	return $mdf;
}

sub updateMetadataFormat {
	my ($self, $mdf) = @_;
	$self->do("UPDATE MetadataFormats SET `metadataPrefix`=?, `schema`=?, `metadataNamespace`=? WHERE `id`=?",{},
		$mdf->metadataPrefix,
		$mdf->schema,
		$mdf->metadataNamespace,
		$mdf->id,
	);
	return $mdf;
}

=item $dbh->addMetadataFormat( $repo, $mdf )

Where $repo isa L<Celestial::Repository> and $mdf isa L<HTTP::OAI::MetadataFormat>.

=cut

sub addMetadataFormat {
	my ($self, $repo, $mdf) = @_;
	if( defined(my $omdf = $repo->getMetadataFormat( $mdf->metadataPrefix )) ) {
		return $omdf;
	}
	my $sth = $self->prepare("REPLACE MetadataFormats (`id`,`repository`,`metadataPrefix`,`schema`,`metadataNamespace`) VALUES (?,?,?,?,?)");
	$sth->execute(
		undef,
		$repo->id,
		$mdf->metadataPrefix,
		$mdf->schema,
		$mdf->metadataNamespace
	) or Carp::confess("MySQL Error: $!");
	my $id = $sth->{'mysql_insertid'};
	$mdf = $repo->getMetadataFormat( $id );

	# Create auxillary tables
	$mdf->create_tables;

	return $mdf;
}

sub _resetFormat {
	my ($self,$mdf) = @_;
	$self->do("DELETE FROM Status WHERE id=".$mdf->id);
	$self->do("DELETE FROM ".$mdf->table) if $self->table_exists($mdf->table);
}

sub _disableFormat {
	my ($self,$mdf) = @_;
	$self->resetFormat($mdf);
	$self->do("UPDATE MetadataFormats SET id=null WHERE id=".$mdf->id);
	$self->do("DROP TABLE IF EXISTS ".$mdf->table);
}

sub _enableFormat {
	my ($self,$repo,$mdf) = @_;
	Carp::confess "Usage: \$dbh->enableFormat(\$repo,\$mdf)\n" unless $mdf;
	$self->do("DELETE FROM MetadataFormats WHERE repository=? AND metadataPrefix=?",{},$repo->id,$mdf->metadataPrefix);
	return $repo->addMetadataFormat($mdf);
}

sub getFulltext {
	my( $self, $mdf ) = @_;
	return Celestial::Fulltext->new({
		dbh=>$self,
		repository=>$mdf->repository,
		id=>$mdf->id,
		lastFulltextHarvest=>$self->_status( $mdf->id, 'lastFullHarvest' ),
	});
}

# Internal set/get values from the MetadataFormats table (status part)

sub _status
{
	my( $self, $id, $key, $value ) = @_;
	if( @_ == 4 ) {
		$self->do("UPDATE MetadataFormats SET `$key`=? WHERE `id`=?",{},$value,$id)
			or Carp::confess("$key => $value: $!");
	} else {
		my $cols = scalar(grep { $_ eq $key } @Celestial::MetadataFormat::DATE_COLUMNS) ?
			"DATE_FORMAT(`$key`,'$DATE_FORMAT')" :
			"`$key`";
		my $sth = $self->prepare("SELECT $cols FROM MetadataFormats WHERE `id`=?");
		$sth->execute( $id )
			or Carp::confess("$key => $value: $!");
		($value) = $sth->fetchrow_array;
		$sth->finish;
	}
	return $value;
}

=item $ts = $dbh->lastHarvest($mdf,[ts])

Return an optionally set the lastHarvest for metadata format $mdf.

Returns the date in 'yyyymmddhhmmss' format.

=cut

sub lastHarvest($@) {
	my ($self,$mdf) = (shift,shift);
	if( @_ and !defined($self->lastHarvest($mdf)) ) {
		$self->_status($mdf->id, 'lastFullHarvest', @_);
	}
	return $self->_status( $mdf->id, 'lastHarvest', @_ );
}

sub setCardinality
{
	my( $self, $mdf ) = @_;
	return $self->_status( $mdf->id, 'cardinality', @_ );
}

=item $ts = $dbh->lastAttempt($mdf,[ $ts ])

Return and optionally set the lastAttempt datestamp.

Returns the date in yyyymmdd format.

=cut

sub lastAttempt
{
	my( $self, $mdf ) = splice(@_,0,2);
	return $self->_status( $mdf->id, 'lastAttempt', @_ );
}

=item $ts = $dbh->lastToken($mdf,[ $token ])

Return and optionally set the last attempted resumption token.

=cut

sub lastToken
{
	my( $self, $mdf ) = splice(@_,0,2);
	return $self->_status( $mdf->id, 'lastToken', @_ );
}

=item $ts = $dbh->lastFulltextHarvest($ft,[ $ts ])

Return and optionally set the lastFulltextHarvest datestamp.

Returns the date in yyyymmdd format.

=cut

sub lastFulltextHarvest
{
	my( $self, $mdf ) = splice(@_,0,2);
	return $self->_status( $mdf->id, 'lastFulltextHarvest', @_ );
}


sub addRepositoryError {
	my ($self,$repo,$url,$error,$response) = @_;
	for ($self->listMetadataFormats($repo)) {
		$self->addError($_,$url,$error,$response);
	}
}

sub addError {
	my ($self,$mdf,$url,$error,$response) = @_;
	$self->ping;
	$self->do("DELETE FROM harvestLog WHERE `metadataFormat`=? AND `datestamp` < NOW() - INTERVAL 1 MONTH",{},$mdf->id);
	my $sth = $self->prepare("INSERT INTO harvestLog VALUES (?,null,?,?,?)");
	$sth->execute($mdf->id,$url,$error,substr($response,0,$DB_MAX_ERROR_SIZE));
	$sth->finish;
}

sub listErrors($$)
{
	my( $dbh, $mdf ) = @_;
	my $sth = $dbh->prepare("SELECT *,DATE_FORMAT(`datestamp`,'$DATE_FORMAT') AS datestamp FROM harvestLog WHERE `metadataFormat`=?");
	$sth->execute($mdf->id) or Carp::confess $!;
	return Celestial::Error->new({
		dbh=>$dbh,
		metadataFormat=>$mdf,
		_sth=>$sth,
	});
}

sub addRecord($$$) {
	my( $self, $mdf, $rec ) = @_;
	$self->addProvenance($mdf, $rec);
	$self->updateRecord($mdf, $rec);
}

sub addProvenance($$$) {
	my ($self, $mdf, $rec ) = @_;
	my $repo = $mdf->repository;

	my $dom = XML::LibXML->createDocument('1.0','UTF-8');
	$dom->setDocumentElement(my $prov = $dom->createElementNS('http://www.openarchives.org/OAI/2.0/provenance','provenance'));
#	$prov->setAttribute('xmlns','http://www.openarchives.org/OAI/2.0/provenance');
	$prov->setAttribute('xmlns:xsi','http://www.w3.org/2001/XMLSchema-instance');
	$prov->setAttribute('xsi:schemaLocation','http://www.openarchives.org/OAI/2.0/provenance http://www.openarchives.org/OAI/2.0/provenance.xsd');
	my $oriDesc = $prov->appendChild($dom->createElement('originDescription'));
	$oriDesc->setAttribute('altered','false');
	$oriDesc->setAttribute('harvestDate',timestamp($self->now));
	$oriDesc->appendChild($dom->createElement('baseURL'))->appendText($repo->baseURL) if $repo->baseURL;
	$oriDesc->appendChild($dom->createElement('identifier'))->appendText($rec->identifier) if $rec->identifier;
	$oriDesc->appendChild($dom->createElement('datestamp'))->appendText($rec->datestamp) if $rec->datestamp;
	$oriDesc->appendChild($dom->createElement('metadataNamespace'))->appendText($mdf->metadataNamespace) if $mdf->metadataNamespace;
	$rec->about(HTTP::OAI::Metadata->new(dom=>$dom));
}

sub updateRecord($$$)
{
	my ($self, $mdf, $rec ) = @_;
	
	if( !$rec->datestamp ) {
		$rec->datestamp($self->datestamp($self->now()));
	}

	return updateMetadata(@_);
}

sub updateMetadata($$$)
{
	my( $self, $mdf, $rec ) = @_;
	my $repo = $mdf->repository;
	my $tblname = $mdf->table;

	my ($hd,$md,$ab) = ('','','');
	$hd = $rec->header->dom->toString;
	$md = $rec->metadata->toString if defined($rec->metadata);
	if( $rec->about ) {
		my $dom = XML::LibXML->createDocument('1.0','UTF-8');
		$dom->setDocumentElement(my $root = $dom->createElement('about'));
		$root->appendChild($_) for( map {
				my $node = $_->dom->getDocumentElement->cloneNode(1);
				$dom->adoptNode($node);
				$node
			} $rec->about );
		$ab = $dom->toString;
	}

	$self->do("LOCK TABLES $tblname WRITE");

	my $sth = $self->prepare("SELECT `id`,`accession` FROM $tblname WHERE `identifier`=?");
	$sth->execute($rec->identifier) or die $!;
	my( $id, $accession ) = $sth->fetchrow_array;

	# Remove the existing record
	if( defined($id) ) {
		$self->do("DELETE FROM $tblname WHERE `identifier`=?",{},
			$rec->identifier
		);
	} else {
		$accession = $rec->datestamp;
	}

	$sth = $self->prepare("REPLACE $tblname (`id`,`datestamp`,`accession`,`identifier`,`header`,`metadata`,`about`) VALUES (?,NOW(),?,?,?,?,?)");
	$sth->execute($id,$accession,$rec->identifier,$hd,$md,$ab);
	$sth->finish;
	$id = $sth->{'mysql_insertid'} unless defined($id);

	# Update the cursor
	$self->do("UPDATE $tblname SET `datestamp`=`datestamp`, `cursor`=CONCAT(DATE_FORMAT(`datestamp`,'\%Y\%m\%d\%H\%i\%S'),LPAD(MOD(`id`,1000),3,'0')) WHERE `id`=?",{},$id);

	$self->do("UNLOCK TABLES");

	# Process sets
	for ($rec->header->setSpec) {
		if( defined(my $set = $repo->getSetId($_)) ) {
			$repo->addSetMembership($set,$id);
		}
	}

	return $id;
}

sub updateHeader {
	my ($self, %args) = @_;
	Carp::confess "updateHeader deprecated";
}

=item @rows = $dbh->listIdsByIdentifier( $identifier )

Returns an array of array refs containing the repository id, metadata format id and record id that match $identifier.

=cut

sub listIdsByIdentifier {
	my( $self, $identifier ) = @_;

	my $sth = $self->prepare("SELECT `repository`,`id` FROM MetadataFormats");
	$sth->execute() or die $!;
	my @matches;
	while( my $row = $sth->fetchrow_arrayref ) {
		my $tblname = sprintf('Records_%d_%d', @$row);
		next unless $self->table_exists( $tblname );
		my $h = $self->prepare("SELECT `id` FROM $tblname WHERE `identifier`=?");
		$h->execute($identifier) or die $!;
		my( $id ) = $h->fetchrow_array or next;
		push @matches, [@$row,$id];
	}
	$sth->finish;
	return @matches;
}

sub getDomContent {
	my( $self, $dom ) = @_;
	my @docs;
	foreach my $node ($dom->documentElement->getChildNodes) {
		next unless $node->nodeType == XML_ELEMENT_NODE;
		my $doc = XML::LibXML->createDocument( '1.0', 'UTF-8' );
		$doc->setDocumentElement($node);
		push @docs, $doc;
	}
	return @docs;
}

sub getRecord {
	my ($self, $mdf, $id, %opts) = @_;
	my $parser = $self->parser;

	my $sth = $self->prepare("SELECT `header`,`metadata`,`about` FROM ".$mdf->table." WHERE `id`=?");
	$sth->execute($id) or die $!;
	my $ary = $sth->fetchrow_arrayref or return;

	my $rec = new HTTP::OAI::Record(version=>2.0, %opts);
	$rec->header(new HTTP::OAI::Header(dom=>$parser->parse_string($$ary[0])));
	if( $$ary[1] ) {
		my ($dom) = $self->getDomContent($parser->parse_string($$ary[1]));
		$rec->metadata(new HTTP::OAI::Metadata(dom=>$dom)) if $dom;
	}
	if( $$ary[2] ) {
		for($self->getDomContent($parser->parse_string($$ary[2]))) {
			$rec->about(new HTTP::OAI::Metadata(dom=>$_));
		}
	}
	$sth->finish;

	return $rec;
}

sub getHeader {
	my ($self, $repo, $id) = @_;

	my $sth = $self->prepare("SELECT header FROM ".$repo->table." WHERE id=? LIMIT 1");
	$sth->execute($id);
	my $ary = $sth->fetchrow_arrayref or die "Record $id doesn't exist";
	$sth->finish;

	return HTTP::OAI::Header
			->new(-version=>2.0)
			->parse($$ary[0]);
}

sub getRecordAccession {
	my( $self, $mdf, $id ) = @_;

	my $sth = $self->prepare("SELECT DATE_FORMAT(`accession`,'$DATE_FORMAT') FROM ".$mdf->table." WHERE `id`=$id");
	$sth->execute() or die $!;
	my $ary = $sth->fetchrow_arrayref;
	$sth->finish;
	unless( $ary ) {
		warn "Record $id (".$mdf->table.") doesn't exist";
		return;
	}

	return $ary->[0];
}

=back

=head1 AUTHOR

Tim Brody <tdb01r@ecs.soton.ac.uk>

=cut

#
#	Encapsulation
#
# Encapsulation simply encapsulates a data structure in
# an OO wrapper.
#

package Celestial::Encapsulation;

use Carp qw( confess );
use vars qw($AUTOLOAD);

sub new {
	my $class = shift;
	if( @_ == 1 and ref($_[0]) eq 'HASH' ) {
		for(values %{$_[0]}) {
			utf8::decode($_) if defined($_);
		}
		return bless({_elem => $_[0]}, $class);
	} else {
		return bless({_elem => {}}, $class);
	}
}

sub require {
	my $self = shift;
	for(@_) {
		unless(defined($self->{_elem}->{$_})) {
			confess("Requires argument: $_");
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

=pod

=head1 NAME

Celestial::Repository - Encapsulates a repository

=head1 SYNOPSIS

	my $repo = $dbh->getRepository($id);

	$mdf = new HTTP::OAI::MetadataFormat(...);
	$mdf = $repo->addMetadataFormat($mdf);

	$mdf = $repo->getMetadataFormat('oai_dc');
	@mdfs = $repo->listMetadataFormats();

=cut

package Celestial::Repository;

use vars qw(@ISA @COLUMNS %SETS_SCHEMA $MDF_SCHEMA );
@ISA = qw(Celestial::Encapsulation);

@COLUMNS = qw( id identifier baseURL harvestMethod harvestSets harvestFrequency fullHarvestFrequency Identify );

$SETS_SCHEMA{'Sets'} = <<EOS;
(
`id` INT(10) UNSIGNED NOT NULL AUTO_INCREMENT,
`setSpec` TINYTEXT NOT NULL,
`setName` TEXT,
PRIMARY KEY(`setSpec`(64)),
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
	foreach my $type (keys %SETS_SCHEMA) {
		my $fn = lc($type) . '_table';
		my $tblname = $type . "_" . $self->id;
		$self->$fn($tblname);
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
	shift->dbh->getLock(@_);
}

sub addReport {
	my $self = shift;
	my $rec = shift;
	$rec->{repository} = $self->id;
	return Celestial::Report->addReport( $self->dbh, $rec );
}

sub getReport {
	my $self = shift;
	return Celestial::Report->getReport( $self->dbh, $self, @_ );
}

sub listReports {
	my $self = shift;
	return Celestial::Report->listReports( $self->dbh, $self );
}

sub removeReport {
	my $self = shift;
	return Celestial::Report->removeReport( $self->dbh, $self, @_ );
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

=head1 NAME

Celestial::MetadataFormat - Encapsulates a metadata format

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

package Celestial::MetadataFormat;

use vars qw(@ISA $TABLE_SCHEMA @COLUMNS @DATE_COLUMNS);
@ISA = qw(Celestial::Encapsulation);

@COLUMNS = qw( id repository metadataPrefix schema metadataNamespace lastToken cardinality );
@DATE_COLUMNS = qw( lastHarvest lastFullHarvest lastAttempt lastFulltextHarvest );

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

package Celestial::Set;

use vars qw(@ISA);
@ISA = qw(Celestial::Encapsulation);

sub new {
	my $self = shift->SUPER::new(@_);
	$self->require(qw( dbh id ));
	$self;
}

=head1 NAME

Celestial::Fulltext

=head1 DESCRIPTION

Represents a Fulltext table that contains all of the full-text URLs and formats for a repository. The id of the Fulltext table is the id of the oai_dc metadata format (although the full-text may not actually be linked from DC).

=head1 METHODS

=over 4

=cut

package Celestial::Fulltext;

use vars qw(@ISA $TABLE_SCHEMA @COLUMNS @DATE_COLUMNS);
@ISA = qw(Celestial::Encapsulation);

$TABLE_SCHEMA = "
(
`record` INT UNSIGNED NOT NULL,
`datestamp` DATETIME NOT NULL,
`url` VARCHAR(255) NOT NULL,
`mimetype` VARCHAR(64) NOT NULL,
`puid` VARCHAR(64),
`format` VARCHAR(255) NOT NULL,
PRIMARY KEY(`record`,`url`,`format`)
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

sub removeFulltext {
	my( $self, $id ) = @_;

	$self->dbh->do("DELETE FROM ".$self->table." WHERE `record`=?", {}, $id)
}

sub lastHarvest {
	my $self = shift;
	return $self->dbh->lastFulltextHarvest($self, @_);
}

=back

=head1 NAME

Celestial::Report

=head1 METHODS

=over 4

=cut

package Celestial::Report;

use vars qw(@ISA @FIELDS);
@ISA = qw(Celestial::Encapsulation);

@FIELDS = qw(repository email confirmed frequency previous include);

sub new
{
	my $self = shift->SUPER::new(@_);
	$self->require(qw( dbh repository email frequency include ));
	$self;
}

sub addReport
{
	my( $self, $dbh, $rec ) = @_;
	$dbh->do("REPLACE Report (".join(',',@FIELDS).") VALUES (".join(',',map {'?'} @FIELDS).")", {},
		@$rec{@FIELDS}
	) or Carp::confess($!);
}

sub removeReport
{
	my( $self, $dbh, $repo, $email ) = @_;
	$dbh->do("DELETE FROM Report WHERE repository=? AND email=?",{},$repo->id,$email) or Carp::confess($!);
}

sub getReport
{
	my( $self, $dbh, $repo, $email ) = @_;
	my $sth = $dbh->prepare("SELECT ".join(',',@FIELDS).',DATE_FORMAT(previous,"%Y%m%d%H%i%s") AS previous FROM Report WHERE repository=? AND email=?');
	$sth->execute($repo->id,$email) or Carp::confess($!);
	my $row = $sth->fetchrow_hashref or return;
	$sth->finish;
	return Celestial::Report->new({
		%$row,
		dbh=>$dbh,
		repository=>$repo,
	});
}

sub listReports
{
	my( $self, $dbh, $repo ) = @_;
	my @reps;
	my $sth = $dbh->prepare("SELECT ".join(',',@FIELDS).',DATE_FORMAT(previous,"%Y%m%d%H%i%s") AS previous FROM Report WHERE repository=?');
	$sth->execute($repo->id) or Carp::confess($!);
	while( my $row = $sth->fetchrow_hashref ) 
	{
		push @reps, Celestial::Report->new({
				%$row,
				dbh=>$dbh,
				repository=>$repo,
				});
	}
	$sth->finish;
	return @reps;
}

sub isDue
{
	my $self = shift;
	my $sth = $self->dbh->prepare("SELECT 1 FROM Report WHERE repository=? AND email=? AND confirmed is not Null AND (previous is Null OR previous + INTERVAL frequency DAY <= NOW())");
	$sth->execute( $self->repository->id, $self->email );
	my ($r) = $sth->fetchrow_array;
	$sth->finish;
	return $r;
}

sub touch
{
	my $self = shift;
	$self->dbh->do("UPDATE Report SET previous=NOW() WHERE repository=? AND email=?",{},$self->repository->id,$self->email);
}

sub reset
{
	my $self = shift;
	$self->dbh->do("UPDATE Report SET previous=Null WHERE repository=? AND email=?",{},$self->repository->id,$self->email);
}

sub confirm
{
	my $self = shift;
	$self->dbh->do("UPDATE Report SET confirmed='' WHERE repository=? AND email=?",{},$self->repository->id,$self->email);
}

=item $rep->recordsReport

Returns a hash of metadata formats and total records since previous report.

=cut

sub recordsReport
{
	my $self = shift;
	my $dbh = $self->dbh;
	my $repo = $self->repository;
	my %recs;
	my $from = $self->previous || 0;
	foreach my $mdf ($repo->listMetadataFormats)
	{
		my $sth = $dbh->prepare("SELECT COUNT(*) FROM ".$mdf->table." WHERE datestamp>=?");
		$sth->execute($from) or Carp::confess($!);
		my $row = $sth->fetchrow_arrayref;
		$recs{$mdf->metadataPrefix} = $row->[0];
		$sth->finish;
	}
	return %recs;
}

sub errorsReport
{
	my $self = shift;
	my $dbh = $self->dbh;
	my $repo = $self->repository;
	my %recs;
	my $from = $self->previous || 0;
	foreach my $mdf ($repo->listMetadataFormats)
	{
		my $sth = $dbh->prepare("SELECT CONCAT_WS(' ',datestamp,error) FROM harvestLog WHERE id=? AND datestamp>=?");
		$sth->execute($mdf->id, $from) or Carp::confess($!);
		while( my $row = $sth->fetchrow_arrayref )
		{
			push @{$recs{$mdf->metadataPrefix}}, $row->[0];
		}
		$sth->finish;
	}
	return %recs;
}

sub fulltextsReport
{
	my $self = shift;
	my $dbh = $self->dbh;
	my $repo = $self->repository;
	my %fmts;
	my $from = $self->previous || 0;
	my $mdf = $repo->getMetadataFormat('oai_dc') or return;
	my $ftt = $mdf->getFulltext or return;
	my $sth = $dbh->prepare("SELECT format,COUNT(*) FROM ".$ftt->table." WHERE datestamp>=? GROUP BY format");
	$sth->execute($from) or Carp::confess($!);
	while( my $row = $sth->fetchrow_arrayref )
	{
		$fmts{$row->[0]} = $row->[1];
	}
	$sth->finish;
	return %fmts;
}

=back

=cut

package Celestial::Error;

use overload "<>" => \&_next;

use vars qw(@ISA @FIELDS);
@ISA = qw(Celestial::Encapsulation);

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
