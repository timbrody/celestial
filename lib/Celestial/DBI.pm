package Celestial::DBI;

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
use encoding 'utf8';

use Celestial::Config;

use Celestial::DBI::Encapsulation;
use Celestial::DBI::Error;
use Celestial::DBI::Fulltext;
use Celestial::DBI::MetadataFormat;
use Celestial::DBI::Report;
use Celestial::DBI::Repository;
use Celestial::DBI::Set;

use POSIX qw/strftime/;
use DBI;
use XML::LibXML;
use Encode;
use Net::SMTP;

use HTTP::OAI;

use Carp;

use vars qw($AUTOLOAD $errstr );

our $DB_MAX_ERROR_SIZE = 2**15; # 32k
our $DB_MAX_FIELD_SIZE = .5 * 1024 * 1024; # 512k of XML ...

our $DATE_FORMAT = '%Y%m%d%H%i%S';

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
	my $user = $self->{_username} = $db->{ username };
	my $pw = $self->{_password} = $db->{ password };
	my @opts;
	for(qw( database host port )) {
		next unless exists( $db->{ $_ });
		push @opts, join( '=', $_ => $db->{ $_ });
	}
	my $dsn = $self->{_dsn} = "dbi:mysql:" . join(';', @opts);
	unless( $self->dbh(DBI->connect($dsn, $user, $pw, {
		PrintError => 1,
		RaiseError => 0,
	})) ) {
		$errstr = $DBI::errstr;
		return undef;
	}
	return $self;
}

sub reconnect {
	my $self = shift;
	# Don't overwrite the existing handle in case we need the error message
	# that triggered this reconnect
	my $dbh = DBI->connect($self->{_dsn}, $self->{_username}, $self->{_password})
		or return undef;
	return $self->dbh( $dbh );
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

sub DESTROY {}

sub AUTOLOAD {
	my $self = shift;
	my $dbh = $self->dbh;
	$AUTOLOAD =~ s/^.*:://;
#	warn "${self}::$AUTOLOAD(".join(',',@_).")\n";
	RETRY:
	$dbh->{RaiseError} = 0;
	my @r = $dbh->$AUTOLOAD(@_);
	if( defined $dbh->err ) {
		if( $dbh->errstr =~ /MySQL server has gone away/ ) {
			if( $self->reconnect ) {
				goto RETRY;
			}
		}
		Carp::confess "Database error: " . $dbh->errstr;
	}
	return wantarray ? @r : $r[0];
}

=pod

=item $dbh->lock( $repo_id [, $timestamp] )

Attempt to lock a repository $repo_id optionally using $timestamp, returns 1 on success or undef on failure (already locked).

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
	my $sth = $dbh->prepare("SELECT DATE_FORMAT(`timestamp`,'$DATE_FORMAT') FROM Locks WHERE `repository`=?");
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

=item $dbh->cardinality($name)

Returns the number of records in table $name

=cut

sub cardinality {
	my ($self,$name) = @_;
	my $sth = $self->prepare("SELECT COUNT(*) FROM `$name`");
	$sth->execute() or Carp::confess $!;
	my ($c) = $sth->fetchrow_array();
	$sth->finish;
	return $c;
}

=item $dbh->storage($name)

Returns the number of bytes used by table $name (as reported by MySQL's SHOW TABLE STATUS).

=cut

sub storage {
	my( $self, $name ) = @_;
	my $sth = $self->prepare("SHOW TABLE STATUS LIKE '$name'");
	$sth->execute;
	my $row = $sth->fetchrow_hashref;
	return $row->{Data_length};
}

sub listConfigs {
	return qw( adminEmail repositoryName maxHarvesters mailHost );
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

=item $identifier = $dbh->getRecordIdentifier( $mdf, $id )

Return the identifier for $id, or undef if it doesn't exist.

=cut

sub getRecordIdentifier
{
	my( $dbh, $mdf, $id ) = @_;

	my $sth = $dbh->prepare("SELECT `identifier` FROM ".$mdf->table." WHERE id=$id");
	$sth->execute;
	my( $identifier ) = $sth->fetchrow_array;
	return $identifier;
}

=item $repo = $dbh->getRepository($id)

Get a new repository object using its id.

=cut

sub getRepository($$) {
	my( $self, $id ) = @_;
	my $sth = $self->prepare("SELECT * FROM Repositories WHERE `id`=?");
	$sth->execute( $id );
	my $row = $sth->fetchrow_hashref or return;
	return Celestial::DBI::Repository->new({
		%$row,
		dbh => $self,
	});
}

=item $id = $dbh->getRepositoryId($identifier)

Get a repository id using its identifier.

=cut

sub getRepositoryId($$) {
	my ($self,$identifier) = @_;
	my $sth = $self->prepare("SELECT `id` FROM Repositories WHERE `identifier`=?");
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
		push @REPOS, Celestial::DBI::Repository->new({
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
		join(',',map{"`$_`"} @Celestial::DBI::Repository::COLUMNS),
		join(',',map{'?'} @Celestial::DBI::Repository::COLUMNS)
	));
	$sth->execute(map { $repo->$_ } @Celestial::DBI::Repository::COLUMNS)
		or die "$sql: $!";
	if( !defined($repo->id) ) {
		$repo = $self->getRepository($self->dbh->{mysql_insertid});
	}
	$sth->finish;

	# Auxillary tables
	$repo->create_tables;
	
	return $repo;
}

sub listMetadataFormats {
	my ($self, $repo) = @_;
	my $cols = join(',',
		map({ "`$_`" } @Celestial::DBI::MetadataFormat::COLUMNS),
		map({ "DATE_FORMAT(`$_`,'$DATE_FORMAT') as `$_`" } @Celestial::DBI::MetadataFormat::DATE_COLUMNS));
	my $sth = $self->prepare("SELECT $cols FROM MetadataFormats WHERE `repository`=?");
	$sth->execute($repo->id);
	my @mdfs;
	while( my $row = $sth->fetchrow_hashref ) {
		push(@mdfs, Celestial::DBI::MetadataFormat->new({
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
		map({ "`$_`" } @Celestial::DBI::MetadataFormat::COLUMNS),
		map({ "DATE_FORMAT(`$_`,'$DATE_FORMAT') as `$_`" } @Celestial::DBI::MetadataFormat::DATE_COLUMNS));
	if( defined($mdp) and $repo->isa('Celestial::DBI::Repository') ) {
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
	my $mdf = Celestial::DBI::MetadataFormat->new({
		%$row,
		dbh=>$self,
		repository=>$repo,
	});
	$sth->finish;
	return $mdf;
}

sub updateMetadataFormat {
	my ($self, $mdf) = @_;
	my @cols = (@Celestial::DBI::MetadataFormat::COLUMNS, @Celestial::DBI::MetadataFormat::DATE_COLUMNS);
	$self->do("REPLACE MetadataFormats (" .
		join(',',map({"`$_`"} @cols)) .
		") VALUES (" .
		join(',',map({'?'} @cols)) .
		")",{},
		map({ref($mdf->$_) ? $mdf->$_->id : $mdf->$_} @cols)
	) or die $!;
	return $mdf;
}

=item $dbh->addMetadataFormat( $repo, $mdf )

Where $repo isa L<Celestial::DBI::Repository> and $mdf isa L<HTTP::OAI::MetadataFormat>.

=cut

sub addMetadataFormat {
	my ($self, $repo, $mdf) = @_;
	if( defined(my $omdf = $repo->getMetadataFormat( $mdf->metadataPrefix )) ) {
		$omdf->create_tables; # Make sure tables have been created
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
	$mdf = $repo->getMetadataFormat( $mdf->metadataPrefix );

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
	return Celestial::DBI::Fulltext->new({
		dbh=>$self,
		repository=>$mdf->repository,
		id=>$mdf->id,
		lastFulltextHarvest=>$self->_status( $mdf->id, 'lastFullHarvest' ),
	});
}

sub listReportsByEmail {
	my( $dbh, $email ) = @_;
	my $cols = join(',',
		map({ "`$_`" } @Celestial::DBI::Report::COLUMNS),
		map({ "DATE_FORMAT(`$_`,'$DATE_FORMAT') as `$_`" } @Celestial::DBI::Report::DATE_COLUMNS));
	
	my $sth = $dbh->prepare( "SELECT $cols FROM Reports WHERE `email`=?" );
	$sth->execute( $email ) or Carp::confess $!;
	my @reps;
	while( my $row = $sth->fetchrow_hashref ) {
		push @reps, Celestial::DBI::Report->new({
			%$row,
			dbh => $dbh,
			repository => $dbh->getRepository( $row->{ repository } )
		});
	}
	return @reps;
}

=item $dbh->sendConfirmation( $report )

Checks whether the user has ever confirmed a report before, if so set this report has confirmed. Otherwise send a confirmation email.

=cut

sub sendConfirmation {
	my( $dbh, $cgi, $report ) = @_;
	return if $report->confirmed;

	my @reps = $dbh->listReportsByEmail( $report->email );
	my $confirmed;
	for( @reps ) {
		last if $confirmed = $_->confirmed;
	}

	if( $confirmed ) {
		$report->confirmed( 1 );
		$report->commit;
		return $confirmed;
	}

	my $host = $dbh->mailHost || 'localhost';
	my $smtp = Net::SMTP->new( $host );

	my $curl = $cgi->absolute_link($cgi->as_link( 'subscription',
		email => $report->email,
		action => 'confirm',
	));
	my $surl = $cgi->absolute_link($cgi->as_link( 'subscription',
		email => $report->email,
	));

	my $msg = "To: " . $report->email . "\n" .
		"Subject: " . $cgi->msg( 'report.confirm.subject' ) . "\n\n" .
		$cgi->msg( 'report.confirm.message', $report->email, $curl, $surl );
	$smtp->mail( $dbh->adminEmail );
	$smtp->to( $report->email );
	$smtp->data( encode("iso-8859-1", $msg ));

	$smtp->quit;

	return 0;
}

# Internal set/get values from the MetadataFormats table (status part)

sub _status
{
	my( $self, $id, $key, $value ) = @_;
	if( @_ == 4 ) {
		$self->do("UPDATE MetadataFormats SET `$key`=? WHERE `id`=?",{},$value,$id)
			or Carp::confess("$key => $value: $!");
	} else {
		my $cols = scalar(grep { $_ eq $key } @Celestial::DBI::MetadataFormat::DATE_COLUMNS) ?
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
	my( $self, $mdf, $c ) = @_;
	return $self->_status( $mdf->id, 'cardinality', $c );
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
	my $sth = $dbh->prepare("SELECT *,DATE_FORMAT(`datestamp`,'$DATE_FORMAT') AS `datestamp` FROM harvestLog WHERE `metadataFormat`=?");
	$sth->execute($mdf->id) or Carp::confess $!;
	return Celestial::DBI::Error->new({
		dbh=>$dbh,
		metadataFormat=>$mdf,
		_sth=>$sth,
	});
}

sub addRecord($$$) {
	my( $self, $mdf, $rec ) = @_;
	$self->addProvenance($mdf, $rec);
	return $self->updateRecord($mdf, $rec);
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

sub updateRecord
{
	my ($self, $mdf, $rec ) = @_;
	
	if( !$rec->datestamp ) {
		$rec->datestamp($self->datestamp($self->now()));
	}

	return updateMetadata(@_);
}

sub updateMetadata
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

	if(
		length($hd) > $DB_MAX_FIELD_SIZE or
		length($md) > $DB_MAX_FIELD_SIZE or
		length($ab) > $DB_MAX_FIELD_SIZE
		)
	{
		warn $repo->id . " " . $rec->identifier . " is larger than max allowed size ($DB_MAX_FIELD_SIZE)\n";
		return undef;
	}

	my( $id, $accession );
	eval {
	$self->do("LOCK TABLES $tblname WRITE");

	my $sth = $self->prepare("SELECT `id`,`accession` FROM $tblname WHERE `identifier`=?");
	$sth->execute($rec->identifier)
		or die "Error writing to $tblname: $!";
	( $id, $accession ) = $sth->fetchrow_array;

	# Remove the existing record
	if( defined($id) ) {
		$self->do("DELETE FROM $tblname WHERE `identifier`=?",{},
			$rec->identifier
		) or die "Error writing to $tblname: $!";
	} else {
		$accession = $rec->datestamp;
	}

	$sth = $self->prepare("REPLACE $tblname (`id`,`datestamp`,`accession`,`identifier`,`status`,`header`,`metadata`,`about`) VALUES (?,NOW(),?,?,?,COMPRESS(?),COMPRESS(?),COMPRESS(?))");
	$sth->execute($id,$accession,$rec->identifier,$rec->status,$hd,$md,$ab)
		or die "Error writing to $tblname: $!";
	$sth->finish;
	$id = $sth->{'mysql_insertid'} unless defined($id);

	# Update the cursor
	$self->do("UPDATE $tblname SET `datestamp`=`datestamp`, `cursor`=CONCAT(DATE_FORMAT(`datestamp`,'\%Y\%m\%d\%H\%i\%S'),LPAD(MOD(`id`,1000),3,'0')) WHERE `id`=?",{},$id)
		or die "Error writing to $tblname: $!";

	}; # End of Table Lock
	$self->do("UNLOCK TABLES");
	die $@ if $@;

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

	my $sth = $self->prepare("SELECT UNCOMPRESS(`header`),UNCOMPRESS(`metadata`),UNCOMPRESS(`about`) FROM ".$mdf->table." WHERE `id`=?");
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

	my $sth = $self->prepare("SELECT UNCOMPRESS(`header`) FROM ".$repo->table." WHERE `id`=$id LIMIT 1");
	$sth->execute();
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

1;
