=head1 NAME

Celestial::DBI::Report

=head1 METHODS

=over 4

=cut

package Celestial::DBI::Report;

use vars qw(@ISA @COLUMNS @DATE_COLUMNS);
@ISA = qw(Celestial::DBI::Encapsulation);

@COLUMNS = qw(repository email confirmed frequency include);
@DATE_COLUMNS = qw(previous);

sub new
{
	my $self = shift->SUPER::new(@_);
	$self->require(qw( dbh repository email frequency include ));
	$self;
}

sub addReport
{
	my( $self, $dbh, $rec ) = @_;
	$rec->{repository} = $rec->{repository}->id
		if ref($rec->{repository});
	$dbh->do("REPLACE Reports (".join(',',@COLUMNS,@DATE_COLUMNS).") VALUES (".join(',',map {'?'} @COLUMNS,@DATE_COLUMNS).")", {},
		@$rec{@COLUMNS,@DATE_COLUMNS}
	) or Carp::confess($!);
}

sub removeReport
{
	my( $self, $dbh, $repo, $email ) = @_;
	$dbh->do("DELETE FROM Reports WHERE repository=? AND email=?",{},$repo->id,$email) or Carp::confess($!);
}

sub getReport
{
	my( $self, $dbh, $repo, $email ) = @_;
	my $sth = $dbh->prepare("SELECT ".join(',',@COLUMNS).",DATE_FORMAT(`previous`,'$Celestial::DBI::DATE_FORMAT') AS previous FROM Reports WHERE `repository`=? AND `email`=?");
	$sth->execute($repo->id,$email) or Carp::confess($!);
	my $row = $sth->fetchrow_hashref or return;
	$sth->finish;
	return Celestial::DBI::Report->new({
		%$row,
		dbh=>$dbh,
		repository=>$repo,
	});
}

sub listReports
{
	my( $self, $dbh, $repo ) = @_;
	my @reps;
	my $sth = $dbh->prepare("SELECT ".join(',',@COLUMNS).",DATE_FORMAT(`previous`,'$Celestial::DBI::DATE_FORMAT') AS previous FROM Reports WHERE `repository`=?");
	$sth->execute($repo->id) or Carp::confess($!);
	while( my $row = $sth->fetchrow_hashref ) 
	{
		push @reps, Celestial::DBI::Report->new({
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
	my $sth = $self->dbh->prepare("SELECT 1 FROM Reports WHERE repository=? AND email=? AND confirmed is not Null AND (previous is Null OR previous + INTERVAL frequency DAY <= NOW())");
	$sth->execute( $self->repository->id, $self->email );
	my ($r) = $sth->fetchrow_array;
	$sth->finish;
	return $r;
}

sub touch
{
	my $self = shift;
	$self->dbh->do("UPDATE Reports SET previous=NOW() WHERE repository=? AND email=?",{},$self->repository->id,$self->email);
}

sub reset
{
	my $self = shift;
	$self->dbh->do("UPDATE Reports SET previous=Null WHERE repository=? AND email=?",{},$self->repository->id,$self->email);
}

sub confirm
{
	my $self = shift;
	$self->dbh->do("UPDATE Reports SET confirmed='' WHERE repository=? AND email=?",{},$self->repository->id,$self->email);
}

sub commit
{
	my $self = shift;
	my $rec;
	for(@COLUMNS,@DATE_COLUMNS) {
		$rec->{ $_ } = $self->$_;
	}
	$self->addReport($self->dbh, $rec);
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
		# We already have a record count
		if( $from == 0 ) {
			$recs{$mdf->metadataPrefix} = $mdf->cardinality;
			next;
		}
		my $sth = $dbh->prepare("SELECT COUNT(*) FROM ".$mdf->table." WHERE `datestamp`>=?");
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
		my $sth = $dbh->prepare("SELECT CONCAT_WS(' ',`datestamp`,`error`) FROM harvestLog WHERE `metadataFormat`=? AND `datestamp`>=?");
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
	my $sth = $dbh->prepare("SELECT `format`,COUNT(*) FROM ".$ftt->table." WHERE `datestamp`>=? GROUP BY `format`");
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

1;
