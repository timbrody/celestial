package Celestial::Handler::oai;

use strict;
use warnings;
use encoding 'utf8';

use Celestial::Handler;

use vars qw( @ISA );
@ISA = qw( Celestial::Handler );

push @ORDER, 'oai';

#use CGI qw/:standard -oldstyle_urls/;
#$CGI::USE_PARAM_SEMICOLONS = 0; # We really, really want ampersands
use HTML::Entities;
use XML::LibXML;
use Encode;

use HTTP::OAI;
use HTTP::OAI::Harvester;

sub page
{
	my( $self, $CGI ) = @_;
	my $dbh = $self->dbh;

	my $source = $CGI->section_path;
	my( $repo, $mdf );
	my $u = $CGI->url;
	$u =~ s/;/&/g;
	$u = URI->new( $u );
	my %vars = $u->query_form;
	for(values %vars) { # Ordinarily param() would unescape
		$_ = $CGI->uri_unescape($_);
	}

	if( $source ) {
		$source =~ s/^\///;
		$source = $CGI->uri_unescape($source);
		my $repoid;
		if( $source =~ /^https?:\/\// ) {
			$repoid = $dbh->getRepositoryBaseURL($source);
		} elsif( $source ne '' and $source !~ /[^0-9]/ ) {
			$repoid = 0 + $source;
		} else {
			$repoid = $dbh->getRepositoryId($source);
		}
		$repo = $dbh->getRepository($repoid) if defined($repoid);
	}

	unless( $repo ) {
		$CGI->not_found;
		return;
	}

	# Create the response object
	my $r = $vars{ verb } || '';
	unless( $r =~ /^(?:GetRecord|Identify|ListIdentifiers|ListMetadataFormats|ListRecords|ListSets)$/ ) {
		$r = "Response";
	}
	$r = "HTTP::OAI::$r";
	$r = $r->new(
		requestURL=>$CGI->url,
		xslt=>$CGI->as_link( 'static/celestial.xsl' ),
	);

	$r->errors( HTTP::OAI::Repository::validate_request(%vars) );

	if( my $set = $vars{set} ) {
		if( !$repo->listSetIds($set) ) {
			$r->errors(new HTTP::OAI::Error(
				code=>'noMatchingRecords',
				message=>'The specified set ("'.$vars{set}.'") does not exist.',
			));
		}
	}
	if( defined(my $mdp = $vars{metadataPrefix}) ) {
		unless( defined( $mdf = $repo->getMetadataFormat($mdp) ) ) {
			$r->errors(new HTTP::OAI::Error(code=>'cannotDisseminateFormat'));
		}
	}

	if( $vars{from} && $vars{from} =~ /^(\d{4}-\d{2}-\d{2})$/ ) {
		$vars{from} = "$1T00:00:00Z";
	}
	if( $vars{until} && $vars{until} =~ /^(\d{4}-\d{2}-\d{2})$/ ) {
		$vars{until} = "$1T23:59:59Z";
	}

	$vars{from} =~ s/\D//g if $vars{from};
	$vars{until} =~ s/\D//g if $vars{until};

	unless( $r->errors ) {
		my $f = $vars{ verb };
		$self->$f(
			$r,
			repository => $repo,
			metadataFormat => $mdf,
			baseURL => $CGI->absolute_link( $CGI->as_link( 'oai' ) . '/' . $CGI->uri_escape( $source )),
			args => \%vars,
		);
	}

	$CGI->content_type( 'text/xml; charset=utf-8' );
	binmode(STDOUT,":raw"); # toFH prints in binary
	$r->toDOM()->toFH(\*STDOUT,1);

	return undef;
}

sub GetRecord {
	my( $self, $r, %args ) = @_;
	my $dbh = $self->dbh;
	my %vars = %{$args{args}};
	my $repo = $args{ repository };
	my $mdf = $args{ metadataFormat };

	my $identifier = $vars{identifier};
	my $id = $mdf->getRecordId( $identifier );
	unless( defined($id) ) {
		for($repo->listMetadataFormats) {
			if( defined($_->getRecordId( $identifier ))) {
				$r->errors(new HTTP::OAI::Error(code=>'cannotDisseminateFormat'));
				return $r;
			}
		}
		$r->errors(new HTTP::OAI::Error(code=>'idDoesNotExist'));
		return $r;
	}

	$r->record( $mdf->getRecord( $id ));

	return $r;
}
sub Identify {
	my( $self, $r, %args ) = @_;
	my $dbh = $self->dbh;
	my %vars = %{$args{args}};
	my $repo = $args{ repository };
	my $mdf = $args{ metadataFormat };

	$r->deletedRecord('persistent');
	$r->earliestDatestamp('0001-01-01');
	$r->granularity('YYYY-MM-DDThh:mm:ssZ');
	my $requestURL = $r->requestURL;
	my $responseDate = $r->responseDate;

	if( $repo->Identify and length($repo->Identify) > 0 ) {
		$r->parse_string($repo->Identify);
	} else {
		$r->adminEmail('mailto:' . $dbh->adminEmail);
		$r->repositoryName($dbh->repositoryName);

		my $sth = $dbh->prepare("SELECT distinct baseURL FROM Repositories ORDER BY baseURL");
		$sth->execute;

		my $dom = XML::LibXML->createDocument('1.0','UTF-8');
		$dom->setDocumentElement(my $md = $dom->createElementNS('http://www.openarchives.org/OAI/2.0/friends/','friends'));
		$md->setAttribute('xmlns:xsi','http://www.w3.org/2001/XMLSchema-instance');
		$md->setAttribute('xsi:schemaLocation','http://www.openarchives.org/OAI/2.0/friends/ http://www.openarchives.org/OAI/2.0/friends.xsd');

		while( my ($baseURL) = $sth->fetchrow_array ) {
			$md->appendChild($dom->createElement('baseURL'))->appendText($baseURL);
		}
		$sth->finish;

		$r->description(new HTTP::OAI::Metadata(dom=>$dom));
	}

	# Reset to the correct data
	$r->responseDate($responseDate);
	$r->requestURL($requestURL);
	$r->baseURL($args{ baseURL });

	return $r;
}
sub ListIdentifiers {
	my( $self, $r, %args ) = @_;
	my $dbh = $self->dbh;
	my %vars = %{$args{args}};
	my $repo = $args{ repository };
	my $mdf = $args{ metadataFormat };

	my ($rid,$from,$until,$set,$start,$mdp);
	$rid = 0;
	$from = $vars{from} || '';
	$until = $vars{until} || '';
	$set = $vars{set} || '';
	$start = 0;
	$mdp = $vars{metadataPrefix} || '';

	if( exists($vars{resumptionToken}) ) {
		my $token = $vars{resumptionToken};
		my @args = decodeToken($token);
		($start,$from,$until,$mdp,$set) = @args;
		if( !$mdp ) {
			$r->errors(new HTTP::OAI::Error(code=>'badResumptionToken',message=>"metadataPrefix part of resumption token missing"));
		} elsif( !defined($mdf = $repo->getMetadataFormat( $mdp )) ) {
			$r->errors(new HTTP::OAI::Error(code=>'badResumptionToken',message=>"Invalid metadataPrefix part [$mdp] of resumption token [$token]"));
		} elsif( length($start) > 10 ) {
			$r->errors(new HTTP::OAI::Error(code=>'badResumptionToken',message=>"Starting offset (\"$start\") too long"));
		} elsif( $from && length($from) != 14 ) {
			$r->errors(new HTTP::OAI::Error(code=>'badResumptionToken',message=>"From date (\"$from\") not 14 digits long"));
		} elsif( $until && length($until) != 14 ) {
			$r->errors(new HTTP::OAI::Error(code=>'badResumptionToken',message=>"Until date (\"$until\") not 14 digits long"));
		}
		return $r if $r->errors;
	}

#	my @rids = sort { $a <=> $b } repositoryIds($dbh, $source, $mdp);

	my $SQL = "SELECT `id`,DATE_FORMAT(`datestamp`,'$Celestial::DBI::DATE_FORMAT'),`header` FROM " . $mdf->table . " ";
	my @LOGIC = ();
	my @VALUES = ();
	my @ORDER = ();

	# Manual join with repository table (coz MySQL table joins suck)
#	push(@LOGIC, '(' . join(' OR ', map { 'repository=?' } @rids) . ')');
#	push(@VALUES, @rids);
	if( $from ) {
		push(@LOGIC, " `cursor` >= CONCAT(?,LPAD(MOD(?,1000),3,'0')) ");
		$from =~ s/\D//g;
		push(@VALUES, $from, ($start < 0 ? 0 : $start));
	}
	if( $until ) {
		push(@LOGIC, " `cursor` <= CONCAT(?,'999') ");
		$until =~ s/\D//g;
		push(@VALUES, $until);
	}
	if( $set ) {
		my @setIds = $repo->listSetIds($set);
		$SQL .= ", " . $repo->setmemberships_table . " AS sm ";
		push(@LOGIC, " `id`=sm.`record` ");
		push(@LOGIC, '(' . join(' OR ', map { 'sm.`set`=?' } @setIds ) . ')');
		push(@VALUES, @setIds);
	}
	if( @LOGIC ) {
		$SQL .= " WHERE " . join(' AND ', @LOGIC);
	}
	$SQL .= " ORDER BY `cursor` ASC LIMIT 501";
#warn "Executing (" . join(',', @VALUES), "):\n$SQL\n";
	my $sth = $dbh->prepare($SQL);
	$sth->execute(@VALUES) or die "Error executing $SQL: $!";

	my ($UID,$datestamp,$header);
	$sth->bind_columns(\$UID,\$datestamp,\$header);
	my $c = 0;

	my @UIDS;

	my $parser = XML::LibXML->new;
	while( $sth->fetch ) {
		if( ++$c == 501 ) {
			$r->resumptionToken(new HTTP::OAI::ResumptionToken(resumptionToken=>encodeToken($UID,$datestamp,$until,$mdp,$set)));
			last;
		}

		$r->identifier(new HTTP::OAI::Header(dom=>$parser->parse_string($header)));
	}

	$sth->finish;

	if( !$r->identifier ) {
		if( exists($vars{resumptionToken}) ) {
			$r->errors(new HTTP::OAI::Error(
				code=>'badResumptionToken',
				message=>'No records match the resumption token',
			));
			$r->errors(new HTTP::OAI::Error(
				code=>'noRecordsMatch',
				message=>'No records match the resumption token',
			));
		} else {
			$r->errors(new HTTP::OAI::Error(
				code=>'noRecordsMatch'
			));
		}
	} elsif( !$r->resumptionToken && exists($vars{resumptionToken}) ) {
		$r->resumptionToken(new HTTP::OAI::ResumptionToken);
	}

	return $r;
}
sub ListMetadataFormats {
	my( $self, $r, %args ) = @_;
	my $dbh = $self->dbh;
	my %vars = %{$args{args}};
	my $repo = $args{ repository };
	my $mdf = $args{ metadataFormat };

	my $identifier = $vars{identifier};
	my @mdfs = $repo->listMetadataFormats;

	if( defined($identifier) ) {
		@mdfs = grep { defined($_->getRecordId( $identifier )) } @mdfs;
	}

	for (@mdfs) {
		$r->metadataFormat(
			HTTP::OAI::MetadataFormat->new(
				metadataPrefix=>$_->metadataPrefix,
				schema=>$_->schema,
				metadataNamespace=>$_->metadataNamespace
		));
	}

	if( !$r->metadataFormat ) {
		if( @mdfs ) {
			$r->errors(new HTTP::OAI::Error(code=>'noMetadataFormats'));
		} else {
			$r->errors(new HTTP::OAI::Error(code=>'idDoesNotExist'));
		}
	}

	return $r;
}
sub ListRecords {
	my( $self, $r, %args ) = @_;
	my $dbh = $self->dbh;
	my %vars = %{$args{args}};
	my $repo = $args{ repository };
	my $mdf = $args{ metadataFormat };

	my ($rid,$from,$until,$set,$start,$mdp);
	$rid = 0;
	$from = $vars{'from'} || '';
	$until = $vars{'until'} || '';
	$set = $vars{'set'} || '';
	$start = 0;
	$mdp = $vars{metadataPrefix};

	if( exists($vars{resumptionToken}) ) {
		my $token = $vars{resumptionToken} || '';
		my @args = decodeToken($token);
		($start,$from,$until,$mdp,$set) = @args;
		if( !$mdp ) {
			$r->errors(new HTTP::OAI::Error(code=>'badResumptionToken',message=>"metadataPrefix part of resumption token missing [$token]"));
		} elsif( !defined($mdf = $repo->getMetadataFormat( $mdp )) ) {
			$r->errors(new HTTP::OAI::Error(code=>'badResumptionToken',message=>"Invalid metadataPrefix part [$mdp] of resumption token [$token]"));
		} elsif( length($start) > 10 ) {
			$r->errors(new HTTP::OAI::Error(code=>'badResumptionToken',message=>"Starting offset (\"$start\") too long [$token]"));
		} elsif( $from && length($from) != 14 ) {
			$r->errors(new HTTP::OAI::Error(code=>'badResumptionToken',message=>"From date (\"$from\") not 14 digits long [$token]"));
		} elsif( $until && length($until) != 14 ) {
			$r->errors(new HTTP::OAI::Error(code=>'badResumptionToken',message=>"Until date (\"$until\") not 14 digits long [$token]"));
		}
		return $r if $r->errors;
	}

#	my @rids = sort { $a <=> $b } repositoryIds($dbh, $source, $mdp);

	my $SQL = "SELECT `id`,DATE_FORMAT(`datestamp`,'$Celestial::DBI::DATE_FORMAT'),`header`,`metadata`,`about` FROM " . $mdf->table . " ";
	my @LOGIC = ();
	my @VALUES = ();
	my @ORDER = ();

	# Manual join with repository table (coz MySQL table joins suck)
#	push(@LOGIC, '(' . join(' OR ', map { 'repository=?' } @rids) . ')');
#	push(@VALUES, @rids);
	if( $from ) {
		push(@LOGIC, " `cursor` >= CONCAT(?,LPAD(MOD(?,1000),3,'0')) ");
		push(@VALUES, $from, ($start < 0 ? 0 : $start));
	}
	if( $until ) {
		push(@LOGIC, " `cursor` <= CONCAT(?,'999') ");
		push(@VALUES, $until);
	}
	if( $set && (my @setIds = $repo->listSetIds($set)) ) {
		$SQL .= ", " . $repo->setmemberships_table . " AS sm ";
		push(@LOGIC, " `id`=`record` ");
		push(@LOGIC, '(' . join(' OR ', map { 'sm.`set`=?' } @setIds ) . ')');
		push(@VALUES, @setIds);
	} elsif( $set ) {
		$set = encode_entities($set);
		$r->errors(new HTTP::OAI::Error(code=>'badResumptionToken',message=>"A set was in the resumption token ('$set'), but doesn't exist"));
		return $r;
	}
	if( @LOGIC ) {
		$SQL .= " WHERE " . join(' AND ', @LOGIC);
	}
	$SQL .= " ORDER BY `cursor` ASC LIMIT 101";
#warn "Executing (" . join(',', @VALUES), "):\n$SQL\n";
	my $sth = $dbh->prepare($SQL) or die $!;
	$sth->execute(@VALUES) or die "Error executing $SQL: $!";

	my ($UID,$datestamp,$header,$metadata,$about);
	$sth->bind_columns(\$UID,\$datestamp,\$header,\$metadata,\$about);
	my $c = 0;

	my $parser = XML::LibXML->new;
	while( $sth->fetch ) {
		if( ++$c == 101 ) {
			$r->resumptionToken(new HTTP::OAI::ResumptionToken(resumptionToken=>encodeToken($UID,$datestamp,$until,$mdp,$set)));
			last;
		}

		$r->record(my $record = new HTTP::OAI::Record(
			version=>2.0,
			header=>HTTP::OAI::Header->new(dom=>$parser->parse_string($header))
		));
		eval {
			if( $metadata ) {
				my ($dom) = $dbh->getDomContent($parser->parse_string($metadata));
				$record->metadata(HTTP::OAI::Metadata->new(dom=>$dom)) if $dom;
			}
		};
		warn $@ if $@;
		if( $about ) {
			for($dbh->getDomContent($parser->parse_string($about))) {
				$record->about(HTTP::OAI::Metadata->new(dom=>$_));
			}
		}
	}

	$sth->finish;

	if( !$r->record ) {
		if( exists($vars{resumptionToken}) ) {
			$r->errors(new HTTP::OAI::Error(
				code=>'badResumptionToken',
				message=>'No records match the resumption token',
			));
			$r->errors(new HTTP::OAI::Error(
				code=>'noRecordsMatch',
				message=>'No records match the resumption token',
			));
		} else {
			$r->errors(new HTTP::OAI::Error(
				code=>'noRecordsMatch'
			));
		}
	} elsif( !$r->resumptionToken && exists($vars{resumptionToken}) ) {
		$r->resumptionToken(new HTTP::OAI::ResumptionToken);
	}

	return $r;
}
sub ListSets {
	my( $self, $r, %args ) = @_;
	my $dbh = $self->dbh;
	my %vars = %{$args{args}};
	my $repo = $args{ repository };
	my $mdf = $args{ metadataFormat };

	my $sth = $dbh->prepare("SELECT `id`,`setSpec`,`setName` FROM ".$repo->sets_table." ORDER BY `id`");
	$sth->execute() or die $!;

	my ($id,$setSpec,$setName);
	$sth->bind_columns(\$id,\$setSpec,\$setName);
	while( $sth->fetch ) {
		$r->set(HTTP::OAI::Set->new(
			setSpec=>$setSpec,
			setName=>$setName
		));
	}
	$sth->finish;

	if( !$r->set ) {
		$r->errors(new HTTP::OAI::Error(code=>'noSetHierarchy'));
	}

	return $r;
}

sub datestamp {
	my $ds = shift;
	$ds =~ s/(\d{4})(\d{2})(\d{2})/$1-$2-$3/;
	return $ds;
}

sub encodeToken {
	return join '!', map { Celestial::CGI::uri_escape(($_||''),"^A-Za-z0-9") } @_;
}

sub decodeToken {
	return map { Celestial::CGI::uri_unescape($_) } split /!/, $_[0];
}

1;
