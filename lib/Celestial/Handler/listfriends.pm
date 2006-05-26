#!/usr/bin/perl -w

# See README.TXT for legal.

use strict;

use vars qw(%vars $source $source_id);

use lib "/home/celestial/lib";
my $CFG_FILE = "/home/celestial/etc/celestial.conf";

use CGI qw/:standard/;
use XML::LibXML;
use Encode;

binmode(STDOUT,":utf8");

use Celestial::DBI;

# Connect to the database
my $dbh = Celestial::DBI->connect($CFG_FILE) or die "Unable to connect to database: $!";

my $dom = XML::LibXML->createDocument('1.0','UTF-8');

$dom->setDocumentElement(my $root = $dom->createElement('BaseURLs'));

my $sth = $dbh->prepare("SELECT identifier,baseURL FROM Repositories");
$sth->execute;

my ($identifier,$baseURL);
$sth->bind_columns(\$identifier,\$baseURL);

my $c = 0;

my $url = url();
$url =~ s/\/[^\/]*$//;
$url .= "/oaia2/";

while( $sth->fetch ) {
	Encode::_utf8_on($identifier);
	Encode::_utf8_on($baseURL);
	my $repo = $root->appendChild($dom->createElement('baseURL'));
	$repo->setAttribute('id',$identifier);
	$repo->appendText($baseURL);
	$repo->setAttribute('mirror',$url.$identifier);
	$c++;
}

$root->setAttribute('number',$c);

$sth->finish;

print header(
	-type=>'text/xml',
	-expires=>'now',
	-charset=>'utf-8'
),
	$dom->toString;
