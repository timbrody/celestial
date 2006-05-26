package Celestial::Handler::static;

use strict;
use warnings;

use Celestial::Handler;
use Celestial::Config;
use vars qw( @ISA );
@ISA = qw( Celestial::Handler );

push @ORDER, 'static';

sub page
{
	my( $self, $CGI ) = @_;
	my $file = $CGI->section_path;
	$file =~ s/\.\.//sg; # Prevent walking up the tree

	unless( $file ) {
		$CGI->not_found;
		return;
	}
	
	my $css_file = $SETTINGS->{ paths }->{ html } . $file;
	
	unless( -e $css_file ) {
		$CGI->not_found;
		return;
	}

	if( $file =~ /\.css$/ ) {
		$CGI->content_type( 'text/css' );
	} elsif( $file =~ /\.xsl$/ ) {
		$CGI->content_type( 'text/xml' );
	} else {
		$CGI->content_type( 'text/html' );
	}

	open(my $fh, "<", $css_file)
		or die "Unable to open css file [$css_file]: $!";
	binmode($fh);
	binmode(STDOUT);
	my $buffer;
	while(sysread($fh,$buffer,4096)) {
		print $buffer;
	}
	close($fh);

	return undef;
}

1;
