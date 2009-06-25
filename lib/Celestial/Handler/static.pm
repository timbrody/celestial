package Celestial::Handler::static;

use Celestial::Handler;

@ISA = qw( Celestial::Handler );

use strict;

push @ORDER, 'static';

# Avoid a heavy dependency in MIME::Types
our %MIME_TYPES = qw(
txt text/plain
css text/css
xsl text/xml
gif image/gif
jpg image/jpeg
jpeg image/jpeg
);

sub page
{
	my( $self, $CGI ) = @_;
	my $file = $CGI->section_path;
	$file =~ s/\.\.//sg; # Prevent walking up the tree

	unless( $file ) {
		$CGI->not_found;
		return;
	}
	
	my $css_file = $Celestial::Config::SETTINGS->{ paths }->{ html } . '/' . $file;
	
	unless( -e $css_file ) {
		$CGI->not_found;
		return;
	}

	if( $file =~ /\.(\w+)$/ and exists($MIME_TYPES{$1}) ) {
		$CGI->content_type( $MIME_TYPES{$1} );
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
