package Celestial::Config;

=head1 NAME

Celestial::Config - Read config file

=cut

use YAML;
use Exporter;

use vars qw( $CFG_ROOT $LANG_ROOT $SETTINGS );

use vars qw( @ISA );
@ISA = qw( Exporter );

use vars qw( @EXPORT );
@EXPORT = qw( $CFG );

$CFG_ROOT = '/etc/celestial';
$LANG_ROOT = "$CFG_ROOT/languages";

unless( $CFG ) {
	$SETTINGS = YAML::LoadFile( "$CFG_ROOT/celestial.conf" )
		or die "Unable to read configuration: $!";
	opendir( my $dir, $LANG_ROOT )
		or die "Unable to open language file directory: $!";
	my @files = grep { /\.yml$/ } readdir( $dir );
	closedir( $dir );
	$SETTINGS->{ languages } = {};
	foreach my $file (@files) {
		next unless $file =~ /^(.+)\./;
		$SETTINGS->{ languages }->{ $1 } = "$LANG_ROOT/$file";
	}
}

1;
