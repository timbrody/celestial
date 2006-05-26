package Celestial::Config;

=head1 NAME

Celestial::Config - Read config file

=cut

use YAML;
use Exporter;

use vars qw( $CFG_ROOT $SETTINGS );

use vars qw( @ISA );
@ISA = qw( Exporter );

use vars qw( @EXPORT );
@EXPORT = qw( $SETTINGS );
@EXPORT_OK = qw( $SETTINGS );

$CFG_ROOT = '/etc/celestial';

unless( $CFG ) {
	$SETTINGS = YAML::LoadFile( "$CFG_ROOT/celestial.conf" )
		or die "Unable to read configuration [$CFG_ROOT/celestial.conf]: $!";
	$SETTINGS->{ paths } ||= {};
	$SETTINGS->{ paths }->{ html } ||= "$CFG_ROOT/html";
	my $lang_path =
		$SETTINGS->{ paths }->{ languages } ||= "$CFG_ROOT/languages";
	for(values %{$SETTINGS->{ paths }}) {
		$_ =~ s/\/+$//;
	}
	opendir( my $dir, $lang_path )
		or die "Unable to open language file directory [$lang_path]: $!";
	my @files = grep { /\.yml$/ } readdir( $dir );
	closedir( $dir );
	$SETTINGS->{ languages } = {};
	foreach my $file (@files) {
		next unless $file =~ /^(.+)\./;
		$SETTINGS->{ languages }->{ $1 } = "$lang_path/$file";
	}
}

1;
