#!/usr/bin/perl

use strict;
use warnings;

use Module::Build;

my $class = Module::Build->subclass(
	class => "Module::Build::Custom",
	code => <<'SUBCLASS' );

use vars qw( $ETC_PATH );

sub ACTION_install {
	my $self = shift;
	use File::Find;

	$ETC_PATH = $self->install_path->{conf};
	printf( "mkdir %s\n", $ETC_PATH );
	mkdir( $ETC_PATH );
	find(\&_wanted, 'etc');
	$self->SUPER::ACTION_install;
}

sub _wanted {
	return if $File::Find::dir =~ /\/\./ or /^\./;
	my $dest_path = $File::Find::dir;
	$dest_path =~ s/^\w+\/?//;
	$ETC_PATH .= '/' if $ETC_PATH !~ /\/$/;
	$dest_path = $ETC_PATH . $dest_path;
	$dest_path .= '/' if $dest_path !~ /\/$/;
	unless( -e $dest_path ) {
		printf( "mkdir %s\n", $dest_path );
		mkdir($dest_path);
	}
	my $src = $_;
	my $dest = $dest_path . $_;
	return if -d $_;
	if( -e $dest ) {
		printf("Skipping $dest (exists)\n");
		return;
	}
	printf( "Writing $dest\n" );
	system('cp',$src,$dest);
}

SUBCLASS

my $build = $class->new(
	module_name => 'Celestial',
	script_files => ['bin/celestial', 'bin/celestial-report', 'bin/roar_update'],
	license => 'perl',
	requires => {
		'HTTP::OAI' => '3.17',
		'URI::Find' => '0.1',
		'CGI' => '3.05',
		'Number::Bytes::Human' => '0.04',
	},
	install_path => {
		conf => '/etc/celestial',
	},
);

$build->create_build_script;
