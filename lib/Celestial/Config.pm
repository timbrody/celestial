package Celestial::Config;

use YAML;

=head1 NAME

Celestial::Config - Read config file

=head1 SYNOPSIS

	my $cfg = Celestial::Config->new( "/etc/celestial/celestial.conf" );

	my $setting = $cfg->get_conf( "foo", "bar" );

=head1 METHODS

=over 4

=cut

use strict;

our $SETTINGS;
our $CFG_ROOT = '/etc/celestial';

sub new
{
	my( $class, $cfg_file ) = @_;

	return $SETTINGS if defined $SETTINGS;

	$cfg_file ||= "$CFG_ROOT/celestial.conf";

	my $self = $class->load_config( $cfg_file )
		or Carp::croak "Error loading config file $cfg_file: $!";

	$self = bless $self, $class;

	$self->{ paths } ||= {};
	$self->{ paths }->{ config } = $cfg_file;
	$self->{ paths }->{ html } ||= "$CFG_ROOT/html";
	$self->{ paths }->{ languages } ||= "$CFG_ROOT/languages";
	for(values %{$self->{ paths }}) {
		$_ =~ s/\/+$//;
	}

	$self->{ languages } = $self->load_languages( $self->{paths}->{languages} );
	unless( scalar keys %{$self->{ languages }} ) {
		Carp::croak "No language files found in $self->{paths}->{languages}";
	}

	$SETTINGS = $self;

	return $self;
}

=item $setting = $cfg->get_conf( ID [, ID [, ID ... ] ] )

Returns the setting for ID, or undef if unset.

=cut

sub get_conf
{
	my( $self, @path ) = @_;

	for(@path)
	{
		return undef unless defined($self->{$_});
		$self = $self->{$_};
	}

	return $self;
}

sub load_config
{
	my( $class, $cfg_file ) = @_;

	return YAML::LoadFile( $cfg_file );
}

sub load_languages
{
	my( $class, $path ) = @_;

	my %langs;

	opendir( my $dir, $path )
		or Carp::croak "Error opening language directory $path: $!";
	my @files = readdir( $dir );
	closedir( $dir );

	for(@files)
	{
		next unless /\.yml$/;
		next if /^\./;
		next unless -f "$path/$_";
		$langs{ substr($_,0,-4) } = "$path/$_";
	}

	return \%langs;
}

1;
