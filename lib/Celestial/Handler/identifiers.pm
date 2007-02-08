#!/usr/bin/perl -w

use strict;
use warnings;

use encoding "utf8";

use Celestial::Handler;
use vars qw( @ISA );
@ISA = qw( Celestial::Handler );
use URI::Escape qw(uri_escape_utf8);

push @ORDER, 'repository';

use Celestial::DBI;
use POSIX qw/strftime pow/;
use CGI;
use Text::Wrap;
use XML::LibXML;
use HTTP::OAI::Metadata::OAI_DC;

our $dom = XML::LibXML::Document->new( '1.0', 'UTF-8' );
our $dbh;

our $logy = CGI::param('logy') || 1;
our $from = CGI::param('from');
our $until = CGI::param('until');
our $baseURL = CGI::param('baseURL') or die "Requires baseURL";
our $format = CGI::param('format') || 'table';
our $dataset = CGI::param('dataset');
our $width = CGI::param('width')||0;
our $height = CGI::param('height')||0;
$width = 800 if !$width or $width =~ /\D/;
$height = 300 if !$height or $height =~ /\D/;

eval {

$dbh = Celestial::DBI->connect() or die "Unable to connect to database: $!";
my $iar_dbh = DBI->connect("dbi:mysql:host=leo;port=3316;database=iar","iar","") or die $!;

my $repo = $dbh->getRepository($dbh->getRepositoryBaseURL($baseURL))
	or die "baseURL doesn't match any registered repository";

my $mdf = $repo->getMetadataFormat('oai_dc')
	or die "Repository does not have oai_dc";

my $table = $mdf->table;

my @logic;
my @values;
if( $from and $until ) {
	push @logic, "`accession` BETWEEN ? AND ?";
	push @values, $from, $until;
} elsif( $from ) {
	push @logic, "`accession` >= ?";
	push @values, $from;
} elsif( $until ) {
	push @logic, "`accession` <= ?";
	push @values, $until;
}
if( $dataset ) {
	push @logic, "`accession` >= ? AND `accession` < ? + INTERVAL 1 DAY";
	push @values, $dataset, $dataset;
}

if( $format eq 'graph' )
{
	my $sth = $dbh->prepare("SELECT DATE_FORMAT(`accession`,'\%Y\%m\%d') AS `d`,COUNT(*) AS `c` FROM `$table`" . (@logic ? ' WHERE ' . join(' AND ', @logic) : '') . " GROUP BY `d` ORDER BY `d` ASC"); 
	$sth->execute(@values) or die $dbh->errstr;

	my @DATA = ([],[]);
	my $max = 0;
	while(my($day,$c) = $sth->fetchrow_array)
	{
		push @{$DATA[0]}, $day;
		push @{$DATA[1]}, $c;
		$max = $c if $c > $max;
	}
	for(my $i = 0; $i < $#{$DATA[0]}; $i++)
	{
		my $n = day_inc($DATA[0]->[$i]);
		while($n < $DATA[0]->[$i+1] and @{$DATA[0]} < 10000) {
			splice(@{$DATA[0]}, $i+1, 0, $n);
			splice(@{$DATA[1]}, $i+1, 0, 0);
			$n = day_inc($n);
		}
	}
	
	if( @{$DATA[0]} == 10000 ) {
		die "Internal Error: Can't plot more than 10000 data points\n"
	}

	my $w = $width;
	my $h = $height;
	
	my $svg = svg($w,$h);

	my $x = 0; my $y = 0;
	my $max_x = $w; my $max_y = $h;
	
	$x += 15;
	$y += 5;
	$max_x -= 30;
	$max_y -= 5;

	$svg->appendChild( me( 'desc', mt( 'Deposits per Day' )));
	$svg->appendChild( me( 'rect', undef, {
		dx => 0,
		dy => 0,
		width => $w,
		height => $h,
		stroke => '#888',
		fill => 'none',
		'stroke-width' => '1',
	}));

	$svg->appendChild( my $ctx = me( 'g', undef, {
#		transform => "translate($w $h) rotate(180)"
	}));

	$max_y -= 20;
	if( $logy ) {
		svg_log_y_axis( $ctx, 0, $max, $x, $y, $max_y-$y );
	} else {
		svg_y_axis( $ctx, 0, $max, $x, $y, $max_y-$y );
	}
	$x += 20;
	$max_y += 20;
	svg_x_axis( $ctx, $DATA[0], $x, $max_y, $max_x-$x, {
		skip_ticks => int(@{$DATA[0]}/10+.5),
	});
	$max_y -= 20;

	$ctx->appendChild( me( 'rect', undef, {
		x => $x,
		y => $y,
		width => $max_x-$x,
		height => $max_y-$y,
		stroke => '#000',
		fill => 'none',
		'stroke-width' => 1,
	}));

	if( $logy ) {
		svg_log_y_plot_series( $ctx, @DATA, $max, $x, $y, $max_x-$x, $max_y-$y );
	} else {
		svg_plot_series( $ctx, $DATA[1], $max, $x, $y, $max_x-$x, $max_y-$y );
	}

	binmode(STDOUT, ":utf8");
	print CGI::header('image/svg+xml');
	print $dom->toString(1);
}
elsif( $format eq 'detail' )
{
	my $sth = $dbh->prepare("SELECT `id`,DATE_FORMAT(`accession`,'\%Y\%m\%d'),`identifier` FROM `$table`" . (@logic ? ' WHERE ' . join(' AND ', @logic) : '')); 
	$sth->execute(@values);

	my %sets;

	print CGI::header('text/html'),
		CGI::start_html('Detail');

	print "<table>";
	while(my( $id, $ds, $identifier ) = $sth->fetchrow_array )
	{
		my $u = URI->new( $baseURL );
		$u->query_form(
			verb => 'GetRecord',
			metadataPrefix => 'oai_dc',
			identifier => $identifier
		);
		printf("<tr><td>%s</td><td>%s</td>\n",
			$ds,
			sprintf("<a href=\"%s\">%s</a>",
				CGI::escapeHTML($u),
				CGI::escapeHTML($identifier),
			)
		);
		my $rec = $mdf->getRecord($id);
		foreach my $set ($rec->header->setSpec) {
			$sets{$set}++;
		}
		if( my $md = $rec->metadata ) {
			my $dc = HTTP::OAI::Metadata::OAI_DC->new();
			$md->set_handler(HTTP::OAI::SAXHandler->new(
				Handler => $dc
			));
			$md->generate;
			my( $link ) = @{$dc->dc->{identifier}};
			my( $title ) = @{$dc->dc->{title}};
			print "<td><a href=\"".CGI::escapeHTML($link)."\">".CGI::escapeHTML($title)."</a></td>";
		}
		print "</tr>";
	}
	print "</table>";

	print "<table>";
	foreach my $k (sort { $sets{$b} <=> $sets{$a} } keys %sets) {
		my $set = $repo->getSet($repo->getSetId($k));
		my $name = $set ? $set->setName : $k;
		printf("<tr><td>%s</td><td>%s</td><td>%d</td></tr>", $k, $name, $sets{$k});
	}
	print "</table>";

	print CGI::end_html();
}
elsif( $format eq 'raw' )
{
	my $sth = $dbh->prepare("SELECT DATE_FORMAT(`accession`,'\%Y\%m\%d'),`identifier` FROM `$table`" . (@logic ? ' WHERE ' . join(' AND ', @logic) : '')); 
	$sth->execute(@values);

	print CGI::header('text/plain');

	while(my( $ds, $i ) = $sth->fetchrow_array )
	{
		printf("%s\t%s\n", $ds, $i);
	}
}
else
{
	my( $html, $head, $body ) = html();

	my $u = URI->new('','http');
	$u->query_form(
		baseURL => $baseURL,
		format => 'graph',
		from => $from,
		until => $until,
		width => $width,
		height => $height,
	);
	$body->appendChild( me('iframe', me( 'b', mt( 'Requires frames' )), {
		src => "$u",
		type => "image/svg+xml",
		width => $width,
		height => $height,
		style => "border: none;",
	}));

	my( $data, $summary ) = (me('table'), me('table'));

	$data->appendChild( my $tr = me( 'tr' ));
	$tr->appendChild( me( 'th', mt( 'Datestamp' )));
	$tr->appendChild( me( 'th', mt( 'Identifier' )));
	$tr->appendChild( me( 'th', mt( 'Title' )));
	$summary->appendChild( $tr = me( 'tr' ));
	$tr->appendChild( me( 'th', mt( 'Set Spec' )));
	$tr->appendChild( me( 'th', mt( 'Records in Set' )));
	$tr->appendChild( me( 'th', mt( 'Set Name' )));
	
	my $total = 0;

	if( $dataset )
	{
		my $sth = $dbh->prepare("SELECT `id`,DATE_FORMAT(`accession`,'\%Y\%m\%d'),`identifier` FROM `$table`" . (@logic ? ' WHERE ' . join(' AND ', @logic) : '')); 
		$sth->execute(@values);

		my %sets;

		while(my( $id, $ds, $identifier ) = $sth->fetchrow_array )
		{
			$total++;
			my $u = URI->new( $baseURL );
			$u->query_form(
				verb => 'GetRecord',
				metadataPrefix => 'oai_dc',
				identifier => $identifier
			);
			$data->appendChild(my $tr = me('tr'));
			$tr->appendChild( me( 'td', mt( $ds )));
			$tr->appendChild( me( 'td', me( 'a', mt( $identifier ), {
				href => $u
			})));
			my $rec = $mdf->getRecord($id);
			foreach my $set ($rec->header->setSpec) {
				$sets{$set}++;
			}
			if( my $md = $rec->metadata ) {
				my $dc = HTTP::OAI::Metadata::OAI_DC->new();
				$md->set_handler(HTTP::OAI::SAXHandler->new(
					Handler => $dc
				));
				$md->generate;
				my( $link ) = grep { /^http/ } @{$dc->dc->{identifier}};
				my( $title ) = @{$dc->dc->{title}};
				$tr->appendChild( me( 'td', me( 'a', mt( $title ), {
					href => $link
				})));
			}
		}

		foreach my $k (sort { $sets{$b} <=> $sets{$a} } keys %sets) {
			my $set = $repo->getSet($repo->getSetId($k));
			my $name = $set ? $set->setName : $k;
			$summary->appendChild( my $tr = me( 'tr' ));
			$tr->appendChild( me( 'td', mt( $k )));
			$tr->appendChild( me( 'td', mt( $sets{$k} )));
			$tr->appendChild( me( 'td', mt( $name )));
		}
	}

	$body->appendChild( me( 'h2', mt(
		$dataset ?
		"$dataset - $total matching records" :
		'No data set selected'
	)));
	$body->appendChild( $summary );
	$body->appendChild( $data );

	print CGI::header(
			-type => 'text/html',
			-charset => 'utf-8',
		),
		$dom->toString(1);
}

$dbh->disconnect;

}; # End of Eval
if( $@ ) {
	my $msg = $@;
	print CGI::header('text/plain');

	print wrap('', '', "An error occurred during processing: $msg\n");
}

sub html
{
	$dom->createInternalSubset( "HTML", "-//W3C//DTD HTML 4.0 Transitional//EN", "http://www.w3.org/TR/REC-html40/loose.dtd" );

	$dom->setDocumentElement( my $html = me( 'html' ));

	$html->appendChild( my $head = me( 'head' ));
	$html->appendChild( my $body = me( 'body' ));

	return ($html, $head, $body);
}

sub svg
{
	my( $w, $h ) = @_;
	
	$dom = XML::LibXML::Document->new( '1.0' );
	$dom->setStandalone( 0 );
	$dom->createInternalSubset( "svg", "-//W3C//DTD SVG 1.1//EN", "http://www.w3.org/Graphics/SVG/1.1/DTD/svg11.dtd" );

	my $r = $h/$w;

	my $svg = me( 'svg', undef, {
		width => "${w}px",
		height => "${h}px",
		version => "1.1",
		'xmlns' => "http://www.w3.org/2000/svg",
		'xmlns:xlink' => "http://www.w3.org/1999/xlink"
	});

	$dom->setDocumentElement( $svg );

	return $svg;
}

sub svg_log_y_axis
{
	my( $svg, $min, $max, $x, $y, $h ) = @_;

	my $dy = log10(1+$max-$min);

	my $scale_y = $h/$dy;

	$svg->appendChild( my $ctx = me( 'g', undef, {
		transform => "translate($x $y) scale(1 1)",
		'font-family' => 'sans-serif',
		'font-size' => '10pt',
		fill => 'black',
	}));

	my $step = int($dy/10);
	for(my $i = 1; $i < $max**10; $i = $i."0" ) {
		$ctx->appendChild( me( 'text', mt( $i ), {
			x => 10,
			y => ($dy-log10($i+1))*$scale_y+5,
			#y => ($dy-$i)*$scale_y,
			style => 'text-align:right;text-anchor:middle;',
		}));
	}
	
	return 20;
}

sub svg_y_axis
{
	my( $svg, $min, $max, $x, $y, $h ) = @_;

	my $dy = $max-$min;

	my $scale_y = $h/$dy;

	$svg->appendChild( my $ctx = me( 'g', undef, {
		transform => "translate($x $y) scale(1 1)",
		'font-family' => 'sans-serif',
		'font-size' => '10pt',
		fill => 'black',
	}));

	my $step = int($dy/10);
	for(my $i = 1; $i < $max; $i += $step ) {
		$ctx->appendChild( me( 'text', mt( $i ), {
			x => 10,
			y => ($dy-$i)*$scale_y+5,
			style => 'text-align:right;text-anchor:middle;',
		}));
	}
	
	return 20;
}

sub svg_x_axis
{
	my( $svg, $pts, $x, $y, $w, $opts ) = @_;
	$opts ||= {};

	my $scale_x = $w / @$pts;

	my @show = map { 1 } @$pts;
	if( $opts->{skip_ticks} ) {
		for(my $i = 0; $i < @show; $i++) {
			unless($i % $opts->{skip_ticks} == 0) {
				$show[$i] = 0;
			}
		}
		for(my $i = $#show-$opts->{skip_ticks}; $i > 0 and $i < @show; $i++) {
			$show[$i] = 0;
		}
		$show[0] = 1;
		$show[$#show] = 1;
	}

	$svg->appendChild( my $ctx = me( 'g', undef, {
		transform => "translate($x $y) scale(1 1)",
		'font-family' => 'sans-serif',
		'font-size' => '9pt',
		fill => 'black',
	}));

	for(my $i = 0; $i < @$pts; $i++) {
		next unless $show[$i];

		$ctx->appendChild( me( 'rect', undef, {
			x => $i*$scale_x+.5,
			y => -20,
			width => 1,
			height => 5
		}));
			
		$ctx->appendChild( me( 'text', mt( $pts->[$i] ), {
			x => $i*$scale_x,
			style => 'text-align:center;text-anchor:middle;',
		}));
	}
	
	return 20;
}

sub svg_log_y_plot_series
{
	my( $svg, $labels, $data, $max, $x, $y, $w, $h ) = @_;
	
	$max = log10($max+1);

	my $scale_x = $w/@$data;
	my $scale_y = $h/$max;
	
	$svg->appendChild( my $plot = me( 'g', undef, {
		transform => "translate($x $y) scale($scale_x $scale_y)"
	}));
	for(my $i = 0; $i < @$data; $i++)
	{
		my $v = $data->[$i] or next;
		$v = log10($v+1);
		my $l = URI->new('', 'http');
		$l->query_form(
			baseURL => $baseURL,
			dataset => $labels->[$i],
			from => $from,
			until => $until,
			width => $width,
			height => $height,
		);
		my $r = int(255*$v/$max);
		my $b = 255-int(255*$v/$max);
		$plot->appendChild( me( 'a', me( 'rect', undef, {
						x => $i,
						y => $max-$v,
						width => 1,
						height => $v,
						fill => sprintf("#%02x00%02x",$r,$b),
						}), {
					'xlink:href' => "$l",
					target => "_top",
					}));
	}
}

sub svg_plot_series
{
	my( $svg, $data, $max, $x, $y, $w, $h ) = @_;
	
	my $scale_x = $w/@$data;
	my $scale_y = $h/$max;
	
	$svg->appendChild( my $plot = me( 'g', undef, {
		transform => "translate($x $y) scale($scale_x $scale_y)"
	}));
	for(my $i = 0; $i < @$data; $i++)
	{
		my $v = $data->[$i] or next;
		my $l = URI->new('', 'http');
		$l->query_form(
			baseURL => $baseURL,
			dataset => $data->[$i],
			from => $from,
			until => $until,
			width => $width,
			height => $height,
		);
		my $r = int(255*$v/$max);
		my $b = 255-int(255*$v/$max);
		$plot->appendChild( me( 'a', me( 'rect', undef, {
						x => $i,
						y => $max-$v,
						width => 1,
						height => $v,
						fill => sprintf("#%02x00%02x",$r,$b),
						}), {
					'xlink:href' => "$l",
					target => "_top",
					}));
	}
}

sub me
{
	my( $name, $child, $attr ) = @_;
	my $e = $dom->createElement( $name );
	if( defined($child) ) {
		if( ref($child) eq 'ARRAY' ) {
			$e->appendChild( $_ ) for @$child;
		} else {
			$e->appendChild( $child );
		}
	}
	if( defined($attr) and ref($attr) eq 'HASH' ) {
		while(my($k,$v) = each %$attr) {
			$e->setAttribute( $k, $v );
		}
	}
	$e;
}

sub mt
{
	$dom->createTextNode( shift );
}

sub day_inc
{
	my $d = shift;
	my $sth = $dbh->prepare("SELECT DATE_FORMAT(? + INTERVAL 1 DAY, '\%Y\%m\%d')");
	$sth->execute($d) or die $dbh->errstr;;
	($d) = $sth->fetchrow_array;
	return $d;
}

sub log10
{
	log(shift)/log(10);
}
