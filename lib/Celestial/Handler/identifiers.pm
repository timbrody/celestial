package Celestial::Handler::identifiers;

use strict;
use warnings;
use encoding "utf8";

use Celestial::Handler;
use vars qw( @ISA );
@ISA = qw( Celestial::Handler );

push @ORDER, 'identifiers';

use Celestial::DBI;
use POSIX qw/strftime pow/;
use CGI;
use Text::Wrap;
use XML::LibXML;
use HTTP::OAI::Metadata::OAI_DC;

our $YMD = '%Y%m%d';

sub title
{
	my( $self, $CGI ) = @_;
	return $CGI->msg( 'identifiers.title' );
}

sub page
{
	my( $self, $CGI ) = @_;
	my $dbh = $self->dbh;
	my $dom = $self->dom;

	my $url = $CGI->url;
	$url =~ s/;/&/g;
	my %vars = URI->new($url)->query_form;

	my $logy = defined($vars{logy}) ? $vars{logy} : 1;
	my $from = $vars{from};
	my $until = $vars{until};
	$from ||= $self->now(" - INTERVAL 1 YEAR");
	$until ||= $self->now('');
	my $baseURL = $vars{baseURL} or return $self->error( $CGI, "Requires baseURL");
	my $format = $vars{format} || 'table';
	my $dataset = $vars{dataset};
	my $width = $vars{width} || 0;
	my $height = $vars{height} || 0;
	$width = 800 if !$width or $width =~ /\D/;
	$height = 300 if !$height or $height =~ /\D/;
	my $set = $vars{set};
	my $dna = $vars{dna};

	$from =~ s/[^0-9]//sg;
	$until =~ s/[^0-9]//sg;

	my $repo = $dbh->getRepository($dbh->getRepositoryBaseURL($baseURL))
		or return $self->error( $CGI, "baseURL doesn't match any registered repository: $baseURL" );

	my $mdf = $repo->getMetadataFormat('oai_dc')
		or return $self->error( $CGI, "Repository does not have oai_dc: $baseURL");

	if( defined($set) and length($set) ) {
		unless(defined($set = $repo->getSetId($set))) {
			return $self->error( $CGI, "Set not found in repository: $vars{set}");
		}
	}

	my $table = $mdf->table;
	my $sm_table = $repo->setmemberships_table;
	my $sets_table = $repo->sets_table;

	my $tables = "`$table`";

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
	if( defined $set ) {
		push @logic, "`set` = $set";
		$tables .= " INNER JOIN `$sm_table` ON `id`=`record`";
	}

	if( $format eq 'graph' )
	{
		my $SQL = "SELECT DATE_FORMAT(`accession`,'\%Y\%m\%d') AS `d`,COUNT(*) AS `c` FROM $tables" . (@logic ? ' WHERE ' . join(' AND ', @logic) : '') . " GROUP BY `d` ORDER BY `d` ASC"; 
		my $sth = $dbh->prepare($SQL);
		$sth->execute(@values)
			or return $self->error( $CGI, $dbh->errstr);

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
			my $n = $self->day_inc($DATA[0]->[$i]);
			while($n < $DATA[0]->[$i+1]) {
				splice(@{$DATA[0]}, $i+1, 0, $n);
				splice(@{$DATA[1]}, $i+1, 0, 0);
				$n = $self->day_inc($n);
				if( @{$DATA[0]} > 10000 ) {
					return $self->error( $CGI, "Internal Error: Can't plot more than 10000 data points\n");
				}

			}
		}

		if( @{$DATA[0]} and $from ) {
			for(my $i = $DATA[0]->[0]; $DATA[0]->[0] > $from; $i = $self->day_dec($i)) {
				unshift @{$DATA[0]}, $i;
				unshift @{$DATA[1]}, 0;
				if( @{$DATA[0]} > 10000 ) {
					return $self->error( $CGI, "Internal Error: Can't plot more than 10000 data points\n");
				}
			}
		}
		if( @{$DATA[0]} and $until ) {
			for(my $i = $DATA[0]->[$#{$DATA[0]}]; $DATA[0]->[$#{$DATA[0]}] < $until; $i = $self->day_inc($i)) {
				push @{$DATA[0]}, $i;
				push @{$DATA[1]}, 0;
				if( @{$DATA[0]} > 10000 ) {
					return $self->error( $CGI, "Internal Error: Can't plot more than 10000 data points\n");
				}
			}
		}

		my $w = $width;
		my $h = $height;

		my $svg = $self->svg($CGI,$w,$h);

		my $x = 0; my $y = 0;
		my $max_x = $w; my $max_y = $h;

		$x += 15;
		$y += 1;
		$max_x -= 1;
#		$max_y -= 5;

		$svg->appendChild( dataElement( 'desc', 'Deposits per Day' ));
#		$svg->appendChild( dataElement( 'rect', undef, {
#					dx => 0,
#					dy => 0,
#					width => $w,
#					height => $h,
#					stroke => '#888',
#					fill => 'none',
#					'stroke-width' => '1',
#					}));

		$svg->appendChild( my $ctx = dataElement( 'g', undef, {
#		transform => "translate($w $h) rotate(180)"
					}));

		$max_y -= 20;
		if( $logy ) {
			$x += svg_log_y_axis( $ctx, 0, $max, $x, $y, $max_y-$y );
		} else {
			$x += svg_y_axis( $ctx, 0, $max, $x, $y, $max_y-$y );
		}
		$max_y += 20;
		svg_x_axis( $ctx, $DATA[0], $x, $max_y, $max_x-$x, {} );
		$max_y -= 20;

		$ctx->appendChild( dataElement( 'rect', undef, {
					x => $x,
					y => $y,
					width => $max_x-$x,
					height => $max_y-$y,
					stroke => '#000',
					fill => '#ddd',
					'stroke-width' => 1,
					}));

		my $l = URI->new('', 'http');
		$l->query_form(%{{%vars, format => ''}});

		if( $logy ) {
			svg_log_y_plot_series( $ctx, $l, @DATA, $max, $x, $y, $max_x-$x, $max_y-$y );
		} elsif( $dna ) {
			svg_dna_plot_series( $ctx, $l, @DATA, $max, $x, $y, $max_x-$x, $max_y-$y );
		} else {
			svg_plot_series( $ctx, $l, @DATA, $max, $x, $y, $max_x-$x, $max_y-$y );
		}

		binmode(STDOUT, ":utf8");
		$CGI->content_type('image/svg+xml');
		print $dom->toString(1);
	}
	elsif( $format eq 'csv' )
	{
		my $sth = $dbh->prepare("SELECT DATE_FORMAT(`accession`,'$YMD') AS d,`setName`,COUNT(*) FROM `$table` AS R INNER JOIN `$sm_table` ON R.`id`=`record` INNER JOIN `$sets_table` AS S ON `set`=S.`id`" . (@logic ? ' WHERE ' . join(' AND ', @logic) : '') . " GROUP BY d,`set`");
		$sth->execute(@values) or die $dbh->errstr;
		
		my @DATA;

		$CGI->content_type('text/plain');
		my %sets;
		my @cols;
		my $row = 0;
		my $l = URI->new($CGI->url,'http');
		$l->query_form(%{{
			%vars,
			format => '',
		}});
		while(my( $d, $set, $c) = $sth->fetchrow_array)
		{
			if( @cols and $cols[1] != $d ) {
				$l->query_form(%{{
					$l->query_form,
					dataset => $cols[1],
				}});
				$cols[0] = $l;
				for(my $i = 0; $i < @cols; $i++) {
					$DATA[$i]->[$row] = $cols[$i];
				}
				@cols = ();
				$row++;
			}
			my $col = $sets{$set} ||= scalar(keys %sets) + 2;
			$cols[1] = $d;
			$cols[2] += $c;
			$cols[$col] = $c;
		}
		if( @cols ) {
			$l->query_form(%{{
				$l->query_form,
				dataset => $cols[1],
			}});
			$cols[0] = $l;
			for(my $i = 0; $i < @cols; $i++) {
				$DATA[$i]->[$row] = $cols[$i];
			}
		}

		if( $from and $until ) {
			for(my $i = $from, my $j = 0; $i <= $until; $i = $self->day_inc($i), $j++) {
				if( $DATA[1]->[$j] != $i ) {
					$l->query_form(%{{
							$l->query_form,
							dataset => $i,
							}});
					splice @{$DATA[0]}, $j, 0, $l;
					splice @{$DATA[1]}, $j, 0, $i;
					for(my $k = 2; $k < @DATA; $k++) {
						splice @{$DATA[$k]}, $j, 0, 0;
					}
				}
				if( @{$DATA[1]} > 10000 ) {
					return $self->error( $CGI, "Internal Error: Can't plot more than 10000 data points\n");
				}
			}
		}

		print "URL\tDay\tTotal Records";
		foreach my $k (sort { $sets{$a} <=> $sets{$b} } keys %sets)
		{
			print "\t$k";
		}
		print "\n";
		for(my $i = 0; $i < @{$DATA[0]}; $i++) {
			my @row;
			for(my $j = 0; $j < @DATA; $j++) {
				push @row, $DATA[$j]->[$i];
			}
			print join("\t", map { defined($_) ? $_ : 0 } @row), "\n";
		}
	}
	elsif( $format eq 'raw' )
	{
		my $sth = $dbh->prepare("SELECT DATE_FORMAT(`accession`,'\%Y\%m\%d'),`identifier` FROM $tables" . (@logic ? ' WHERE ' . join(' AND ', @logic) : '')); 
		$sth->execute(@values);

		$CGI->content_type('text/plain');

		while(my( $ds, $i ) = $sth->fetchrow_array )
		{
			printf("%s\t%s\n", $ds, $i);
		}
	}
	else
	{
		return $self->SUPER::page($CGI);
	}
	return 0;
}

sub body
{
	my( $self, $CGI ) = @_;
	my $dbh = $self->dbh;
	my $dom = $self->dom;

	my $url = $CGI->url;
	$url =~ s/;/&/g;
	my %vars = URI->new($url)->query_form;

	my $logy = defined($vars{logy}) ? $vars{logy} : 1;
	my $from = $vars{from};
	my $until = $vars{until};
	$from ||= $self->now(" - INTERVAL 1 YEAR");
	$until ||= $self->now('');
	my $baseURL = $vars{baseURL} or return $self->SUPER::error( $CGI, "Requires baseURL");
	my $format = $vars{format} || 'table';
	my $dataset = $vars{dataset};
	my $width = $vars{width} || 0;
	my $height = $vars{height} || 0;
	$width = 800 if !$width or $width =~ /\D/;
	$height = 300 if !$height or $height =~ /\D/;
	my $set = $vars{set};

	my $repo = $dbh->getRepository($dbh->getRepositoryBaseURL($baseURL))
		or return $self->SUPER::error( $CGI, "baseURL doesn't match any registered repository");

	my $mdf = $repo->getMetadataFormat('oai_dc')
		or return $self->SUPER::error( $CGI, "Repository does not have oai_dc");

	if( defined($set) and length($set) ) {
		unless(defined($set = $repo->getSetId($set))) {
			return $self->SUPER::error( $CGI, "Set not found in repository: $vars{set}");
		}
	}

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

	my $body = dataElement( 'div' );

	$body->appendChild( dataElement( 'a', dataElement( 'h2', $repo->identifier ), {
		href => $baseURL
	}));

	my $img_link = URI->new('','http');
	$img_link->query_form(%{{%vars, format=>'graph', dataset=>''}});
	my $set_link = URI->new('','http');
	$set_link->query_form(%vars);

	$body->appendChild( dataElement('iframe', dataElement( 'b', 'Requires frames' ), {
				src => "$img_link",
				type => "image/svg+xml",
				width => $width,
				height => $height,
				style => "border: none;",
				}));

	my( $data, $summary ) = (dataElement('table'), dataElement('table'));

	$data->appendChild( my $tr = dataElement( 'tr' ));
	$tr->appendChild( dataElement( 'th', 'Datestamp' ));
	$tr->appendChild( dataElement( 'th', 'Identifier' ));
	$tr->appendChild( dataElement( 'th', 'Title' ));
	$summary->appendChild( $tr = dataElement( 'tr' ));
	$tr->appendChild( dataElement( 'th', 'Set Spec' ));
	$tr->appendChild( dataElement( 'th', 'Records before Period' ));
	$tr->appendChild( dataElement( 'th', 'Records in Period' ));
	$tr->appendChild( dataElement( 'th', 'Records on Day' ));
	$tr->appendChild( dataElement( 'th', 'Set Name' ));

	my $total = 0;

	if( $dataset )
	{
		my $sth = $dbh->prepare("SELECT `id`,DATE_FORMAT(`accession`,'\%Y\%m\%d'),`identifier` FROM `$table`" . (@logic ? ' WHERE ' . join(' AND ', @logic) : '')); 
		$sth->execute(@values);

		my %sets;

		while(my( $id, $ds, $identifier ) = $sth->fetchrow_array )
		{
			$total++;
			my $oai_link = URI->new( $baseURL );
			$oai_link->query_form(
					verb => 'GetRecord',
					metadataPrefix => 'oai_dc',
					identifier => $identifier
					);
			$data->appendChild(my $tr = dataElement('tr'));
			$tr->appendChild( dataElement( 'td', $ds ));
			$tr->appendChild( dataElement( 'td', dataElement( 'a', $identifier, {
							href => $oai_link
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
				$tr->appendChild( dataElement( 'td', dataElement( 'a', $title, {
								href => $link
								})));
			}
		}

		my $sets_prev = $from ?
			$self->sets_summary($repo, $mdf, undef, $self->day_dec($from) ) :
			{};
		my $sets_totals = $self->sets_summary($repo, $mdf, $from, $until );

		foreach my $k (sort { $sets{$b} <=> $sets{$a} } keys %sets) {
			my $set = $repo->getSet($repo->getSetId($k));
			my $name = $set ? $set->setName : $k;
			$summary->appendChild( my $tr = dataElement( 'tr' ));
			$set_link->query_form(%{{%vars, set => $k}});
			$tr->appendChild( dataElement( 'td', dataElement( 'a', $name, {
				href => $set_link,
			})));
			$tr->appendChild( dataElement( 'td', $sets_prev->{$k} || '-' ));
			$tr->appendChild( dataElement( 'td', $sets_totals->{$k} ));
			$tr->appendChild( dataElement( 'td', $sets{$k} ));
			$tr->appendChild( dataElement( 'td', $k ));
		}
	}

	$body->appendChild( dataElement( 'h2', 
				$dataset ?
				"$dataset - $total matching records" :
				'No data set selected'
				));
	$body->appendChild( $summary );
	$body->appendChild( $data );

	return $body;
}

sub sets_summary
{
	my( $self, $repo, $mdf, $from, $until ) = @_;
	my $dbh = $self->dbh;

	my $table = $mdf->table;
	my $sets_table = $repo->sets_table;
	my $sm_table = $repo->setmemberships_table;

	my( @logic, @values );

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

	my $sth = $dbh->prepare("SELECT `setSpec`,COUNT(*) FROM `$table` AS md INNER JOIN `$sm_table` AS sm ON md.`id`=sm.`record` INNER JOIN `$sets_table` AS st ON sm.`set`=st.`id`" . (@logic ? " WHERE " . join(' AND ', @logic) : '') . " GROUP BY sm.`set`");
	$sth->execute(@values) or die $dbh->errstr;
	
	my %sets;
	while(my( $spec, $c ) = $sth->fetchrow_array) {
		$sets{$spec} = $c;
	}

	$sth->finish;

	return \%sets;
}

sub svg
{
	my( $self, $CGI, $w, $h ) = @_;
	my $dom = $self->dom;
	
	$dom->setStandalone( 0 );
	$dom->createInternalSubset( "svg", "-//W3C//DTD SVG 1.1//EN", "http://www.w3.org/Graphics/SVG/1.1/DTD/svg11.dtd" );

	my $r = $h/$w;

	my $svg = dataElement( 'svg', undef, {
		width => "${w}px",
		height => "${h}px",
		version => "1.1",
		'xmlns' => "http://www.w3.org/2000/svg",
		'xmlns:xlink' => "http://www.w3.org/1999/xlink"
	});

	$dom->setDocumentElement( $svg );

	$svg->appendChild( $self->script( $CGI ));
	$svg->setAttribute( 'onload', 'plotInit(evt)' );

	return $svg;
}

sub svg_log_y_axis
{
	my( $svg, $min, $max, $x, $y, $h ) = @_;

	return 0 if $max-$min <= 0;

	my $w = length($max) * 3; # 5 pixels-ish per char
	my $tick_width = 4;

	my $dy = log10(1+$max-$min);

	my $scale_y = $h/$dy;

	$svg->appendChild( my $ctx = dataElement( 'g', undef, {
		transform => "translate($x $y) scale(1 1)",
		'font-family' => 'sans-serif',
		'font-size' => '10pt',
		fill => 'black',
	}));

	my $step = int($dy/10);
	for(my $i = 1; $i < $max; $i = $i."0" ) {
		$ctx->appendChild( dataElement( 'rect', undef, {
			x => $w - $tick_width,
			y => ($dy-log10($i+1))*$scale_y,
			width => $tick_width,
			height => 1,
			fill => 'black',
		}));
		$ctx->appendChild( dataElement( 'text', $i, {
			x => $w - $tick_width - 2,
			y => ($dy-log10($i+1))*$scale_y+5,
			#y => ($dy-$i)*$scale_y,
			style => 'text-align:right;text-anchor:end;',
		}));
	}
	
	return $w;
}

sub svg_y_axis
{
	my( $svg, $min, $max, $x, $y, $h ) = @_;

	my $w = length($max) * 3; # 3 pixels-ish per char
	my $tick_width = 4;

	my $dy = $max-$min;

	return 0 if $dy == 0;

	my $scale_y = $h/$dy;

	my $max_ticks = $h / 10 > 10 ? 10 : 5;

	$svg->appendChild( my $ctx = dataElement( 'g', undef, {
		transform => "translate($x $y) scale(1 1)",
		'font-family' => 'sans-serif',
		'font-size' => '10pt',
		fill => 'black',
	}));

	my $step = int($dy/$max_ticks) || 1;
	
	for(my $i = 0; $i < $max; $i += $step ) {
		$ctx->appendChild( dataElement( 'rect', undef, {
			x => $w - $tick_width,
			y => ($dy-$i)*$scale_y,
			width => $tick_width,
			height => 1,
			fill => 'black',
		}));
		$ctx->appendChild( dataElement( 'text', $i, {
			x => $w - $tick_width - 2,
			y => ($dy-$i)*$scale_y+5,
			style => 'text-align:right;text-anchor:end;',
		}));
	}
	
	return $w;
}

sub svg_x_axis
{
	my( $svg, $pts, $x, $y, $w, $opts ) = @_;
	$opts ||= {};

	return 0 if @$pts == 0;

	my $scale_x = $w / @$pts;

	my @show = map { 1 } @$pts;
	my $max_x_ticks = $w/80;
	my $skip_ticks = int(@$pts/$max_x_ticks+.5);
	if( $skip_ticks ) {
		for(my $i = 0; $i < @show; $i++) {
			unless($i % $skip_ticks == 0) {
				$show[$i] = 0;
			}
		}
		for(my $i = $#show-$skip_ticks; $i > 0 and $i < @show; $i++) {
			$show[$i] = 0;
		}
		$show[0] = 1;
		$show[$#show] = 1;
	}

	$svg->appendChild( my $ctx = dataElement( 'g', undef, {
		transform => "translate($x $y) scale(1 1)",
		'font-family' => 'sans-serif',
		'font-size' => '9pt',
		fill => 'black',
	}));

	for(my $i = 1; $i < $#$pts; $i++) {
		next unless $show[$i];

		$ctx->appendChild( dataElement( 'rect', undef, {
			x => $i*$scale_x+.5,
			y => -20,
			width => 1,
			height => 5
		}));
			
		$ctx->appendChild( dataElement( 'text', $pts->[$i], {
			x => $i*$scale_x+.5,
			style => 'text-align:center;text-anchor:middle;',
		}));
	}

	# Left-align the first point
	$ctx->appendChild( dataElement( 'rect', undef, {
		x => .5,
		y => -20,
		width => 1,
		height => 5
	}));
	
	if( $x > 25 )
	{
		$ctx->appendChild( dataElement( 'text', $pts->[0], {
			x => .5,
			style => 'text-align:center;text-anchor:middle;',
		}));
	}
	else
	{
		$ctx->appendChild( dataElement( 'text', $pts->[0], {
			x => $x * -1,
			style => 'text-align:left;text-anchor:start;',
		}));
	}

	# Right-align the last point
	$ctx->appendChild( dataElement( 'rect', undef, {
		x => $#$pts*$scale_x+.5,
		y => -20,
		width => 1,
		height => 5
	}));
			
	$ctx->appendChild( dataElement( 'text', $pts->[$#$pts], {
		x => $w,
		style => 'text-align:right;text-anchor:end;',
	}));
	
	return 20;
}

sub svg_log_y_plot_series
{
	my( $svg, $l, $labels, $data, $max, $x, $y, $w, $h ) = @_;
	
	return 0 if $max <= 0;
	
	$max = log10($max+1);

	my $scale_x = $w/@$data;
	my $scale_y = $h/$max;
	
	$svg->appendChild( my $plot = dataElement( 'g', undef, {
		transform => "translate($x $y) scale(1 1)",
		id => "plot",
		_size => scalar @$data,
	}));
	for(my $i = 0; $i < @$data; $i++)
	{
		my $v = $data->[$i] or next;
		$v = log10($v+1);
		my %qry = $l->query_form;
		$qry{dataset} = $labels->[$i];
		$l->query_form(%qry);
		my $r = int(255*$v/$max);
		my $b = 255-int(255*$v/$max);
		$plot->appendChild( dataElement( 'a', dataElement( 'rect', undef, {
						x => $i*$scale_x,
						y => ($max-$v)*$scale_y,
						width => $scale_x < 1 ? 1 : $scale_x,
						height => $v*$scale_y,
						fill => sprintf("#%02x00%02x",$r,$b),
						stroke => '#000',
						'stroke-width' => ($scale_x > 1 ? 1 : 0),
						}), {
					'xlink:href' => "$l",
					target => "_top",
					}));
	}
}

sub svg_dna_plot_series
{
	my( $svg, $l, $labels, $data, $max, $x, $y, $w, $h ) = @_;
	
	return 0 if @$data == 0;

	my $scale_x = $w/@$data;
	my $scale_y = $h/$max;
	
	$svg->appendChild( my $plot = dataElement( 'g', undef, {
		transform => "translate($x $y) scale($scale_x $scale_y)",
		id => "plot",
		_size => scalar @$data,
	}));
	for(my $i = 0; $i < @$data; $i++)
	{
		my $v = $data->[$i] or next;
		my %qry = $l->query_form;
		$qry{dataset} = $labels->[$i];
		$l->query_form(%qry);
		my $r = int(255*$v/$max);
		my $b = 255-int(255*$v/$max);
		$plot->appendChild( dataElement( 'a', dataElement( 'rect', undef, {
						x => $i,
						y => $max-$v,
						width => 1,
						height => $v,
						fill => sprintf("#%02x00%02x",$r,$b),
						stroke => '#000',
						'stroke-width' => '.2',
						}), {
					'xlink:href' => "$l",
					target => "_top",
					}));
	}
}

sub svg_plot_series
{
	my( $svg, $l, $labels, $data, $max, $x, $y, $w, $h ) = @_;
	
	return 0 if @$data == 0;

	my $scale_x = $w/@$data;
	my $scale_y = $h/$max;
	
	$svg->appendChild( my $plot = dataElement( 'g', undef, {
		transform => "translate($x $y) scale(1 1)",
		id => "plot",
		_size => scalar @$data,
	}));
	for(my $i = 0; $i < @$data; $i++)
	{
		my $v = $data->[$i] or next;
		my %qry = $l->query_form;
		$qry{dataset} = $labels->[$i];
		$l->query_form(%qry);
		my $r = int(255*$v/$max);
		my $b = 255-int(255*$v/$max);
		$plot->appendChild( dataElement( 'a', dataElement( 'rect', undef, {
						x => $i*$scale_x,
						y => ($max-$v)*$scale_y,
						width => ($scale_x < 1 ? 1 : $scale_x),
						height => $v*$scale_y,
						fill => sprintf("#%02x00%02x",$r,$b),
						stroke => '#000',
						'stroke-width' => ($scale_x > 1 ? 1 : 0),
						}), {
					'xlink:href' => "$l",
					target => "_top",
					}));
	}
}

sub day_inc
{
	my( $self, $d ) = @_;
	my $sth = $self->dbh->prepare("SELECT DATE_FORMAT(? + INTERVAL 1 DAY, '\%Y\%m\%d')");
	$sth->execute($d) or die $self->dbh->errstr;;
	($d) = $sth->fetchrow_array;
	return $d;
}

sub day_dec
{
	my( $self, $d ) = @_;
	my $sth = $self->dbh->prepare("SELECT DATE_FORMAT(? - INTERVAL 1 DAY, '\%Y\%m\%d')");
	$sth->execute($d) or die $self->dbh->errstr;;
	($d) = $sth->fetchrow_array;
	return $d;
}

sub now
{
	my( $self, $diff ) = @_;
	$diff ||= '';
	my $sth = $self->dbh->prepare("SELECT DATE_FORMAT(NOW() $diff, '\%Y\%m\%d')");
	$sth->execute() or die $self->dbh->errstr;
	my ($d) = $sth->fetchrow_array;
	return $d;
}

sub log10
{
	log(shift)/log(10);
}

sub error
{
	my( $self, $CGI, $err ) = @_;

	$CGI->internal_error();
	$CGI->content_type( 'text/plain' );

	print $err;

	return 0;
}

sub script
{
	my( $self, $CGI ) = @_;

	my $script = dataElement( 'script', undef, {
		type => 'text/ecmascript',
		'xlink:href' => $CGI->as_link( 'static/ajax/handler/identifiers.js' ),
	});

	return $script;
}

1;
