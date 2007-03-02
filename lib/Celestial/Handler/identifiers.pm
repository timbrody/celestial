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
use Time::Local;

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

	my $cumu = $vars{cumu};
	my $logy = defined($vars{logy}) ? $vars{logy} : 1;
	my $logy_cumu = defined($vars{logy_cumu}) ? $vars{logy_cumu} : 1;
	my $scale_max = $vars{scale_max} || 'auto';
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
	my $granularity = $vars{granularity} || '';

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
	if( $dataset ) {
		push @logic, "`accession` >= ? AND `accession` < ? + INTERVAL 1 DAY";
		push @values, $dataset, $dataset;
	}
	if( defined $set ) {
		push @logic, "`set` = $set";
		$tables .= " INNER JOIN `$sm_table` ON `id`=`record`";
	}
	my( @b4_logic, @b4_values );
	if( $from )
	{
		push @b4_logic, "`accession` < ?";
		push @b4_values, $from;
	}

	my( $date_format, $inc_method, $dec_method );
	if( $granularity eq 'yearly' )
	{
		$date_format = '%Y';
		$inc_method = \&year_inc;
		$dec_method = \&year_dec;
		$from = substr($from,0,4) if $from;
		$until = substr($until,0,4) if $until;
	}
	elsif( $granularity eq 'monthly' )
	{
		$date_format = '%Y%m';
		$inc_method = \&month_inc;
		$dec_method = \&month_dec;
		$from = substr($from,0,6) if $from;
		$until = substr($until,0,6) if $until;
	}
	else
	{
		$date_format = '%Y%m%d';
		$inc_method = \&day_inc;
		$dec_method = \&day_dec;
		$from = substr($from,0,8) if $from;
		$until = substr($until,0,8) if $until;
	}

	if( $format eq 'graph' )
	{
		$self->{colors} = {
			y_axis => '#000',
			x_axis => '#000',
		};
		
		my $sum = 0;
		# We need to fetch the total before the period for cumulative total
		if( $cumu and @b4_logic )
		{
			my $SQL = "SELECT COUNT(*) AS `c` FROM $tables WHERE " . join(' AND ', @b4_logic);
warn "$SQL $b4_values[0]";
			my $sth = $dbh->prepare($SQL);
			$sth->execute(@b4_values)
				or return $self->error( $CGI, $dbh->errstr);
			($sum) = $sth->fetchrow_array;
		}

		my $SQL = "SELECT DATE_FORMAT(`accession`,'$date_format') AS `d`,COUNT(*) AS `c` FROM $tables" . (@logic ? ' WHERE ' . join(' AND ', @logic) : '') . " GROUP BY `d` ORDER BY `d` ASC"; 
#return $self->error($CGI, "Executing: ".join(',',@values,$SQL));
		my $sth = $dbh->prepare($SQL);
		$sth->execute(@values)
			or return $self->error( $CGI, $dbh->errstr);

		my @DATA = ([],[],[]);
		my $max = 0;
		while(my($day,$c) = $sth->fetchrow_array)
		{
			push @{$DATA[0]}, $day;
			push @{$DATA[1]}, $c;
			push @{$DATA[2]}, $sum += $c;
			$max = $c if $c > $max;
		}
		for(my $i = 0; $i < $#{$DATA[0]}; $i++)
		{
			my $n = &$inc_method($self,$DATA[0]->[$i]);
			while($n < $DATA[0]->[$i+1]) {
				splice(@{$DATA[0]}, $i+1, 0, $n);
				splice(@{$DATA[1]}, $i+1, 0, 0);
				splice(@{$DATA[2]}, $i+1, 0, $DATA[2]->[$i]);
				$n = &$inc_method($self,$n);
				if( @{$DATA[0]} > 10000 ) {
					return $self->error( $CGI, "Internal Error: Can't plot more than 10000 data points [gaps]\n");
				}

			}
		}

		if( @{$DATA[0]} and $from ) {
			for(my $i = $DATA[0]->[0]; $DATA[0]->[0] > $from; $i = &$dec_method($self,$i)) {
				unshift @{$DATA[0]}, $i;
				unshift @{$DATA[1]}, 0;
				unshift @{$DATA[2]}, $DATA[2]->[0];
				if( @{$DATA[0]} > 10000 ) {
					return $self->error( $CGI, "Internal Error: Can't plot more than 10000 data points [from: $DATA[0]->[0] > $from = $i]\n");
				}
			}
		}
		if( @{$DATA[0]} and $until ) {
			for(my $i = $DATA[0]->[$#{$DATA[0]}]; $DATA[0]->[$#{$DATA[0]}] < $until; $i = &$inc_method($self,$i)) {
				push @{$DATA[0]}, $i;
				push @{$DATA[1]}, 0;
				push @{$DATA[2]}, $DATA[2]->[$#{$DATA[2]}];
				if( @{$DATA[0]} > 10000 ) {
					return $self->error( $CGI, "Internal Error: Can't plot more than 10000 data points [until: ".$DATA[0]->[$#{$DATA[0]}]." < $until = $i]\n");
				}
			}
		}

		if( $granularity eq 'yearly' ) {
		} elsif( $granularity eq 'monthly' ) {
			for(@{$DATA[0]}) {
				substr($_,4,0) = '-';
			}
		} else {
			for(@{$DATA[0]}) {
				substr($_,4,0) = '-';
				substr($_,7,0) = '-';
			}
		}

		my $w = $width;
		my $h = $height;

		my $svg = $self->svg($CGI,$w,$h);

		my $x = 0; my $y = 0;
		my $max_x = $w-1; my $max_y = $h-1;

#		$x += 1;
#		$y += 1;
#		$max_x -= 1;
#		$max_y -= 1;

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

		my $title_height = ($max_y-$y) / 10;
		$self->svg_title( $ctx, $x + ($max_x-$x) / 2, $y, $title_height, 'Deposits', 'center' );
		$self->svg_title( $ctx, $x, $y, $title_height, 'Cumulative', 'left' );
		$y += $self->svg_title( $ctx, $max_x, $y, $title_height, 'Per Day', 'right' );

		$y += 5;

		if( $scale_max eq 'log' ) {
			$sum = y_axis_max($sum, 1);
			$max = y_axis_max($max, 1);
		} else {
			$sum = y_axis_max($sum, $logy_cumu);
			$max = y_axis_max($max, $logy);
		}
		
		$max_y -= 15;
		if( $cumu ) {
			$self->{colors}->{y_axis} = '#080';
			my $f = $logy_cumu ? \&svg_log_y_axis : \&svg_y_axis;
			$x += &$f( $self, $ctx, 0, $sum, $x, $y, $max_y-$y );

			if( $logy ) {
				$self->{colors}->{y_axis_right} = 'shaded';
				$f = \&svg_log_y_axis_right;
			} else {
				$self->{colors}->{y_axis_right} = '#00a';
				$f = \&svg_y_axis_right;
			}
			$max_x -= &$f( $self, $ctx, 0, $max, $max_x, $y, $max_y-$y );
		} else {
			my $f;
			if( $logy ) {
				$self->{colors}->{y_axis} = 'shaded';
				$f = \&svg_log_y_axis;
			} else {
				$self->{colors}->{y_axis} = '#00a';
				$f = \&svg_y_axis;
			}
			$x += &$f( $self, $ctx, 0, $max, $x, $y, $max_y-$y );
		}
		$max_y += 15;
		$self->svg_x_axis( $ctx, $DATA[0], $x, $max_y, $max_x-$x, 15, {} );
		$max_y -= 15;

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

		my $f = $logy ? \&svg_log_y_plot_series : \&svg_plot_series;
		if( $cumu ) {
			&$f( $self, $ctx, $l, @DATA[0,1], $max, $x, $y, $max_x-$x, $max_y-$y );
		} else {
			&$f( $self, $ctx, $l, @DATA[0,1], $max, $x, $y, $max_x-$x, $max_y-$y );
		}
		if( $cumu ) {
			my $f = $logy_cumu ? \&svg_log_y_plot_line : \&svg_plot_line;
			&$f( $self, $ctx, $l, @DATA[0,2], $sum, $x, $y, $max_x-$x, $max_y-$y, 1 );
		}
		if( $cumu ) {
			my $f = $logy_cumu ? \&svg_log_y_plot_line : \&svg_plot_line;
			&$f( $self, $ctx, $l, @DATA[0,2], $sum, $x, $y, $max_x-$x, $max_y-$y, 0 );
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

sub svg_title
{
	my( $self, $svg, $x, $y, $h, $text, $align ) = @_;

	my $size = $h * .75;

	$align ||= 'center';
	my $anchor = $align eq 'left' ? 'start' : $align eq 'center' ? 'middle' : 'end';

	$svg->appendChild( dataElement( 'text', $text, {
		x => $x,
		y => $y + $size,
		style => "font-family: sans-serif; font-size: ${size}px; font-weight: bold; text-align: $align; text-anchor: $anchor",
	}));

	return $h;
}

sub power10_label
{
	my( $val ) = @_;
	my $p = length("$val") - 1;
	return [
		'10',
		dataElement( 'tspan', $p, {
			dy => -5,
			'font-size' => '80%',
		})
	];
}

sub svg_y_tick
{
	my( $self, $svg, $w, $y, $label, $color ) = @_;

	my $tick_width = 4;

	$svg->appendChild( dataElement( 'rect', undef, {
		x => $w - $tick_width,
		y => $y,
		width => $tick_width,
		height => 1,
		($color ? (fill => $color) : ()),
	}));
	$svg->appendChild( dataElement( 'text', $label, {
		x => $w - $tick_width - 2,
		y => $y + 5,
		style => 'text-align:right;text-anchor:end;',
		($color ? (fill => $color) : ()),
	}));
}

sub svg_y_tick_right
{
	my( $self, $svg, $w, $y, $label, $color ) = @_;

	my $tick_width = 4;

	$svg->appendChild( dataElement( 'rect', undef, {
		x => -1 * $w,
		y => $y,
		width => $tick_width,
		height => 1,
		($color ? (fill => $color) : ()),
	}));
	$svg->appendChild( dataElement( 'text', $label, {
		x => -1 * $w + $tick_width + 2,
		y => $y + 5,
		style => 'text-align:left;text-anchor:start;',
		($color ? (fill => $color) : ()),
	}));
}

sub svg_log_y_axis
{
	my( $self, $svg, $min, $max, $x, $y, $h ) = @_;

	return 0 if $max-$min <= 0;

	my $w = 4 + 3 * 8; # 5 pixels-ish per char
	my $tick_width = 4;

	my $dy = log10(1+$max-$min);

	my $scale_y = $h/$dy;

	$svg->appendChild( my $ctx = dataElement( 'g', undef, {
		transform => "translate($x $y) scale(1 1)",
		style => 'font-family: sans-serif; font-size: 12px',
	}));

	my $step = int($dy/10);
	for(my $i = 1; $i <= $max; $i = $i."0" ) {
		my $color = $self->{colors}->{y_axis};
		if( $color eq 'shaded' ) {
			my $r = int(255*log10($i)/log10($max));
			my $b = 255-int(255*log10($i)/log10($max));
			$color = sprintf("#%02x00%02x",$r,$b);
		}
		$self->svg_y_tick(
			$ctx,
			$w,
			($dy-log10($i+1))*$scale_y,
			power10_label($i),
			$color
		);
	}
	
	return $w;
}

sub svg_log_y_axis_right
{
	my( $self, $svg, $min, $max, $x, $y, $h ) = @_;

	my $w = 4 + 3 * 8; # 6 pixels-ish per char

	my $dy = log10(1+$max);

	return 0 if $max == 0;

	my $scale_y = $h/$dy;

	$svg->appendChild( my $ctx = dataElement( 'g', undef, {
		transform => "translate($x $y) scale(1 1)",
		style => 'font-family: sans-serif; font-size: 12px',
	}));

	my $step = int($dy/10);
	for(my $i = 1; $i <= $max; $i = $i."0" ) {
		my $color = $self->{colors}->{y_axis_right};
		if( $color eq 'shaded' ) {
			my $r = int(255*log10($i)/log10($max));
			my $b = 255-int(255*log10($i)/log10($max));
			$color = sprintf("#%02x00%02x",$r,$b);
		}
		$self->svg_y_tick_right(
			$ctx,
			$w,
			($dy-log10($i+1))*$scale_y,
			power10_label($i),
			$color
		);
	}
	
	return $w;
}

sub svg_y_axis
{
	my( $self, $svg, $min, $max, $x, $y, $h ) = @_;

	my $w = 4 + length("$max") * 8; # 5 pixels-ish per char
	my $tick_width = 4;

	my $dy = $max-$min;

	return 0 if $dy == 0;

	my $scale_y = $h/$dy;

	# At least 15 pixels between ticks
	my $max_ticks = $h / 15 > 20 ? 20 : int($h / 15);
	$max_ticks = 2 if $max_ticks < 2;
	my $min_ticks = int($max_ticks/2);
	$min_ticks = 2 if $min_ticks < 2;

	my $divisor = y_axis_divisor($dy, $min_ticks, $max_ticks);

	$svg->appendChild( my $ctx = dataElement( 'g', undef, {
		transform => "translate($x $y) scale(1 1)",
		style => 'fony-family: sans-serif; font-size: 12px;',
		fill => $self->{colors}->{y_axis},
	}));

	my $step = int($dy/$divisor) || 1;
	
	my $i;
	for($i = 0; $i <= $max; $i += $step ) {
		$self->svg_y_tick( $ctx, $w, ($dy-$i)*$scale_y, $i );
	}
	
	return $w;
}

sub svg_y_axis_right
{
	my( $self, $svg, $min, $max, $x, $y, $h ) = @_;

	my $w = 4 + length("$max") * 8; # 5 pixels-ish per char
	my $tick_width = 4;

	my $dy = $max-$min;

	return 0 if $dy == 0;

	my $scale_y = $h/$dy;

	# At least 15 pixels between ticks
	my $max_ticks = $h / 15 > 20 ? 20 : int($h / 15);
	$max_ticks = 2 if $max_ticks < 2;
	my $min_ticks = int($max_ticks/2);
	$min_ticks = 2 if $min_ticks < 2;

	my $divisor = y_axis_divisor($dy, $min_ticks, $max_ticks);

	$svg->appendChild( my $ctx = dataElement( 'g', undef, {
		transform => "translate($x $y) scale(1 1)",
		style => 'fony-family: sans-serif; font-size: 12px;',
		fill => $self->{colors}->{y_axis_right},
	}));

	my $step = int($dy/$divisor) || 1;
	
	my $i;
	for($i = 0; $i <= $max; $i += $step ) {
		$self->svg_y_tick_right( $ctx, $w, ($dy-$i)*$scale_y, $i );
	}
	
	return $w;
}

sub svg_x_axis
{
	my( $self, $svg, $pts, $x, $y, $w, $h, $opts ) = @_;
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
		fill => $self->{colors}->{x_axis},
		style => 'font-family: sans-serif; font-size: 12px',
	}));

	for(my $i = 1; $i < $#$pts; $i++) {
		next unless $show[$i];

		$ctx->appendChild( dataElement( 'rect', undef, {
			x => $i*$scale_x+.5*$scale_x,
			y => -1*$h,
			width => 1,
			height => 5
		}));
			
		$ctx->appendChild( dataElement( 'text', $pts->[$i], {
			x => $i*$scale_x+.5*$scale_x,
			style => 'text-align:center;text-anchor:middle;',
		}));
	}

	# Left-align the first point
	$ctx->appendChild( dataElement( 'rect', undef, {
		x => .5*$scale_x,
		y => -1*$h,
		width => 1,
		height => 5
	}));
	
	if( $x > length($pts->[0]) * 8 / 2 )
	{
		$ctx->appendChild( dataElement( 'text', $pts->[0], {
			x => .5*$scale_x,
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
		x => $#$pts*$scale_x+.5*$scale_x,
		y => -1*$h,
		width => 1,
		height => 5
	}));
			
	$ctx->appendChild( dataElement( 'text', $pts->[$#$pts], {
		x => $w,
		style => 'text-align:right;text-anchor:end;',
	}));
	
	return $h;
}

sub svg_log_y_plot_series
{
	my( $self, $svg, $l, $labels, $data, $max, $x, $y, $w, $h ) = @_;
	
	return 0 if $max <= 0 or @$data == 0;
	
	$max = log10($max+1);

	my $scale_x = $w/@$data;
	my $scale_y = $h/$max;
	
	my $color_max = 1;
	for(@$data) {
		$color_max = $_ if $_ > $color_max;
	}
	
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
		my $r = int(255*$v/$color_max);
		my $b = 255-int(255*$v/$color_max);
		die("Error: colors went wrong ($v/$color_max): $r / $b") if $r > 255 or $b > 255;
		$v = log10($v+1);
		$plot->appendChild( dataElement( 'a', dataElement( 'rect', undef, {
						x => $i*$scale_x,
						y => ($max-$v)*$scale_y,
						width => $scale_x < 1 ? 1 : $scale_x,
						height => $v*$scale_y,
						fill => sprintf("#%02x00%02x",$r,$b),
						stroke => '#000',
						'stroke-width' => ($scale_x > 2 ? 1 : 0),
						}), {
					'xlink:href' => "$l",
					target => "_top",
					}));
	}
}

sub svg_log_y_plot_line
{
	my( $self, $svg, $l, $labels, $data, $max, $x, $y, $w, $h, $filled ) = @_;
	
	return 0 if $max <= 0;
	
	$max = log10($max+1);

	my $scale_x = $w/@$data;
	my $scale_y = $h/$max;
	
	$svg->appendChild( my $plot = dataElement( 'g', undef, {
		transform => "translate($x $y) scale(1 1)",
		id => "plot",
	}));
	my @points;
	for(my $i = 0; $i < @$data; $i++)
	{
		my $v = log10($data->[$i]+1);
		push @points, sprintf("%f,%f", ($i+.5)*$scale_x, ($max-$v)*$scale_y);
	}
	if( $filled )
	{
		push @points, sprintf("%f,%f", ($#$data+.5)*$scale_x, $max*$scale_y);
		push @points, sprintf("%f,%f", .5*$scale_x, $max*$scale_y);
		$plot->appendChild( dataElement( 'polyline', undef, {
				points => join(' ', @points),
				stroke => 'none',
				fill => '#fff',
				opacity => '.5',
			}));
	}
	else
	{
		$plot->appendChild( dataElement( 'polyline', undef, {
				points => join(' ', @points),
				stroke => '#080',
				fill => 'none',
				'stroke-width' => ($h / 100 > 2 ? $h / 100 : 2),
			}));
	}
}

sub svg_plot_series
{
	my( $self, $svg, $l, $labels, $data, $max, $x, $y, $w, $h ) = @_;
	
	return 0 if @$data == 0;

	my $scale_x = $w/@$data;
	my $scale_y = $h/$max;
	
	my $color_max = 1;
	for(@$data) {
		$color_max = $_ if $_ > $color_max;
	}
	
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
		my $r = int(255*$v/$color_max);
		my $b = 255-int(255*$v/$color_max);
		$plot->appendChild( dataElement( 'a', dataElement( 'rect', undef, {
						x => $i*$scale_x,
						y => ($max-$v)*$scale_y,
						width => ($scale_x < 1 ? 1 : $scale_x),
						height => $v*$scale_y,
						fill => sprintf("#%02x00%02x",$r,$b),
						stroke => '#000',
						'stroke-width' => ($scale_x > 2 ? 1 : 0),
						}), {
					'xlink:href' => "$l",
					target => "_top",
					}));
	}
}

sub svg_plot_line
{
	my( $self, $svg, $l, $labels, $data, $max, $x, $y, $w, $h, $filled ) = @_;
	
	return 0 if @$data == 0;

	my $scale_x = $w/@$data;
	my $scale_y = $h/$max;
	
	$svg->appendChild( my $plot = dataElement( 'g', undef, {
		transform => "translate($x $y) scale(1 1)",
		id => "plot",
	}));
	my @points;
	for(my $i = 0; $i < @$data; $i++)
	{
		my $v = $data->[$i];
		push @points, sprintf("%f,%f", ($i+.5)*$scale_x, ($max-$v)*$scale_y);
	}
	if( $filled )
	{
		push @points, sprintf("%f,%f", ($#$data+.5)*$scale_x, $max*$scale_y);
		push @points, sprintf("%f,%f", .5, $max*$scale_y);
		$plot->appendChild( dataElement( 'polyline', undef, {
				points => join(' ', @points),
				stroke => 'none',
				fill => '#fff',
				opacity => '.5',
			}));
	}
	else
	{
		$plot->appendChild( dataElement( 'polyline', undef, {
				points => join(' ', @points),
				stroke => '#080',
				fill => 'none',
				'stroke-width' => ($h / 100 > 2 ? $h / 100 : 2),
			}));
	}
}

sub day_inc
{
	my( $self, $d ) = @_;
	my @t = gmtime(
		timegm(0,0,0,substr($d,6,2),substr($d,4,2)-1,substr($d,0,4)-1900) +
		86400); # 1 Day
	return sprintf("%d%02d%02d", 1900+$t[5], 1+$t[4], $t[3]);
}

sub day_dec
{
	my( $self, $d ) = @_;
	my @t = gmtime(
		timegm(0,0,0,substr($d,6,2),substr($d,4,2)-1,substr($d,0,4)-1900) -
		86400); # 1 Day
	return sprintf("%d%02d%02d", 1900+$t[5], 1+$t[4], $t[3]);
}

sub month_inc
{
	my( $self, $d ) = @_;
	my( $y, $m ) = (substr($d,0,4),substr($d,4,2));
	if( $m == 12 )
	{
		$y++;
		$m = 1;
	}
	else
	{
		$m++;
	}
	return sprintf("%d%02d", $y, $m);
}

sub month_dec
{
	my( $self, $d ) = @_;
	my( $y, $m ) = (substr($d,0,4),substr($d,4,2));
	if( $m == 1 )
	{
		$y--;
		$m = 12;
	}
	else
	{
		$m--;
	}
	return sprintf("%d%02d", $y, $m);
}

sub year_inc
{
	return $_[1]+1;
}

sub year_dec
{
	return $_[1]-1;
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

=item y_axis_max MAX, LOGARITHMIC

Round up MAX to 1s.f.

=cut

sub y_axis_max
{
	my( $m, $log ) = @_;
	return 1 if $m < 1;
	return 10 ** length($m) if $log;
	my $frac = substr($m,1);
	return $m if $frac == 0;
	substr($m,0,1)++;
	return $m - $frac;
}

=item y_axis_divisor MAX, LOWER, UPPER

Find a divisor between LOWER and UPPER that maximises the number of zeroes in the fractions (e.g. MAX=800, LOWER=5, UPPER=10 will return 8 because 800/8 = 100).

=cut

sub y_axis_divisor
{
	my($m, $l, $u) = @_;
	my $best = 0;
	my $magic = $u;
	for($l..$u)
	{
		my $frac = $m / $_;
		next if int($frac) != $frac;
		$frac =~ /(0*)$/;
		if( length($1) >= $best )
		{
			$best = length($1);
			$magic = $_;
		}
	}
	return $magic;
}

1;
