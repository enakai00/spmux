#!/usr/bin/perl

while (<>) {
	$_ =~ m/(.*)\t(\d+)/;
	$count{ $1 } += $2;
}

foreach $word ( keys( %count ) ) {
	print "$word\t$count{ $word }\n";
}
