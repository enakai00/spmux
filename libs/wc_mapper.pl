#!/usr/bin/perl

while (<>) {
	@words = split( /\s+/, $_ );
	foreach $word ( @words ) {
		$count{ $word }++;
	}
}

foreach $word ( keys( %count ) ) {
	print "$word\t$count{ $word }\n";
}
