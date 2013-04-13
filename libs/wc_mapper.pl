#!/usr/bin/perl

while (<>) {
	@words = split( /\s+/, $_ );
	foreach $word ( @words ) {
		print "$word\t1\n";
	}
}
