spmux
=====

Simplified pmux (Experimental)

###Setups

- Prepare storage node list file: nodes.txt
- Setup public key ssh authentication from a user on client to the same user on all storage nodes.
- Setup the user as NOPASSWORD sudoer on all storage nodes.
- Mount the glsuterfs volume on /mnt/gluster (at client).
- $ sudo mkdir -m 777 /mnt/gluster/jobs (at client).
- Prepare your own mapper/reducer/key_hash with your favorite language in ~/libs

**Wordcount example**

libs/wc_hash.sh

	#!/bin/sh
	cut -f1 | md5sum | cut -d" " -f1

libs/wc_mapper.pl

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

libs/wc_reducer.pl

	#!/usr/bin/perl
	while (<>) {
	    $_ =~ m/(.*)\t(\d+)/;
	    $count{ $1 } += $2;
	}
	
	foreach $word ( keys( %count ) ) {
	    print "$word\t$count{ $word }\n";
	}

###Usage

$ spmux_run.pl -v volume_name -p 'filepath_pattern' -m mapper -r reducer [-k key_hash] -d lib_dir -n nodelist_file

*Example*

$  ./spmux_run.pl -v vol00 -p 'data00/*.txt' -m wc_mapper.pl -r wc_reducer.pl -k wc_hash.sh -d ~/libs/ -n nodes.txt 

