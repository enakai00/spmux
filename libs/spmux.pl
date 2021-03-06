#!/usr/bin/perl

use strict;
use Getopt::Std;

my $Glustervol = "/mnt/gluster";
my ( $Hasher, $Worker, $Volname, $Filepath, $Jobid, $Brickdir, $Operation );

sub options {
my %opts;

    getopts( "k:v:w:p:j:", \%opts );
    ( $Worker, $Volname, $Filepath, $Jobid )
        = ( $opts{'w'}, $opts{'v'}, $opts{'p'}, $opts{'j'} );
    $Hasher = $opts{'k'} || "md5sum | cut -d' ' -f1";
    $Brickdir =
        `sudo gluster vol info $Volname | grep \$(hostname) | cut -d":" -f3`;
    chomp $Brickdir;

    $Operation = $ARGV[ 0 ];

    unless ( $Hasher && $Worker && $Volname && $Filepath && $Jobid
            && $Brickdir && $Operation =~ m/(map|reduce)/ ) {
        print <<EOF;
Usage: spmux_mapper.pl -v volume_name -p 'filepath_pattern' -w worker -k key_hash -j jobid [map|reduce]
EOF
        exit 0;
    }
}

sub reducer {
my ( $brickpath, $volumepath, $outfile, $statfile );
my ( $line );

    while ( $brickpath = <$Brickdir/jobs/$Jobid/mapped/*> ) {
        $brickpath =~ m|$Brickdir/(.*)|;
        $volumepath = "$Glustervol/$1";	# file path on GlusterFS volume
        $brickpath =~ m|.*/([^/]+)|;
        $statfile = "$Glustervol/jobs/$Jobid/status/reduce/$1";
        $outfile = "$Glustervol/jobs/$Jobid/reduced/$1";

        if ( -f $statfile ) {
            print "$brickpath is alredy processed\n";
            next;
        }
        open ( IN, "<$volumepath" );
        unless ( flock ( IN, 6 ) ) {    # non-blocking write lock.
            close IN;
            print "$brickpath is now being processed on another node\n";
            next;
        }

        print "Succeeded to lock $brickpath\n";
        open ( JOB, "$Worker $brickpath|" );
        while ( $line = <JOB> ) {
            open ( OUT, ">>$outfile" );
            flock ( OUT, 2 );   # blocking write lock.
            print OUT $line;
            close OUT;
        }
        close JOB;

        system ( "touch $statfile" );   
        close IN;   # Releasing the lock.
    }
}

sub mapper {
my ( $brickpath, $volumepath, $outfile, $statfile );
my ( $hashkey, $line );

    while ( $brickpath = <$Brickdir/$Filepath> ) {
        $brickpath =~ m|$Brickdir/(.*)|;
        $volumepath = "$Glustervol/$1";	# file path on GlusterFS volume
        $brickpath =~ m|.*/([^/]+)|;
        $statfile = "$Glustervol/jobs/$Jobid/status/map/$1";
        if ( -f $statfile ) {
            print "$brickpath is alredy processed\n";
            next;
        }

        open ( IN, "<$volumepath" );
        unless ( flock ( IN, 6 ) ) {    # non-blocking write lock.
            close IN;
            print "$brickpath is now being processed on another node\n";
            next;
        }

        print "Succeeded to lock $brickpath\n";
        open ( JOB, "$Worker $brickpath |" );
        while ( $line = <JOB> ) {
            $hashkey = `echo -n "$line" | $Hasher`; chomp $hashkey;
            open ( OUT, ">>$Glustervol/jobs/$Jobid/mapped/$hashkey" );
            flock ( OUT, 2 );   # blocking write lock.
            print OUT $line;
            close OUT;
        }
        close JOB;

        system ( "touch $statfile" );   
        close IN;   # Releasing the lock.
    }
}

MAIN: {
    options();
    printf ( "Operation:%s\nHasher:%s\nWorker:%s\nVolume:%s\nFilepath:%s\nJobID:%s\nBrickdir:%s\n",
            $Operation, $Hasher, $Worker, $Volname, $Filepath, $Jobid, $Brickdir );

    system ( "sudo mkdir -p $Glustervol" );
    system ( "sudo mount -t glusterfs localhost:$Volname $Glustervol" );

    mapper() if ( $Operation eq "map" );
    reducer() if ( $Operation eq "reduce" );
}

# vi:ts=4
