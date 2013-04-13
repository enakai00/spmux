#!/usr/bin/perl

use strict;
use Getopt::Std;
use Digest::MD5 qw( md5_hex );

my $Glustervol = "/mnt/gluster";
my ( $Worker, $Volname, $Filepath, $Jobid, $Brickdir, $Operation );

sub options {
my %opts;

    getopts( "v:w:p:j:", \%opts );
    ( $Worker, $Volname, $Filepath, $Jobid )
        = ( $opts{'w'}, $opts{'v'}, $opts{'p'}, $opts{'j'} );
    $Brickdir =
        `sudo gluster vol info $Volname | grep \$(hostname) | cut -d":" -f3`;
    chomp $Brickdir;

    $Operation = $ARGV[ 0 ];

    unless ( $Worker && $Volname && $Filepath && $Jobid && $Brickdir &&
            $Operation =~ m/(map|reduce)/ ) {
        print <<EOF;
Usage: spmux_mapper.pl -v <volume name> -p '<filepath pattern>' -w <worker> -j <jobid> [map|reduce]
EOF
        exit 0;
    }
}

sub reducer {
my ( $outfile, $line, $statfile, $volumepath );

    while ( <$Brickdir/jobs/$Jobid/mapped/*> ) {
        $_ =~ m|$Brickdir/(.*)|;
        $volumepath = "$Glustervol/$1";
        $_ =~ m|.*/([^/]+)|;
        $statfile = "$Glustervol/jobs/$Jobid/status/reduce/$1";
        $outfile = "$Glustervol/jobs/$Jobid/reduced/$1";

        if ( -f $statfile ) {
            print "$_ is alredy processed\n";
            next;
        }
        open ( IN, "<$volumepath" );
        unless ( flock ( IN, 6 ) ) {    # non-blocking write lock.
            close IN;
            print "$_ is now being processed on another node\n";
            next;
        }

        print "Succeeded to lock $_\n";
        open ( JOB, "$Worker $_|" );
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
my ( $volumepath, $hashkey, $line, $statfile );

    while ( <$Brickdir/$Filepath> ) {
        $_ =~ m|$Brickdir/(.*)|;
        $volumepath = "$Glustervol/$1";
        $_ =~ m|.*/([^/]+)|;
        $statfile = "$Glustervol/jobs/$Jobid/status/map/$1";
        if ( -f $statfile ) {
            print "$_ is alredy processed\n";
            next;
        }

        open ( IN, "<$volumepath" );
        unless ( flock ( IN, 6 ) ) {    # non-blocking write lock.
            close IN;
            print "$_ is now being processed on another node\n";
            next;
        }

        system ( "echo \"Succeeded to lock $_\n\"" );
        open ( RESULT, "$Worker $_ |" );
        while ( $line = <RESULT> ) {
            $hashkey = md5_hex( $line );
            open ( OUT, ">>$Glustervol/jobs/$Jobid/mapped/$hashkey" );
            flock ( OUT, 2 );   # blocking write lock.
            print OUT $line;
            close OUT;
        }
        close RESULT;

        system ( "touch $statfile" );   
        close IN;   # Releasing the lock.
    }
}

MAIN: {
    options();
    printf ( "Operation:%s\nWorker:%s\nVolume:%s\nFilepath:%s\nJobID:%s\nBrickdir:%s\n",
            $Operation, $Worker, $Volname, $Filepath, $Jobid, $Brickdir );

    system ( "sudo mkdir -p $Glustervol" );
    system ( "sudo mount -t glusterfs localhost:$Volname $Glustervol" );

    mapper() if ( $Operation eq "map" );
    reducer() if ( $Operation eq "reduce" );
}

# vi:ts=4
