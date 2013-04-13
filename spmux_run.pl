#!/usr/bin/perl

use strict;
use Getopt::Std;

my $Glustervol = "/mnt/gluster";
my $Jobid = $$;

my ( $Hasher, $Nodelist, $Mapper, $Reducer, $Volname, $Filepath, $Libdir );

sub options {
my %opts;

    getopts( "k:n:v:m:r:p:d:", \%opts );
    ( $Hasher, $Nodelist, $Mapper, $Reducer, $Volname, $Filepath, $Libdir )
        = ( $opts{'k'}, $opts{'n'}, $opts{'m'},
            $opts{'r'}, $opts{'v'}, $opts{'p'}, $opts{'d'} );

    unless ( $Mapper && $Reducer && $Volname && $Filepath && $Libdir ) {
        print <<EOF;
Usage: spmux_run.pl -v volume_name -p 'filepath_pattern' -m map_worker -r reduce_worker [-k key_hasher] -d lib_dir -n nodelist 
EOF
        exit 0;
    }
}

sub wait_children {
my $pid;

    do {
        $pid = wait;
        print "$pid finished.\n" unless ($pid == -1);
    } while ( $pid != -1 );
}

sub run_reducers {
my $node;

    open ( IN,  "<$Nodelist" );
    while ( $node = <IN> ) {
        chomp $node;
        unless ( fork() ) {
            # child
            system ( "ssh -t $node \"$Libdir/spmux.pl -v $Volname -p '$Filepath' -j $Jobid -w $Libdir/$Reducer reduce\" >>logs/$Jobid-$node.log 2>&1" );
            system ( "stty sane" );
            exit 0;
        }
    }
    close IN;

    # parent
    wait_children();
}

sub run_mappers {
my $node;

    open ( IN,  "<$Nodelist" );
    while ( $node = <IN> ) {
        chomp $node;
        unless ( fork() ) {
            # child
            system ( "ssh $node \"rm -rf $Libdir; mkdir -p $Libdir\" >/dev/null 2>&1" );
            system ( "scp $Libdir/* $node:$Libdir/ >/dev/null 2>&1" );
            $Hasher = "-k $Libdir/$Hasher" if ( $Hasher );
            system ( "ssh -t $node \"$Libdir/spmux.pl -v $Volname -p '$Filepath' -j $Jobid -w $Libdir/$Mapper $Hasher map\" >>logs/$Jobid-$node.log 2>&1" );
            system ( "stty sane" );
            exit 0;
        }
    }
    close IN;

    # parent
    wait_children();
}

MAIN: {
    options();
    print "Jobid: $Jobid\n";
    print "Mapper and reducer results are placed in $Glustervol/jobs/$$/(mapped|reduced)\n";

    system ( "mkdir -p logs" );
    system ( "rm -rf $Glustervol/jobs/$Jobid" );
    system ( "mkdir -p $Glustervol/jobs/$Jobid/mapped" );
    system ( "mkdir -p $Glustervol/jobs/$Jobid/reduced" );
    system ( "mkdir -p $Glustervol/jobs/$Jobid/status/map" );
    system ( "mkdir -p $Glustervol/jobs/$Jobid/status/reduce" );

    print "start map phase...\n";
    run_mappers();
    print "start reduce phase...\n";
    run_reducers();
}

# vi:ts=4
