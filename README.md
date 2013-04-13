spmux
=====

Simplified pmux (Experimental)

Usage:
$ spmux_run.pl -v <volume name> -p '<filepath pattern>' -m <map_worker> -r <reduce_worker> -d <libdir> -n <nodelist file>

Example:
$ ./spmux_run.pl -v nfsvol -p 'data00/*.txt' -m wc_mapper.pl -r wc_reducer.pl -d ~/libs/ -n nodes.txt 

Mapper and Reducer (wc_mapper.pl and wc_reduce.pl in this case) should be under <libdir> (~/libs/ in this case).
