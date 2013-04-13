spmux
=====

Simplified pmux (Experimental)

Usage:
$ spmux_run.pl -v volume_name -p 'filepath_pattern' -m map_worker -r reduce_worker -d lib_dir -n nodelist_file

Example:
$ ./spmux_run.pl -v vol00 -p 'data00/*.txt' -m wc_mapper.pl -r wc_reducer.pl -d ~/libs/ -n nodes.txt 

Mapper and Reducer (wc_mapper.pl and wc_reduce.pl in this case) should be under lib_dir (~/libs/ in this case).
