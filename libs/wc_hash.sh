#!/bin/sh

cut -f1 | md5sum | cut -d" " -f1

