#!/usr/bin/perl

$a="test.pl";
$b="../test.pl";

symlink($a, $b);
