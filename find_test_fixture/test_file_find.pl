#!/usr/bin/perl

use strict;
use Data::Dumper;
use File::Find;

find( { wanted => \&wanted, preprocess => \&preprocess }, '.');

sub wanted
  {
  my $complete_pathname = $File::Find::name;
  my $dir = $File::Find::dir;
  my $filename = $_;
  
  #print $name, "\n";
  #print $dir, "\n";
  #print $base, "\n";
  }

sub preprocess
  {
  my @stuff = @_;

  print "Preprocess: ", $File::Find::dir, "\n";

  my @real_stuff = ();

  foreach my $item (@stuff)
    {
    next if $item =~ /pl$/;
    push @real_stuff, $item if ($item =~ /\w/);
    }

  print Dumper \@real_stuff, "\n";
   
  return @real_stuff;
  }