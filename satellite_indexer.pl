#!/usr/bin/perl

use DBI;
use strict;
use threads;
use Thread::Queue;
use warnings;
use JSON;
use File::Path qw(make_path remove_tree);
use UUID::Tiny ':std';
use File::Find;
use File::Copy;
use File::Touch;
use Getopt::Std;
use Digest::SHA;
use Digest::MD5;
use File::Slurp;
use Data::Dumper;
use File::Basename;
use Time::Stopwatch;
use Number::Bytes::Human qw(format_bytes);
tie my $timer, 'Time::Stopwatch';
#use Digest::xxHash qw[xxhash64];

my $carefulness = 1; # make this 20ish;

my $conveyor_belt = Thread::Queue->new();
my @hash_threads;

my $hostname = `hostname`;
chomp($hostname);
my $btsync_path = '/home/frank/CommonPeople/' . $hostname . '/';

my %operands;
while (1) 
  {
  opendir my $dir, $btsync_path or die "Cannot open directory: $!";
  my @files = readdir $dir;
  closedir $dir;

  my %files = map {$_ => 1} @files ;
  foreach my $key (keys(%files))
    {
    next if $key =~ /\.{1,2}/;
    if (defined($operands{$key}))
      {
      if ($operands{$key}->{'found'} > (time - ($carefulness * 60)))
        {
        my $json_data_file = $btsync_path . $key . '/' . 'ready';
        my $data = decode_json(read_file($json_data_file));
	my $path = $btsync_path . $key . '/' . $data->{'file'};

	print Dumper $data, "\n";
	}
      }
    else
      {
      $operands{$key} = {found => time};
      }
    }


#foreach my $file (@files)
  #{
  #next if ($file =~ /^\.{1,2}$/);
  #my $job_dir = $btsync_path . $file;
  #opendir my $dir, $job_dir or die "Cannot open directory: $!";
  #my @files = readdir $dir;
  #closedir $dir;
  #my %files = map {$_ => 1} @files ;
  #print Dumper \%files, "\n";
  #if ($files{'ready'})
    #{
    ##print $file, ": it's ready\n";
    #}
  #}

  sleep 5;
  }
