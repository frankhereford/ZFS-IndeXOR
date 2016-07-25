#!/usr/bin/perl

use DBI;
use strict;
use threads;
use Thread::Queue;
use warnings;
use JSON;
use File::Path qw(make_path remove_tree);
use UUID::Tiny ':std';;
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
#use Digest::xxHash qw[xxhash64];
tie my $timer, 'Time::Stopwatch';

use File::Basename;

my $conveyor_belt = Thread::Queue->new();
my @hash_threads;
my $queue_max_size = 25000;
my %options;
my $expose_jobs = 20;

getopts('th:e', \%options);

my $db = DBI->connect("DBI:mysql:database=indexor;host=10.10.10.1", "indexor");

my $progress_sql = <<SQL;
SELECT 
    COUNT(id) AS all_files,
    (SELECT 
            COUNT(id)
        FROM
            files
        WHERE
            sha512_hex IS NULL) AS undone_files,
    (SELECT 
            COUNT(id)
        FROM
            files
        WHERE
            sha512_hex IS NOT NULL) AS done_files,
    CONCAT(ROUND((((SELECT 
                            COUNT(id)
                        FROM
                            files
                        WHERE
                            sha512_hex IS NOT NULL) / (SELECT 
                            COUNT(id)
                        FROM
                            files
                        WHERE
                            sha512_hex IS NULL)) * 100),
                    3),
            '%') AS percent_done,
    ROUND(AVG(digest_compute_time), 2) AS average_compute_time,
    ROUND((SELECT 
                    AVG(digest_compute_time)
                FROM
                    files
                WHERE
                    sha512_hex IS NOT NULL
                        AND digested_time > (NOW() - INTERVAL 30 MINUTE)),
            2) AS recent_average_compute_time
FROM
    files;
SQL

my $progress_query = $db->prepare($progress_sql);

my $x = 19;
my $difference = 0;
while ($x++)
  {
  $progress_query->execute();
  my $progress = $progress_query->fetchrow_hashref;
  print "Seconds\tTotal\t\tRemaining\tComplete\tDifference\tPercent Complete\tAverage Compute Time\tRecent Compute Time (30 Min)\n" unless $x % 20;
  print int($timer);
  $timer = 0;
  print "\t";
  print $progress->{'all_files'};
  print "\t";
  print $progress->{'undone_files'};
  print "\t";
  print $progress->{'done_files'};
  print "\t";
  print "\t";
  unless ($difference)
    {
    $difference = $progress->{'remaining'};
    print "\t";
    }
  else
    {
    $difference = $difference - $progress->{'remaining'};
    $difference += 0;
    print $difference;
    }
  print "\t";
  print "\t";
  print $progress->{'percent_done'};
  print "\t";
  print "\t";
  print "\t";
  print $progress->{'average_compute_time'};
  print "\t";
  print "\t";
  print "\t";
  print $progress->{'recent_average_compute_time'};

  print "\n";
  sleep 60 * 5;
  }
