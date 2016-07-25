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
use Redis;

my $ttl = 60 * 15;

my $db = DBI->connect("DBI:mysql:database=indexor;host=10.10.10.1", "indexor");
my $redis = Redis->new;

my $dir_sql = "select * from directories where id = ?";
my $dir_query = $db->prepare($dir_sql);

my $sql = "select * from files where sha512_hex is not null order by rand() limit 0,1000000";
my $query = $db->prepare($sql);
$query->execute;
while (my $file = $query->fetchrow_hashref)
  {
  print yellow_brick_road($file->{'directory'}), "\n";
  }

sub yellow_brick_road
  {
  my $id = shift;
  #if ($redis->exists($id) && ($redis->ttl($id) == -1)) { $redis->expire($id, $ttl); }
  #print $redis->ttl($id), "\n" if $redis->exists($id);

  return $redis->get($id) if $redis->exists($id);
  $dir_query->execute($id);  
  my $dir = $dir_query->fetchrow_hashref;
  my @path = ($dir->{'name'});
  while ($dir->{'depth'})
    {
    $dir_query->execute($dir->{'parent'});  
    $dir = $dir_query->fetchrow_hashref;
    unshift(@path, $dir->{'name'});
    }
  my $path = '/' . join('/',@path);
  $redis->setex($id, $ttl, $path);
  return $path;
  }



