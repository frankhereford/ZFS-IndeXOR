#!/usr/bin/perl

use DBI;
use strict;
use threads;
use warnings;
use File::Find;
use Data::Dumper;
use File::Basename;

my $db = DBI->connect("DBI:mysql:database=indexor;host=10.10.10.1", "indexor");

my %how_to_get_to_mordor; # cuz we'll need to know, ya know.

my $insert_directory_sql = "insert into directories (parent, depth, name) values (?, ?, ?)";
my $insert_directory = $db->prepare($insert_directory_sql);
my $get_directory_sql = "select id, depth from directories where name = ? and parent = ?";
my $get_directory = $db->prepare($get_directory_sql);

do_not_dwell_on_what_has_passed_away_or_what_is_yet_to_be();

sub do_not_dwell_on_what_has_passed_away_or_what_is_yet_to_be
  {
  $db->do('TRUNCATE files;'); # it's TOTALLY smart to include TRUNCATE commands right at the top of your code. This has NEVER made me curse and holler. Never.
  $db->do('TRUNCATE directories;');

  my @filesystems = ();
  my $cmd = 'zfs list -o mountpoint -t filesystem';
  open(my $zfs, "-|", $cmd);
  <$zfs>;
  while (my $filesystem = <$zfs>)
    {
    next if $filesystem =~ /workspace/i;
    next unless $filesystem =~ /frank/i;
    chomp $filesystem;
    chop $filesystem if $filesystem =~ /\/$/;
    push @filesystems, $filesystem;
    }
  close $zfs;

  @filesystems = sort(@filesystems);

  my @depths = ();
  foreach my $filesystem (@filesystems)
    {
    my $depth = occurrences('/', $filesystem);
    $depths[$depth] = [] unless defined $depths[$depth];
    push @{$depths[$depth]}, $filesystem;
    }
  shift @depths; #WHY? WHY?! 

  %how_to_get_to_mordor = map { $_ => 1 } @filesystems; # make a map to mordor by um, mapping, the zfs array? Sure why not.

  #print Dumper \@depths, "\n";
  # so now we have this ....thing and we can go up through it backwards and know where we have been and were we ought not go.

  foreach my $depth (reverse(@depths))
    {
    foreach my $directory (@{$depth})
      {
      #print "\n\n\n";
      #print $directory, "\n";
      #print "Ready?\n";
      #<>;
      find({ wanted => \&where_is_my_gypsy_wife_tonight, preprocess => \&stubborn_as_hose_garbage_bags_that_time_cannot_decay },$directory);
      }
    }
  }

sub where_is_my_gypsy_wife_tonight
  {
  my $name = $File::Find::name;
  my $dir = $File::Find::dir;
  my $base = $_;

  my ($filename, $directory) = fileparse($name);

  if (-d $name)
    {
    my $id = &my_house_in_the_middle_of_the_street($name);
    print $id, ": ", $name, "\n";
    }
  #elsif (-e $name and -r $name)
    #{
    #print "Name: ", $name, "\n";
    #print "Dir: ", $dir, "\n";
    #print "Base: ", $base, "\n";
    #print "Filename: ", $filename, "\n";
    #print "Directory: ", $directory, "\n";
    #&insert_file($name);
    #}
  }

sub stubborn_as_hose_garbage_bags_that_time_cannot_decay # check to make sure we're not going down a rabbit hole we've been down before, by checking the map to mordor.
  {
  my @directory_contents = @_;
  #print Dumper \@directory_contents, "\n";
  #print Dumper \%how_to_get_to_mordor, "\n";
  my @good_things;
  foreach my $thing (@directory_contents)
    {
    next if $thing =~ /workspace/i; # nevergood;
    push @good_things, $thing unless $how_to_get_to_mordor{$thing}; 
    }
  return @good_things; # because who likes bad things.
  }



sub my_house_in_the_middle_of_the_street
  {
  #print "Welcome to zombocom\n";
  my $path = shift;
  my @path = split("/", $path);
  my $zeroth_before_the_slash_nothing_lived_here_ignore_this_long_variable_the_compiler_hates_us_both = shift(@path);
  my $directory = shift(@path);
  my $root_sql = "select id from directories where name = ? and depth = ?"; #" and parent = ?";
  my $root_query = $db->prepare($root_sql);
  $root_query->execute($directory, 0);
  my $root_result = $root_query->fetchrow_hashref;
  my $id;
  if ($root_result->{'id'})
    {
    $id = $root_result->{'id'};
    }
  else
    {
    my $insert_root_sql = "insert into directories (name, parent, depth) values (?, 0, 0)";
    my $insert_root_query = $db->prepare($insert_root_sql);
    $insert_root_query->execute($directory);
    $id = &last_id;
    }

  while (scalar(@path))
    {
    my $find_parent_sql = "select id, parent, depth from directories where id = ?";
    my $find_parent_query = $db->prepare($find_parent_sql);
    $find_parent_query->execute($id);
    my $parent = $find_parent_query->fetchrow_hashref;
    my $directory = shift(@path);
    my $child_directory_sql = "select id from directories where name = ? and parent = ?";
    my $child_directory_query = $db->prepare($child_directory_sql);
    $child_directory_query->execute($directory, $parent->{'id'});
    if (($id) = $child_directory_query->fetchrow_array) { }
    else
      {
      my $insert_child_sql = "insert into directories (name, parent, depth) values (?, ?, ?)";
      my $insert_child_query = $db->prepare($insert_child_sql);
      $insert_child_query->execute($directory, $parent->{'id'}, ($parent->{'depth'} + 1));
      $id = &last_id;
      }
    }
  return $id;
  }

sub last_id
  {
  my $last_id_sql = "SELECT LAST_INSERT_ID()";
  my $last_id_query = $db->prepare($last_id_sql);
  $last_id_query->execute();
  my ($id) = $last_id_query->fetchrow_array;
  return $id;
  }

sub occurrences 
  {
  # tip of hat: http://stackoverflow.com/users/74585/matthew-lock
  my( $x, $y ) = @_;

  my $pos = 0;
  my $matches = 0;

  while (1) 
    {
    $pos = index($y, $x, $pos);
    last if($pos < 0);
    $matches++;
    $pos++;
    }   

  return $matches;
  }

