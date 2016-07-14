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

  my %how_to_get_to_mordor = map { $_ => 1 } @depths; # make a map to mordor by um, mapping, the zfs array? Sure why not.

  print Dumper \@depths, "\n";
  # so now we have this ....thing and we can go up through it backwards and know where we have been and were we ought not go.

  foreach my $depth (reverse(@depths))
    {
    foreach my $directory (@{$depth})
      {
      print "\n\n\n";
      print $directory, "\n";
      print "Ready?\n";
      <>;
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
    print $name, "\n";
    #&insert_directory($name); # insert directory
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
  my @good_things;
  foreach my $thing (@directory_contents)
    {
    next if $thing =~ /workspace/i; # nevergood;
    push @good_things, $thing unless -d $thing and $how_to_get_to_mordor{$thing};
    }
  return @good_things; # because who likes bad things.
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

exit;
















my $root = '/';


my $sql;
$sql = "insert into directories (parent, depth, name) values (?, ?, ?)";
my $insert_directory = $db->prepare($sql);
$sql = "select id, depth from directories where name = ? and parent = ?";
my $get_directory = $db->prepare($sql);
$sql = "insert into files (filename, directory) values (?, ?)";
my $insert_file_stub = $db->prepare($sql);
$sql = "select id, name, depth, parent from directories where id = ?";
my $get_directory_build = $db->prepare($sql);
$sql = "update files set size = ?, md5 = ? where id = ?";
my $set_size_md5 = $db->prepare($sql);

for (my $x = 0; $x < 1000; $x++)
  {
  my $sql = "select directory, filename, id from files where size IS NULL and md5 IS NULL and unusual is NULL order by id asc limit 0,1000";
  my $query = $db->prepare($sql);
  $query->execute();
  while (my $file = $query->fetchrow_hashref)
    {
    #print $file->{'filename'}, "\n";
    my @path = ($file->{'filename'});
    $get_directory_build->execute($file->{'directory'});
    my $d = $get_directory_build->fetchrow_hashref;
    unshift(@path, $d->{'name'});
    while ($d->{'depth'} != 0)
      {
      #print "2\n";
      $get_directory_build->execute($d->{'parent'});
      $d = $get_directory_build->fetchrow_hashref;
      unshift(@path, $d->{'name'});
      }
    my $path = join('/', @path);

    print $path, "\n";
    next;
    if (-e $path and -f $path)
      {
      #print "3\n";
      my $size = -s $path;
      #print "Size: ", $size, "\n";
      #my $size = 0;
      #print "4\n";
      open (my $fileh, '<', $path) or die "Can't open '$path': $!";
      #print "4.5\n";
      binmode ($fileh);
      my $md5 = Digest::MD5->new->addfile($fileh)->hexdigest;
      close $fileh;
      #print "5\n";
      print 'Size: ', $size, '; MD5: ', $md5, "\n";
      #$set_size_md5->execute($size, $md5, $file->{'id'});
      }
    else
      {
      my $sql = "update files set unusual = 1 where id = ?";
      my $unusual = $db->prepare($sql);
      #$unusual->execute($file->{'id'});
      }
    }
  }



sub find_files
  {
  $db->do('TRUNCATE files;');
  $db->do('TRUNCATE directories;');
  my $cmd = 'zfs list -o mountpoint -t filesystem';
  open(my $zfs, "-|", $cmd);
  <$zfs>;
  close $zfs;



  foreach my $path (<$zfs>) 
    { 
    chomp $path;
    find(\&corkscrew,$path);
    }

  }

sub corkscrew
  {
  my $name = $File::Find::name;
  my $dir = $File::Find::dir;
  my $base = $_;

  my ($filename, $directory) = fileparse($name);

  if (-d $name)
    {
    &insert_directory($name);
    }
  elsif (-e $name and -r $name)
    {
    if (0)
      {
      print "Name: ", $name, "\n";
      print "Dir: ", $dir, "\n";
      print "Base: ", $base, "\n";
      print "Filename: ", $filename, "\n";
      print "Directory: ", $directory, "\n";
      }
    &insert_file($name);
    }
  }

sub insert_file
  {
  my $d = shift;
  $d = substr($d, length($root));
  my @path = split(/\//,$d);
  shift(@path); # leading slash leaves empty zeroth register
  my $filename = pop(@path);
  my $parent = get_parent_id(\@path,0);
  #print "Parent: ", $parent->{'id'}, "\n";
  $insert_file_stub->execute($filename, $parent->{'id'});
  }

sub insert_directory
  {
  my $d = shift;
  $d = substr($d, length($root));
  my @path = split(/\//,$d);
  shift(@path); # leading slash leaves empty zeroth register
  if (scalar(@path))
    {
    my $parent = get_parent_id(\@path);
    $insert_directory->execute($parent->{'id'},($parent->{'depth'} + 1),pop(@path));
    }
  else
    {
    $insert_directory->execute(0,0,$root);
    }
  }

sub get_parent_id
  {
  my $path = shift;
  my $up_depth = shift // 1;
  $get_directory->execute($root,0);
  my $parent = $get_directory->fetchrow_hashref;
  for (my $x = 0; $x < (scalar(@{$path})-$up_depth); $x++)
    {
    $get_directory->execute($path->[$x], $parent->{'id'});
    $parent = $get_directory->fetchrow_hashref;
    }
  print "Parent ID: ", $parent->{'id'}, "\n";
  return $parent;
  }