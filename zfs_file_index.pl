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

use File::Basename;

my $conveyor_belt = Thread::Queue->new();
my @hash_threads;
my $queue_max_size = 25000;
my %options;
my $expose_jobs = 20;

getopts('th:e', \%options);

my $db = DBI->connect("DBI:mysql:database=indexor;host=10.10.10.1", "indexor");

my %how_to_get_to_mordor; # cuz we'll need to know, ya know.

my $insert_directory_sql = "insert into directories (parent, depth, name) values (?, ?, ?)";
my $insert_directory = $db->prepare($insert_directory_sql);
my $get_directory_sql = "select id, depth from directories where name = ? and parent = ?";
my $get_directory = $db->prepare($get_directory_sql);
my $insert_file_sql = "insert into files (filename, directory) values (?, ?)";
my $insert_file = $db->prepare($insert_file_sql);
my $how_many_left_sql = "select count(id) as count_dracula from files where size is null";
my $how_many_left_query = $db->prepare($how_many_left_sql);

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
    files

SQL

my $progress_query = $db->prepare($progress_sql);

unless ($options{'t'} or $options{'s'} or $options{'h'})
  {
  print <<YOUNEEDHELP;

SO, yea, here ya go:

-t will /t/raverse the ZFS filesystems and stuff all the directory structure and files into the database. You set that up right, right? BTW, this blocks.
-h <thread_count> will start the thread launcher for the hashing and sizer threads. 
-e will empty the database and you'll be starting over fresh

YOUNEEDHELP
  exit;
  }

if ($options{'e'})
  {
  $db->do('TRUNCATE files;');
  $db->do('TRUNCATE directories;');
  }

do_not_dwell_on_what_has_passed_away_or_what_is_yet_to_be() if $options{'t'};
nothing_which_we_are_to_perceive_in_this_world_equals() if $options{'h'};

sub nothing_which_we_are_to_perceive_in_this_world_equals
  {
  for (my $x = 0; $x < $options{'h'}; $x++)
    {
    my $thread = threads->create( \&the_blender);
    push @hash_threads, \$thread;
    }

  #my $thread = threads->create( \&congress_hasher);
  #push @hash_threads, \$thread;

  while (1)
    {
    $how_many_left_query->execute();
    my ($remaining_files) = $how_many_left_query->fetchrow_array;
    last unless $remaining_files;
    my $items_in_queue = $conveyor_belt->pending();
    if ($items_in_queue < $queue_max_size)
      {
      my $rows_needed = $queue_max_size - $items_in_queue;
      next if $rows_needed <= 0;


      my $get_files_to_work_sql = "select id from files where size is null limit 0, " . $rows_needed;
      my $get_files_to_work_query = $db->prepare($get_files_to_work_sql);
      $get_files_to_work_query->execute();
      #for (my $x = 0; $x < $rows_needed; $x++)
      while (my ($id) = $get_files_to_work_query->fetchrow_array)
        {
        $conveyor_belt->enqueue($id);
        }
      $progress_query->execute();
      my $info = $progress_query->fetchrow_hashref;

      print "Remaining to do: ", $remaining_files, "\n";
      print "Rows needed for queue: ", $rows_needed, "\n\n";

      print "All file count: ", $info->{'all_files'}, "\n";
      print "Hashed files: ", $info->{'done_files'}, "\n";
      print "Percent coplete: ", $info->{'percent_done'}, "\n";
      print "Average compute time: ", $info->{'average_compute_time'}, "\n";
      print "Recente average compute time: ", $info->{'recent_average_compute_time'}, "\n";
      print "\n";
      }
    #sleep 60 * 3;
    sleep 30;
    }

  $conveyor_belt->end();

  foreach my $thread (@hash_threads)
    {
    ${$thread}->join();
    }
  }

sub congress_hasher
  {
  my $db = DBI->connect("DBI:mysql:database=indexor;host=10.10.10.1", "indexor");
  tie my $timer, 'Time::Stopwatch';
  my $find_file_sql = "select directory, filename, id from files where id = ?";
  my $find_file_query = $db->prepare($find_file_sql);
  my $get_directory_build_sql = "select id, name, depth, parent from directories where id = ?";
  my $get_directory_build = $db->prepare($get_directory_build_sql);
  my $update_files_with_hash_size_sql = "update files set size = ? where id = ?";
  my $update_files_with_hash_size = $db->prepare($update_files_with_hash_size_sql);

  my $jobs = 0; 

  while (defined(my $id = $conveyor_belt->dequeue())) 
    {
    my $sha = Digest::SHA->new('sha512');
    $find_file_query->execute($id);
    my $file = $find_file_query->fetchrow_hashref;
    my @path = ($file->{'filename'});
    $get_directory_build->execute($file->{'directory'});
    my $d = $get_directory_build->fetchrow_hashref;
    unshift(@path, $d->{'name'});
    while ($d->{'depth'} != 0)
      {
      $get_directory_build->execute($d->{'parent'});
      $d = $get_directory_build->fetchrow_hashref;
      unshift(@path, $d->{'name'});
      }
    my $target = '/' . join('/', @path);
    print $target, ' ', format_bytes(-s $target), "\n";
    #print "Congressing: ", $target, "\n";
    my($filename, $dirs, $suffix) = fileparse($target);
    # shouldn't be on github but yea, my name is frank and I do have a an old Pulp album laying around..
    my $btsync_path = '/home/frank/CommonPeople/' . 'congress' . '/';
    my $dest_path = $btsync_path . create_uuid_as_string() . '/';
    make_path($dest_path);
    my $dest_file = $dest_path . $filename;
    $timer = 0;
    copy($target, $dest_file);
    print "Copy took ", sprintf('%.3f',$timer) , " seconds.\n";
    #touch($dest_path . 'ready');
    open(my $signal, ">", $dest_path . 'ready');
    my $data = { id => $id, file => $filename};
    print $signal encode_json($data);
    close $signal;

    opendir my $dir, $btsync_path or die "Cannot open directory: $!";
    my @files = readdir $dir;
    closedir $dir;
    my $open_jobs = scalar(@files) - 2;

    while ($open_jobs >= $expose_jobs)
      {
      opendir my $dir, $btsync_path or die "Cannot open directory: $!";
      my @files = readdir $dir;
      closedir $dir;
      $open_jobs = scalar(@files) - 2;
      #print "Currently exposing ", $open_jobs, " jobs.\n";


      #job clean up an file here.
      sleep 20;
      }

    print "\n";
    }
  }

sub the_blender
  {
  my $db = DBI->connect("DBI:mysql:database=indexor;host=10.10.10.1", "indexor");
  tie my $timer, 'Time::Stopwatch';
  my $find_file_sql = "select directory, filename, id from files where id = ?";
  my $find_file_query = $db->prepare($find_file_sql);
  my $get_directory_build_sql = "select id, name, depth, parent from directories where id = ?";
  my $get_directory_build = $db->prepare($get_directory_build_sql);
  my $update_files_with_hash_size_sql = "update files set size = ? where id = ?";
  my $update_files_with_hash_size = $db->prepare($update_files_with_hash_size_sql);

  while (defined(my $id = $conveyor_belt->dequeue())) 
    {
    my $sha = Digest::SHA->new('sha512');
    $find_file_query->execute($id);
    my $file = $find_file_query->fetchrow_hashref;
    my @path = ($file->{'filename'});
    $get_directory_build->execute($file->{'directory'});
    my $d = $get_directory_build->fetchrow_hashref;
    unshift(@path, $d->{'name'});
    while ($d->{'depth'} != 0)
      {
      $get_directory_build->execute($d->{'parent'});
      $d = $get_directory_build->fetchrow_hashref;
      unshift(@path, $d->{'name'});
      }
    my $target = '/' . join('/', @path);
    my $size = -s $target;
    #print $target, ' ', format_bytes($size), "\n";

    # SHA512
    $timer = 0;
    $sha->addfile($target); # Here is the heavy lifting. 
    my $digest_time = sprintf('%.8f',$timer);
    #print "File added in ", sprintf('%.3f', $timer), " seconds.\n";
    #$timer = 0;
    my $hex = $sha->hexdigest;
    #print "digest hex in ", sprintf('%.3f', $timer), " seconds.\n"; $timer = 0;
    #my $b64 = $sha->b64digest;
    #print "digest b64 in ", sprintf('%.3f', $timer), " seconds.\n"; $timer = 0;
    #my $sql = "update files set size = ?, sha512_b64 = ?, sha512_hex = ?, digest_compute_time = ?, digested_time = now() where id = ?";

=cut
    # MD5
    $timer = 0;
    open (my $fileh, '<', $target) or die "Can't open '$target': $!";
    binmode ($fileh);
    my $md5 = Digest::MD5->new->addfile($fileh)->hexdigest;
    close $fileh;
    my $digest_time = sprintf('%.8f',$timer);
    my $hex = $md5; # a little fib
=cut

    my $sql = "update files set size = ?, sha512_hex = ?, digest_compute_time = ?, digested_time = now() where id = ?";
    my $query = $db->prepare($sql);
    $query->execute($size, $hex, $digest_time, $id);

    #print $digest_time, ": ", $hex, "\n";
    }
  }

sub do_not_dwell_on_what_has_passed_away_or_what_is_yet_to_be
  {
  die "You can't be calling -t without some -e; it's just not right! I'm just giving you this line to comment out if you're into that.." unless $options{'e'};
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
  %how_to_get_to_mordor = map { $_ => 1 } @filesystems; # make a map to mordor by um, mapping, the zfs array? Sure why not.
  #print Dumper \%how_to_get_to_mordor, "\n";
  my @heights = reverse(@depths);
  foreach my $depth (@heights)
    {
    foreach my $directory (@{$depth})
      {
      find({wanted => \&where_is_my_gypsy_wife_tonight, preprocess => \&stubborn_as_those_garbage_bags_that_time_cannot_decay}, $directory);
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
    #print $id, ": ", $name, "\n";
    }
  elsif (-e $name and -r $name)
    {
    &friend_is_a_four_letter_word($name);
    }
  }

sub friend_is_a_four_letter_word # insert file
  {
  my $full_path = shift;
  my @yellow_brick_road = split(/\//,$full_path);
  my $filename = pop(@yellow_brick_road);
  my $parent = my_house_in_the_middle_of_the_street(join('/', @yellow_brick_road)); 
  $insert_file->execute($filename, $parent)
  }

sub stubborn_as_those_garbage_bags_that_time_cannot_decay # check to make sure we're not going down a rabbit hole we've been down before, by checking the map to mordor.
  {
  my @directory_contents = @_;

  my @good_things = ();
  foreach my $thing (@directory_contents)
    {
    next if $thing =~ /^\.{1,2}$/;
    my $full_path = $File::Find::dir . '/' . $thing;
    push @good_things, $thing unless ($how_to_get_to_mordor{$full_path});
    }

  return @good_things;
  }

sub my_house_in_the_middle_of_the_street
  {
  #print "Welcome to zombocom\n";
  my $path = shift;
  my @path = split("/", $path);
  my $zeroth_before_the_slash_nothing_lived_here_ignore_this_long_variable_the_compiler_hates_us_both = shift(@path);
  my $directory = shift(@path);
  my $root_sql = "select id from directories where name = ? and depth = ?";
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


