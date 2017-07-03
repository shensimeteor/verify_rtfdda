#!/usr/bin/perl 
#use strict;
#no strict 'refs';

# The following are imported from verifyinput.pl:
use Getopt::Std;
use File::Basename;
#parse cmd arg
$HOMEDIR=$ENV{HOME};
$ENSPROCS="$ENV{CSH_ARCHIVE}/ncl";
require "$ENSPROCS/common_tools.pl";

%hash_opt = &tool_get_cmdopt("no_value");
$CYCLE = $hash_opt{c};
$GMID = $hash_opt{id};
$MEMBER = $hash_opt{m};

if( ! ($CYCLE && $GMID && $MEMBER)) {
    print "<usage> $0 -id <GMID> -m <MEMBER> -c <THIS_CYCLE>  \n";
    exit(-1);
}

#define constant
$GSJOBDIR = "$HOMEDIR/data/GMODJOBS/$GMID/";
$RUNDIR = "$HOMEDIR/data/cycles/$GMID/";
$ARCDIR = "$HOMEIDR/data/cycles/$GMID/archive/";
$DEBUG = 1; 
$MM5HOME = "$HOMEDIR/fddahome";
$EXECUTABLE_ARCHIVE = "${MM5HOME}/cycle_code/EXECUTABLE_ARCHIVE";
$CSH_ARCHIVE = "${MM5HOME}/cycle_code/CSH_ARCHIVE";
$PERL_ARCHIVE = "${MM5HOME}/cycle_code/PERL";
$RANGE = "GRM";
$GSJOBID = $GMID;

#requires
$FLEXINPUT  =  $GSJOBDIR . "/flexinput.pl";  
print ("flexinput=$FLEXINPUT\n");
if (-e $FLEXINPUT)
{
  print "$0: Using job configuration in $FLEXINPUT\n" if ($DEBUG);
  require $FLEXINPUT;
}
else
{
 print "\nFile $FLEXINPUT is missing..... EXITING\n";
 exit(-1);
}
if ( -e  $GSJOBDIR."/verifyinput.pl" )
{
   require $GSJOBDIR."/verifyinput.pl";
} else {
   print ( " verifyinput.pl does not exist!  $GSJOBDIR \n");
   exit (1);
}

#qcout replace qc_values for THIS_CYCLE
($MY_GFS, $MY_NML) = split(/_/, $MEMBER);
($OBS_GFS, $OBS_NML) = split(/_/, $OBS_MEMBER);
if ( $MEMBER eq $OBS_MEMBER ){
    system("$GSJOBDIR/run_qcout.replace_qc_value.csh $HOMEDIR $GMID $CYCLE $OBS_GFS");
}


$RANGE= $MEMBER;

our $VERI_WORK_DIR;
our $VERI_PAIRS_DIR;
our $VERI_SFC;
our $VERI_UPR;
our $VERI_PLT;
our $VERI_HGT;
our $ADD_STID;
our $VERI_LENGTH;
our $VERI_INTERM;
our $VERI_LIST;
our $QC_CUT;
our @UPR_HOURS;
our @colors;
our %plot_range;
our $VERI_ARCHIVE_ROOT;

our $KEY;  ## Needed only if verification products are rsync'd to remote host.
our $DEST_SERVER;  ## Needed only if products are rsync'd to remote server.
our $JOB_LOC;

###
my $GMID;
my $NDOM;
my $VERI_DIR;
my $SAVEDIR;
my $i;
my $hrsRetro;
my @old_cycles;
my @old_cycles_q;
my @sfc_pairs_files;
my $domain;
my @stats_file;
my $var;
my @upr_pairs_files;
my $file;
my @stats_upr_file;
my $fn;
my $html_table;
my $upr_dir;
my $d;
my $html_doc;
my %stations;
my $key;
my $cyc;
my $valid_time;
my $oldCycle;
my $upr_plot;
my @upr_plots;

######

print "PID: $$\n";

require  $PERL_ARCHIVE."/TimeUtil.pm";

require "ctime.pl";

$GMID=$GSJOBID;

$OBSDIR= "$RUNDIR/$OBS_MEMBER";
my $MMOUTDIR=$RUNDIR;
print("VERI_WORK_DIR=$VERI_WORK_DIR\n");
if ($VERI_WORK_DIR) {
   $VERI_DIR  = "$VERI_WORK_DIR/$opt_c";  ######
   system("mkdir $VERI_DIR");
   print ("verify dir = $VERI_DIR\n");
} else {
   $VERI_DIR = "$RUNDIR/verify/$opt_c";
   system("mkdir $VERI_DIR");
}

$QCU_CUT=-1;
#$QCU_CUT=3;
if ($VERI_PAIRS_DIR) {
   $SAVEDIR = $VERI_PAIRS_DIR;
} else {
   $SAVEDIR = "$RUNDIR/veri_dat";
}

my $SAVE_DIR_SFC="$SAVEDIR/sfc";
my $SAVE_DIR_UPR="$SAVEDIR/upr";

my $UPR_STATS_TMP="$VERI_DIR/../.upr";

my $range = $RANGE;$range =~ tr/A-Z/a-z/;
my $RANGE_tag =$RANGE;

my $WebServer=$DEST_SERVER;
my $REMOTE_DIR_SFC="$JOB_LOC/veri_dat/sfc";  ######
my $REMOTE_DIR_UPR="$JOB_LOC/veri_dat/upr";  ######
my $web_images_dir = "$JOB_LOC/veri_images";  ######

if($NUM_DOMS) {
  $NDOM=$NUM_DOMS;
} else {
  $NDOM=5;
}

print "+++++++++++++ Start FDDA verification stats ++++++++++++++\n";

system("mkdir -p $SAVE_DIR_SFC/{final,fcst} $VERI_DIR");
if ($VERI_ARCHIVE_ROOT) {
   system("mkdir -p $VERI_ARCHIVE_ROOT/$ENV{LOGNAME}/$GSJOBID/veri_dat/sfc/{final,fcst}");
}
print("veri_dir=$VERI_DIR\n");
chdir $VERI_DIR;


print "        Cycle = $this_cycle  at ", &ctime(time);

if ( ! -e "$RUNDIR/${this_cycle}" ) {
   print "   \n The cycle ${this_cycle} does not exist \n. Something wrong. -- Exit.";
   exit (1);
}

my $date=$this_cycle;

print "Cycling interval is $CYC_INT\n";

if ($VERI_LENGTH) {
   print "VERIFY $VERI_LENGTH hours of forecast!\n";
} else {
   $VERI_LENGTH = 12;
}

my $numOldCycles = $VERI_LENGTH/$CYC_INT;

my $oldestCycle = date_retro($date,$numOldCycles*$CYC_INT);

my $year = substr $date, 0, 4;
my $mons = substr $date, 4, 2;
my $days = substr $date, 6, 2;

$date1 = $year . $mons . $days . "12";
$date2 = $year . $mons . $days . "00";

foreach $i (1..$numOldCycles) {
  $hrsRetro = $i*$CYC_INT;
  push(@old_cycles,date_retro($date,$hrsRetro));
  push(@old_cycles1,date_retro($date1,6));
	if( $i < 2 ) {
         push(@old_cycles_q,date_retro($date,$hrsRetro));
	}
}

if ($VERI_LIST) {
   &parse_stations();
   print "Use stations in $VERI_LIST only!\n";
   if ($DEBUG > 0) {
     foreach $key (keys %stations) {
       print "key = $key; value = $stations{$key}\n";
     }
   }
}

##### sfc stats/plot first
if ($VERI_SFC) {

  print "\ Start sfc stats generation \n";
  


  @sfc_pairs_files=save_sfc();

#
# In the working directory, create a directory using the current cycle name
# to hold the pictures.
#

  mkdir $date, 0755 if( ! -d "$date");

  foreach $domain (1..$NDOM) {

   @stats_file=stats($domain);
   print "stats_file = @stats_file\n";
   print("veri_plt=$VERI_PLT\n");
   if($VERI_PLT) { 
    #foreach $var ('t','rh','q','ws','wd','slp','psfc') {
     foreach $var ('t','rh','q','ws','wd','psfc') {
       &stats_plot($var,@stats_file);
     }
   }

  }

  if($VERI_PLT) {
#
# Create reomte directory on $WebServer and remote copy gifs to $WebServer
#
    if ($DEST_SERVER=~ /localhost/i) {
       system("mkdir -p $JOB_LOC/veri_images/sfc");
       system("cp $date $JOB_LOC/veri_images/sfc/.");
    } else {
       system("rsync -e 'ssh -i $KEY' -avzC $date $DEST_SERVER:$JOB_LOC/veri_images/sfc/.");
    }
# Create webpage and remote copy to $WebServer
    &html_create;

  }


# Remove stats ASCII file

#  unlink <stats_*> if ($DEBUG < 10);

# Remove the picture directory after it's remotely copied to $WebServer

#  unlink <$date/*> if ($DEBUG < 10);
#  rmdir $date if ($DEBUG < 10);

} # $VERI_SFC end

##### upper-air validation

if ($VERI_UPR) {
  print "\ Start upr stats generation \n";

  system("mkdir -p $SAVE_DIR_UPR/{final,fcst} $UPR_STATS_TMP");
  if ($VERI_ARCHIVE_ROOT) {
     system("mkdir -p $VERI_ARCHIVE_ROOT/$ENV{LOGNAME}/$GSJOBID/veri_dat/upr/{final,fcst}");
  }

  @upr_pairs_files=save_upr();
 #print "upper-air pairs files : @upr_pairs_files\n";

  foreach $domain (1..$NDOM) {
    $i=0;
    foreach $file (@upr_pairs_files) {
       $stats_upr_file[$i]=stats_upr($file,$domain);
       $i++;
    }

    if($VERI_PLT) {
      foreach $fn (@stats_upr_file) {
        $html_table=html_table_upr($fn);
        $html_table=~ /^(\d+)_(\d+)_d(\d+)/;
        $cyc = $1;
        $valid_time=$2;
        $d=$3;
       #$html_doc="${upr_dir}_d${d}.html";
       $html_doc="${valid_time}_d${d}.html";
       # $html_doc="${valid_time}_d${d}.html";
        system("mkdir -p $UPR_STATS_TMP/upr_tables/$valid_time");
      system("touch $UPR_STATS_TMP/upr_tables/$valid_time/$html_doc");
      print "cat $html_table >> $UPR_STATS_TMP/upr_tables/$valid_time/$html_doc\n";
      system("cat $html_table >> $UPR_STATS_TMP/upr_tables/$valid_time/$html_doc");
	unlink $html_table;
      &html_create_upr;
       #system("touch $UPR_STATS_TMP/$date/$upr_dir/$html_doc");
#        system("mv $html_table $UPR_STATS_TMP/upr_tables/$cyc/$html_doc");

        @upr_plots=stats_plot_upr($fn);
        system("mkdir -p $UPR_STATS_TMP/upr_plots/$cyc");
        foreach $upr_plot (@upr_plots) {
           system("mv $upr_plot $UPR_STATS_TMP/upr_plots/$cyc/.");
        }
      }

    }
  }  ## end foreach domain

  if ($DEST_SERVER=~ /localhost/i) {
     system("mkdir -p $JOB_LOC/veri_images/upr/upr_tables");
     system("mkdir -p $JOB_LOC/veri_images/upr/upr_plots");
     system("cp $UPR_STATS_TMP/upr_tables/${date} $JOB_LOC/veri_images/upr/upr_tables/.");
     system("cp $UPR_STATS_TMP/upr_plots/${date} $JOB_LOC/veri_images/upr/upr_plots/.");
     system("cp $UPR_STATS_TMP/upr_tables/${valid_time} $JOB_LOC/veri_images/upr/upr_tables/.");
     system("cp $UPR_STATS_TMP/upr_tables/${valid_time} $JOB_LOC/veri_images/upr/.");
     foreach $oldCycle (@old_cycles) {
        if (-d "$JOB_LOC/veri_images/upr_tables/$oldCycle") {
           system("cp $UPR_STATS_TMP/upr_tables/$oldCycle/* $JOB_LOC/veri_images/upr/upr_tables/$oldCycle/.");
        } else {
           system("cp $UPR_STATS_TMP/upr_tables/$oldCycle $JOB_LOC/veri_images/upr/upr_tables/.");
        }

        if (-d "$JOB_LOC/veri_images/upr/upr_plots/$oldCycle") {
           system("cp $UPR_STATS_TMP/upr_plots/$oldCycle/* $JOB_LOC/veri_images/upr/upr_plots/$oldCycle/.");
        } else {
           system("cp $UPR_STATS_TMP/upr_plots/$oldCycle $JOB_LOC/veri_images/upr/upr_plots/.");
        }
     }
     foreach $oldCycle (@old_cycles1) {
        if (-d "$JOB_LOC/veri_images/upr_tables/$oldCycle") {
           system("cp $UPR_STATS_TMP/upr_tables/$oldCycle/* $JOB_LOC/veri_images/upr/upr_tables/$oldCycle/.");
        } else {
           system("cp $UPR_STATS_TMP/upr_tables/$oldCycle $JOB_LOC/veri_images/upr/upr_tables/.");
           system("cp $UPR_STATS_TMP/upr_tables/$oldCycle $JOB_LOC/veri_images/upr/.");
        }

        if (-d "$JOB_LOC/veri_images/upr/upr_plots/$oldCycle") {
           system("cp $UPR_STATS_TMP/upr_plots/$oldCycle/* $JOB_LOC/veri_images/upr/upr_plots/$oldCycle/.");
        } else {
           system("cp $UPR_STATS_TMP/upr_plots/$oldCycle $JOB_LOC/veri_images/upr/upr_plots/.");
        }
     }
  } else {
     system("rsync -e 'ssh -i $KEY' -avzC $UPR_STATS_TMP/upr_tables/$date $DEST_SERVER:$JOB_LOC/veri_images/upr/upr_tables/.");
     system("rsync -e 'ssh -i $KEY' -avzC $UPR_STATS_TMP/upr_tables/$date1 $DEST_SERVER:$JOB_LOC/veri_images/upr/upr_tables/.");
     system("rsync -e 'ssh -i $KEY' -avzC $UPR_STATS_TMP/upr_tables/$date2 $DEST_SERVER:$JOB_LOC/veri_images/upr/upr_tables/.");
     system("rsync -e 'ssh -i $KEY' -avzC $UPR_STATS_TMP/upr_tables/$date1 $DEST_SERVER:$JOB_LOC/veri_images/upr/.");
     system("rsync -e 'ssh -i $KEY' -avzC $UPR_STATS_TMP/upr_tables/$date2 $DEST_SERVER:$JOB_LOC/veri_images/upr/.");
     system("rsync -e 'ssh -i $KEY' -avzC $UPR_STATS_TMP/upr_tables/$valid_time $DEST_SERVER:$JOB_LOC/veri_images/upr/upr_tables/.");
     system("rsync -e 'ssh -i $KEY' -avzC $UPR_STATS_TMP/upr_tables/$valid_time $DEST_SERVER:$JOB_LOC/veri_images/upr/.");

     system("rsync -e 'ssh -i $KEY' -avzC $UPR_STATS_TMP/upr_plots/$date $DEST_SERVER:$JOB_LOC/veri_images/upr/upr_plots/.");

#     system("rm -rf $UPR_STATS_TMP/$date");

     foreach $oldCycle (@old_cycles) {
        system("rsync -e 'ssh -i $KEY' -avzC $UPR_STATS_TMP/upr_tables/$oldCycle $DEST_SERVER:$JOB_LOC/veri_images/upr/upr_tables/.");
        system("rsync -e 'ssh -i $KEY' -avzC $UPR_STATS_TMP/upr_tables/$oldCycle $DEST_SERVER:$JOB_LOC/veri_images/upr/.");
        system("rsync -e 'ssh -i $KEY' -avzC $UPR_STATS_TMP/upr_plots/$oldCycle $DEST_SERVER:$JOB_LOC/veri_images/upr/upr_plots/.");
#        system("rm -rf $UPR_STATS_TMP/upr_plots/$oldCycle");
     }
     foreach $oldCycle (@old_cycles1) {
        system("rsync -e 'ssh -i $KEY' -avzC $UPR_STATS_TMP/upr_tables/$oldCycle $DEST_SERVER:$JOB_LOC/veri_images/upr/upr_tables/.");
        system("rsync -e 'ssh -i $KEY' -avzC $UPR_STATS_TMP/upr_tables/$oldCycle $DEST_SERVER:$JOB_LOC/veri_images/upr/.");
        system("rsync -e 'ssh -i $KEY' -avzC $UPR_STATS_TMP/upr_plots/$oldCycle $DEST_SERVER:$JOB_LOC/veri_images/upr/upr_plots/.");
#        system("rm -rf $UPR_STATS_TMP/upr_plots/$oldCycle");
     }
  }

#  system("rm -rf $UPR_STATS_TMP/*") if ($DEBUG < 10);
#  unlink <*veri_upr_stats*> if ($DEBUG < 10);
 
}   # end of $VERI_UPR
  
exit;

#
#stats_hour/end_hour are relative to cycle
sub lncp_aux_file(){
    my ($cycle, $start_hour, $incre_hour, $end_hour, $domain, $outdir) = $_;
    print ("to cp or link aux files for $cycle $domain-----\n");
    $wd=system("pwd");
    chomp($wd);
    chdir($outdir);
    for($ihr=$start_hour; $ihr<=$end_hour; $ihr+=$incre_hour) {
        if($ihr < 0 ) {
            $dir_run="$RUNDIR/$cycle/WRF_F/";
            $dir_arc="$HOMEDIR/data/cycles/$GMID/archive/$MEMBER/aux3_final/";
        }else {
            $dir_run="$RUNDIR/$cycle/WRF_P/";
            $dir_arc="$HOMEDIR/data/cycles/$GMID/archive/$MEMBER/aux3_$cycle/";
        }
        $datex=&tool_date12_add("${cycle}00", $ihr, "hour");
        $filex1=&tool_date12_to_outfilename("auxhist3_d0${domain}_", $datex, "");
        $pathx1="$dir_run/$filex1";
        $filex2=&tool_date12_to_outfilename("auxhist3_d0${domain}_", $datex, ".nc4.p");
        $pathx2="$dir_arc/$filex2";
        if (-e $filex1) {
            print("$filex1: file exists in WRF_F/WRF_P run dir, to link\n");
            symlink("$pathx1", $filex1);
        }elsif ( -e $filex2) {
            print("$filex2: file exists in ARCHIVE dir, to cp & unpack\n");
            system("cp $pathx2 $filex2");
            $file_name2_unpack=&tool_date12_to_outfilename("auxhist3_d0${domain}_", "$datex", ".nc4");
            system("ncpdq -O -U $filex2 $file_name2_unpack && rm -rf $filex2");
            system("ncks -O -3 $file_name2_unpack $filex1");
        }else {
            print("$filex1 or $filex2 not exist! continue--\n");
            next;
        }
    }
    chdir($wd);
    print ("finish cp or link aux files for $cycle $domain-----\n");
}

sub lncp_wrfout_file(){
    my ($cycle, $start_hour, $incre_hour, $end_hour, $domain, $outdir) = $_;
    print ("to cp or link wrfout files for $cycle $domain-----\n");
    $wd=system("pwd");
    chomp($wd);
    chdir($outdir);
    for($ihr=$start_hour; $ihr<=$end_hour; $ihr+=$incre_hour) {
        $datex=&tool_date12_add("${cycle}00", $ihr, "hour");
        if($ihr < 0 ) {
            $dir_run="$RUNDIR/$cycle/WRF_F/";
            $filex1=&tool_date12_to_outfilename("wrfout_d0${domain}_", $datex, "");
            $pathx1="$dir_run/$filex1";
            $dir_run2="$RUNDIR/$cycle/";
            $filex2=&tool_date12_to_outfilename("wrfout_d0${domain}_", $datex, ".${MEMBER}_F");
            $pathx2="$dir_run2/$filex2";
            $dir_arc="$HOMEDIR/data/cycles/$GMID/archive/$MEMBER/wrfout_upr/final";
            $filexa=&tool_date12_to_outfilename("wrfout_d0${domain}_", $datex, ".${MEMBER}_F");
            $pathxa="$dir_arc/$filexa";
        }else {
            $dir_run="$RUNDIR/$cycle/WRF_P/";
            $filex1=&tool_date12_to_outfilename("wrfout_d0${domain}_", $datex, "");
            $pathx1="$dir_run/$filex1";
            $dir_run2="$RUNDIR/$cycle/";
            $filex2=&tool_date12_to_outfilename("wrfout_d0${domain}_", $datex, ".${MEMBER}_P+FCST");
            $pathx2="$dir_run2/$filex2";
            $dir_arc="$HOMEDIR/data/cycles/$GMID/archive/$MEMBER/wrfout_upr/$cycles";
            $filexa=&tool_date12_to_outfilename("wrfout_d0${domain}_", $datex, ".${MEMBER}_P+FCST");
            $pathxa="$dir_arc/$filexa";
        }
        if (-e $filex1) {
            print("$filex1: file exists in WRF_F/WRF_P run dir, to link\n");
            symlink("$pathx1", $filex1);
        }elsif ( -e $filex2) {
            print("$filex2: file exists in WRF_F/WRF_P top dir, to link\n");
            symlink("$pathx2", $filex1);
        }elsif ( -e $filex3) {
            print("$filex3: file exists in arc_dir, to link\n");
            symlink("$pathx3", $filex1):
        }else {
            print("$filex1 or $filex2 or $filex3 not exist! continue--\n");
            next;
        }
    }
    chdir($wd);
    print ("finish cp or link wrfout files for $cycle $domain-----\n");
}






         


 
#
#
#
sub save_sfc {
  my $cycle;
  my $dateString;
  my $d;
  my $fn;
  my @files;
  my @ds;
  my @dates;
  my $wrfout;
  my $d;
  my $datetime;
  my $wrfout;
  my ($dPush,$datePush);
  my $i;
  my $skip;
  my $len;
  my $wrf;
  my $VERI_DIR_INTERM;
  my $oldCycle;
  my $new_dir;

  unlink 'obs.dat' if(-e 'obs.dat');
  system("rm -rf qc_out_*");
  system("rm -rf qc_out.dat");
  system("rm -rf qc_analysis.out");
  my $date_3 = date_retro($date,6);
  my $date_3 = date_retro($date_3,$CYC_INT);
my $year3 = substr $date_3, 0, 4;
my $mons3 = substr $date_3, 4, 2;
my $days3 = substr $date_3, 6, 2;
my $hour3 = substr $date_3, 8, 2;
my $yeare = substr $date, 0, 4;
my $monse = substr $date, 4, 2;
my $dayse = substr $date, 6, 2;
my $houre = substr $date, 8, 2;
my $date_3_str="${year3}-${mons3}-${days3}_${hour3}";
my $date_e_str="${yeare}-${monse}-${dayse}_${houre}";

  foreach $cycle (sort @old_cycles_q) {
#    system("ln -sf $OBSDIR/$cycle/RAP_RTFDDA/qc_out* .");
# lpan
 @files = `ls -t -1 -r $OBSDIR/$cycle/RAP_RTFDDA/qc_out*`;
	print("OBSDIR=$OBSDIR\n");

     foreach $file0 (sort @files) {

        my $dirname= dirname($file0);
        my $filename= basename($file0);


       $file2 = substr($filename,0,31);
       $file1 = $dirname. "/" . $file2;
      $filesize1 = (-s $file1);
      $filesize2 = (-s $file2);

     print("file1=$file1\n");
     print("file2=$file2\n");
     print("size1=$filesize1\n");
     print("size2=$filesize2\n");

     if(!-e $file2 || $filesize1 >= $filesize2 ){
     unlink ("$file2");
         if($filesize1 > 10000000) {
     system("ln -s $file1 $file2");
             print("used file1 szie:$filesize1\n");

                  }

     }

     }

  }
#  system("ln -sf $OBSDIR/$date/RAP_RTFDDA/qc_out* .");
 @files1 = `ls -t -1 -r $OBSDIR/$date/RAP_RTFDDA/qc_out*`;

     foreach $file0 (sort @files1) {
#	print("$file0\n");
      $file1 = substr($file0,0,99);
#	print("$file1\n");
      $file2 = substr($file1,68,31);
#	print("$file2\n");
      $filesize1 = (-s $file1);
      $filesize2 = (-s $file2);

     print("$file1\n");
     print("$file2\n");
     print("size1=$filesize1\n");
     print("size2=$filesize2\n");

     if(!-e $file2 || $filesize1 >= $filesize2 ){
     unlink ("$file2");
         if($filesize1 > 10000000) {
     system("ln -s $file1 $file2");
             print("used file1 szie:$filesize1\n");

                  }

     }
  system("cat qc_out_* > qc_out.dat");
        system ("cat $file2 >> qc_analysis.out");

     }


  unlink 'surface_obs.cleanup' if(-e 'surface_obs.cleanup');
  system("$EXECUTABLE_ARCHIVE/RT_all.obs_cleanup.sfc.pl -f qc_out.dat ");
#  system("$EXECUTABLE_ARCHIVE/RT_all.obs_cleanup.sfc_cma.pl -f qc_out.dat ");
  if (defined($ADD_STID) && $ADD_STID > 0) {
     system("$EXECUTABLE_ARCHIVE/v_rewrite_obs.exe surface_obs.cleanup ");
  } else {
     system("$EXECUTABLE_ARCHIVE/v_rewrite_obs.exe surface_obs.cleanup");
  }
  system("sort -k1 fort.31 > obs.dat");

  if($VERI_INTERM) {
    $VERI_DIR_INTERM = "$RUNDIR/verify_interm";
    system("mkdir -p $VERI_DIR_INTERM");
    print "cp obs.dat $VERI_DIR_INTERM/obs.dat.${oldestCycle}\n";
    system("cp obs.dat $VERI_DIR_INTERM/obs.dat.${oldestCycle}");
  }

### Forecast!

### Forecast! 
  $cnt_cycle = 0;
  foreach $oldCycle (@old_cycles) {

    $cnt_cycle += 1;
    $hour_ago = $cnt_cycle * $CYC_INT; 
    if($MMOUTDIR eq $RUNDIR) {
#      print "Cycle $VERI_LENGTH hours ago: $oldCycle\n";
      print "Cycle $hour_ago hours ago: $oldCycle \n";
      $new_dir="$RUNDIR/$oldCycle";
    } else {
      $new_dir=$MMOUTDIR;
    }
    @ds=();
    @dates=();
    foreach $wrfout (<$new_dir/WRF_P/aux*00:00 $new_dir/wrfout*P+FCST>) {
       chomp($wrfout);
       $wrfout=~ /d(\d+)_(\d{4}-\d{2}-\d{2}_\d{2})/;
       $d=$1;
       $dateString=$2;
       $dPush=check_element(\@ds,$d);
	if(($dateString ge $date_3_str) and ( $dateString le $date_e_str)){
	print "dateString=$dateString; date_3_str=$date_3_str $date_e_str\n";

         $datePush=check_element(\@dates,$dateString);
         push(@ds,$d) if($dPush == 1);
         push(@dates,$dateString) if($datePush == 1);
	}
    }
    
    $dir_wrfout_aux = "$VERI_WORK_DIR/wrfdata/$oldCycle";
    system("test -d $dir_wrfout_aux || mkdir -p $dir_wrfout_aux");
    $start_hour  = (&tool_date12_diff_minutes("${date_3}00", "${oldCycle}00")) / 60;
    $end_hour = (&tool_date12_diff_minutes("${date}00", "${oldCycle}00")) / 60;
    for $d (@ds) { 
        &lncp_aux_file($oldCycle, $start_hour, 1, $end_hour, $d, $dir_wrfout_aux); 
    }

    $fn = $oldCycle . "_veri_dat_${MEMBER}_P+FCST";
    $fn1 = $oldCycle . "_veri_dat_${MEMBER}_P+FCST_".$date_3;

    print "P+FCST: dates = @dates ; ds = @ds\n";
    if(-e $new_dir) {
      foreach $datetime (@dates) {
        system("rm -f pairs_domain*");
        foreach $d (@ds) {
         #   $wrf="$new_dir/wrfout_d${d}_${datetime}:00:00.${MEMBER}_P+FCST";
         #   $wrf1="$new_dir/WRF_P/auxhist3_d${d}_${datetime}:00:00";
            $wrf1 = "$dir_wrfout_aux/auxhist3_d${d}_${datetime}:00:00";
            $wrf = "$dir_wrfout_aux/wrfout_d${d}_${datetime}:00:00.${MEMBER}_P+FCST";
            if (defined($ADD_STID) && $ADD_STID > 0) {
               system("$EXECUTABLE_ARCHIVE/v_wrf_sfc_interp.exe $wrf -add_stid ") if((-e $wrf) && ($d ne "01") && ( ! -e $wrf1) ); 
	       system("~/bin/v_wrf_sfc_interp_aux3.exe $wrf1 -add_stid ") if( ($d ne "01") && (-e $wrf1 ));
            print "\n process $wrf1 \n" if( -e $wrf1);
            print "\n process $wrf \n" if ((!-e $wrf1) && (-e $wrf));

            } else {
               system("$EXECUTABLE_ARCHIVE/v_wrf_sfc_interp.exe $wrf ") if((-e $wrf) && ($d ne "01") && (! -e $wrf1) ); 
               system("~/bin/v_wrf_sfc_interp_aux3.exe $wrf1  ") if(($d ne "01") && (-e $wrf1 ));
#               system("~/bin/v_wrf_sfc_interp_aux3.exe $wrf1  ") if( ($d ne "01") && (-e $wrf1 ));
            print "\n process $wrf1 \n" if (-e $wrf1);
            print "\n process $wrf \n" if (( -e $wrf) && (! -e $wrf1));

            }
        }
        foreach $d (@ds) {
            $wrf="$new_dir/wrfout_d${d}_${datetime}:00:00.${MEMBER}_P+FCST";
            $wrf1="$new_dir/WRF_P/auxhist3_d${d}_${datetime}:00:00";
            if (defined($ADD_STID) && $ADD_STID > 0) {
               system("$EXECUTABLE_ARCHIVE/v_wrf_sfc_interp.exe $wrf -add_stid") if((-e $wrf) && ($d eq "01") && (! -e $wrf1)); 
	       system("~/bin/v_wrf_sfc_interp_aux3.exe $wrf1 -add_stid") if( ($d eq "01") && (-e $wrf1 ));
            print "\n process $wrf1 \n" if( -e $wrf1);
            print "\n process $wrf \n" if ((-e $wrf) && (! -e $wrf1));
            } else {
               system("$EXECUTABLE_ARCHIVE/v_wrf_sfc_interp.exe $wrf") if((-e $wrf) && ($d eq "01") && (! -e $wrf1) ); 
               system("~/bin/v_wrf_sfc_interp_aux3.exe $wrf1 ") if( ($d eq "01") && (-e $wrf1 ));
            print "\n process $wrf1 \n" if (-e $wrf1);
            print "\n process $wrf \n" if (( -e $wrf) && (! -e $wrf1));
            }
        }
	sleep 10;

        if (defined($ADD_STID) && $ADD_STID > 0) {
           system("$EXECUTABLE_ARCHIVE/v_merge_pairs.pl pairs_domain* -add_stid");
        } else {
           system("$EXECUTABLE_ARCHIVE/v_merge_pairs.pl pairs_domain*");
        }
        system("cat pairs_domain*.out >> $fn1");
      }  # end foreach $date
    }  # if $new_dir exists

##  system("rsync -e 'ssh -i $KEY' -avzC $fn $WebServer:$REMOTE_DIR_SFC/fcst/$fn");
    if ($VERI_ARCHIVE_ROOT) {
       print "cp $fn $VERI_ARCHIVE_ROOT/$ENV{LOGNAME}/$GSJOBID/veri_dat/sfc/fcst/$fn\n";
       system("cp $fn $VERI_ARCHIVE_ROOT/$ENV{LOGNAME}/$GSJOBID/veri_dat/sfc/fcst/$fn");
       print "cp $fn1 $VERI_ARCHIVE_ROOT/$ENV{LOGNAME}/$GSJOBID/veri_dat/sfc/fcst/$fn1\n";
       system("cp $fn1 $VERI_ARCHIVE_ROOT/$ENV{LOGNAME}/$GSJOBID/veri_dat/sfc/fcst/$fn1");
    system("cat $VERI_ARCHIVE_ROOT/$ENV{LOGNAME}/$GSJOBID/veri_dat/sfc//fcst/$fn1 >> $VERI_ARCHIVE_ROOT/$ENV{LOGNAME}/$GSJOBID/veri_dat/sfc//fcst/$fn");
    }
    system("mv $fn1 $SAVE_DIR_SFC/fcst/$fn1");
    system("cat $SAVE_DIR_SFC/fcst/$fn1 >> $SAVE_DIR_SFC/fcst/$fn");
        if (defined($ADD_STID) && $ADD_STID > 0) {
           system("$EXECUTABLE_ARCHIVE/v_merge_pairs1.pl  $SAVE_DIR_SFC/fcst/$fn -add_stid");
        } else {
           system("$EXECUTABLE_ARCHIVE/v_merge_pairs1.pl  $SAVE_DIR_SFC/fcst/$fn");
        }

    system("rm -rf pairs_*");
    unshift(@files,"$SAVE_DIR_SFC/fcst/$fn");

    print "   Finish fcst_sfc of $oldCycle cycle at ", &ctime(time);
  }

### Final!

  @ds=();
  @dates=();
  foreach $wrfout (<$new_dir/WRF_P/aux*00:00 $RUNDIR/$date/wrfout*F>) { #ss, should be WRF_F?
     chomp($wrfout);
     $wrfout=~ /d(\d+)_(\d{4}-\d{2}-\d{2}_\d{2})/;
     $d=$1;
     $dateString=$2;
     $dPush=check_element(\@ds,$d);
     $datePush=check_element(\@dates,$dateString);
     push(@ds,$d) if($dPush == 1);
     push(@dates,$dateString) if($datePush == 1);
  }

  $fn=$date . "_veri_dat_${MEMBER}_F";

  foreach $datetime (@dates) {
    system("rm -f pairs_domain*");
    foreach $d (@ds) {
       if($MMOUTDIR eq $RUNDIR) {
         $wrf="$RUNDIR/$date/wrfout_d${d}_${datetime}:00:00.${MEMBER}_F";
         $wrf1="$RUNDIR/$date/WRF_F/auxhist3_d${d}_${datetime}:00:00";
       } else {
         $wrf="$MMOUTDIR/wrfout_d${d}_${datetime}:00:00.${MEMBER}_F";
         $wrf1="$MMOUTDIR/WRF_F/auxhist3_d${d}_${datetime}:00:00";
       }
       print "\n process $fn \n";
       if (defined($ADD_STID) && $ADD_STID > 0) {
          system("$EXECUTABLE_ARCHIVE/v_wrf_sfc_interp.exe $wrf -add_stid") if( (-e $wrf) && (! -e $wrf1) );
	  system("~/bin/v_wrf_sfc_interp_aux3.exe $wrf1 -add_stid") if(($d eq "01") && (-e $wrf1 ));
       } else {
          system("$EXECUTABLE_ARCHIVE/v_wrf_sfc_interp.exe $wrf") if( (-e $wrf ) && (! -e $wrf1));
          system("~/bin/v_wrf_sfc_interp_aux3.exe $wrf1 ") if((-e $wrf1 ));
       }
    }

    if (defined($ADD_STID) && $ADD_STID > 0) {
       system("$EXECUTABLE_ARCHIVE/v_merge_pairs.pl pairs_domain* -add_stid");
    } else {
       system("$EXECUTABLE_ARCHIVE/v_merge_pairs.pl pairs_domain*");
    }
    system("cat pairs_domain*.out >> $fn");
  }  # end foreach $date

##system("rsync -e 'ssh -i $KEY' -avzC $fn $WebServer:$REMOTE_DIR_SFC/final/$fn");
  if ($VERI_ARCHIVE_ROOT) {
     print "cp $fn $VERI_ARCHIVE_ROOT/$ENV{LOGNAME}/$GSJOBID/veri_dat/sfc/final/.\n";
     system("cp $fn $VERI_ARCHIVE_ROOT/$ENV{LOGNAME}/$GSJOBID/veri_dat/sfc/final/.");
  }
  system("mv $fn $SAVE_DIR_SFC/final/.");

  system("rm -rf pairs_*");
  print "   Finish Final-fdda_sfc of $date cycle  at ", &ctime(time);
  unshift(@files,"$SAVE_DIR_SFC/final/$fn");

  return @files;
}

#
#
#
#
sub stats {

  my $d=$_[0];
  my $file;
  my $count;
  my $bytes;
  my $outf;
  my @f;
  my $file_size;
  my $total_records;
  my $i;
  my ($year,$month,$day,$hour,$minute);
  my ($this_date,$date,$date_min);
  my ($index,$index_max);
  my $nbytes=56;
  my $buf;
  my $missing=-8888;
  my ($tag,$type);
  my ($yy,$mm,$dd,$hh);
  my ($date,$date_min);
  my @output;
  my (@sum_t_b,@sum_t_r,@sum_t_m,@no_t);
  my (@sum_q_b,@sum_q_r,@sum_q_m,@no_q);
  my (@sum_rh_b,@sum_rh_r,@sum_rh_m,@no_rh);
  my (@sum_ws_b,@sum_ws_r,@sum_ws_m,@no_ws);
  my (@sum_wd_b,@sum_wd_r,@sum_wd_m,@no_wd);
  my (@sum_slp_b,@sum_slp_r,@sum_slp_m,@no_slp);
  my (@sum_psfc_b,@sum_psfc_r,@sum_psfc_m,@no_psfc);
  my $count;
  my ($n,$i);
  my ($year,$monthday,$hourmin,$lat,$lon,$domain_id,$platform,
      $psfc_m,$psfc_o,$psfc_qc,
      $slp_m,$slp_o,$slp_qc,
      $ter_m,$ter_o,
      $t2_m,$t2_o,$t2_qc,
      $q_m,$q_o,$q_qc,
      $ws_m,$ws_o,$ws_qc,
      $wd_m,$wd_o,$wd_qc,$st_id);
  my ($rh_m,$rh_o);
  my $ind;
  my ($diff_t,$diff_q,$diff_rh,$diff_wd,$diff_ws,$diff_psfc,$diff_slp);
  my (@t_bias,@t_rmse,@t_mae);
  my (@q_bias,@q_rmse,@q_mae);
  my (@rh_bias,@rh_rmse,@rh_mae);
  my (@ws_bias,@ws_rmse,@ws_mae);
  my (@wd_bias,@wd_rmse,@wd_mae);
  my (@slp_bias,@slp_rmse,@slp_mae);
  my (@psfc_bias,@psfc_rmse,@psfc_mae);
  my ($rlat,$rlon);
  my $latlon;
  my ($x,$y,$skip);
#  my $WS_CUT = 150; # do not include light winds (1.5 m/s) in the wind stats calculation
  my $WS_CUT = 0; # do not include light winds (1.5 m/s) in the wind stats calculation


  $nbytes = 64 if (defined($ADD_STID) && $ADD_STID > 0);

  @output=();
  foreach $file (@sfc_pairs_files) {
# foreach $file ($file_final,$file_fcst) {
    if( -e "$file") {
      $file=~ /(\d+)_veri_dat/;
      $tag=$1;
      $file=~/([A-Z]*)$/;
      $type=$1;
      $outf='stats_' . $tag . '_d' . $d . '_' . $type;
      open(IN,"$file");
      open(OUT,"> $outf");
      push(@output,$outf);
      @f=stat "$file";
      $file_size=$f[7];
      $total_records=$file_size/$nbytes;

      for($i=0; $i <= $VERI_LENGTH+1; $i++) {
         $sum_t_b[$i]=0;
         $sum_t_r[$i]=0;
         $sum_t_m[$i]=0;
         $no_t[$i]=0;

         $sum_rh_b[$i]=0;
         $sum_rh_r[$i]=0;
         $sum_rh_m[$i]=0;
         $no_rh[$i]=0;

         $sum_ws_b[$i]=0;
         $sum_ws_r[$i]=0;
         $sum_ws_m[$i]=0;
         $no_ws[$i]=0;

         $sum_wd_b[$i]=0;
         $sum_wd_r[$i]=0;
         $sum_wd_m[$i]=0;
         $no_wd[$i]=0;

         $sum_slp_b[$i]=0;
         $sum_slp_r[$i]=0;
         $sum_slp_m[$i]=0;
         $no_slp[$i]=0;

         $sum_psfc_b[$i]=0;
         $sum_psfc_r[$i]=0;
         $sum_psfc_m[$i]=0;
         $no_psfc[$i]=0;

         $sum_q_b[$i]=0;
         $sum_q_r[$i]=0;
         $sum_q_m[$i]=0;
         $no_q[$i]=0;

      }

      $count=0;
      for($n=0; $n < $total_records; $n++) {
         seek(IN,0,1);
         $bytes=read(IN,$buf,$nbytes);
         if (defined($ADD_STID) && $ADD_STID > 0) {
         ($year,$monthday,$hourmin,$lat,$lon,$domain_id,$platform,
          $psfc_m,$psfc_o,$psfc_qc,
          $slp_m,$slp_o,$slp_qc,
          $ter_m,$ter_o,
          $t2_m,$t2_o,$t2_qc,
          $q_m,$q_o,$q_qc,
          $ws_m,$ws_o,$ws_qc,
          $wd_m,$wd_o,$wd_qc,$st_id)=unpack("s6a4s20a8",$buf);
         } else {
         ($year,$monthday,$hourmin,$lat,$lon,$domain_id,$platform,
          $psfc_m,$psfc_o,$psfc_qc,
          $slp_m,$slp_o,$slp_qc,
          $ter_m,$ter_o,
          $t2_m,$t2_o,$t2_qc,
          $q_m,$q_o,$q_qc,
          $ws_m,$ws_o,$ws_qc,
          $wd_m,$wd_o,$wd_qc)=unpack("s6a4s20",$buf);
         }

          next if($domain_id != $d);

          if ($VERI_LIST) {
             $skip = 1;
             $st_id =~ s/\s+$//;
             $skip = 0 if(defined($stations{$st_id}));
            #print "skip = $skip ; st_id = $st_id\n";
            #foreach $y (-1..1) {
            #foreach $x (-1..1) {
            #  $rlat = $lat*0.01+$y*0.01;
            #  $rlon = $lon*0.01+$x*0.01;
            #  $rlat = sprintf "%.2f",$rlat;
            #  $rlon = sprintf "%.2f",$rlon;
            #  $latlon = "$rlat$rlon";
            #  $skip = 0 if(defined($stations{$latlon}));
            #}
            #}
             next if($skip);
          }

          $count++;

          $month=int($monthday/100);
          $day=$monthday-$month*100;

          $hour=int($hourmin/100);
          $minute=$hourmin-$hour*100;

	  $this_date=$year*100000000+$month*1000000+$day*10000+$hour*100+$minute;
	  $date=nearest_hour($this_date);

          $date_min=date_retro($date,1) if($count == 1);

          $index=indexcal($date,$date_min);
          if($count == 1) {
            $index_max=$index;
          }
          $index_max=$index if($index > $index_max);

          if(($t2_m > $missing) && ($t2_o > $missing) && ($t2_qc  > $QC_CUT)) {
            $t2_m *= 0.01;
            $t2_o *= 0.01;
            $diff_t=$t2_m-$t2_o;
            $sum_t_b[$index] += $diff_t;
            $sum_t_r[$index] += $diff_t**2;
            $sum_t_m[$index] += abs($diff_t);
            $no_t[$index] += 1;
          }

#yliu -- take very weak winds (<0.1 m/s) out of count of wind directions
#        if(($ws_m > 100.) && ($ws_o > 100.) && ($ws_qc  > $QC_CUT) &&
         if(($ws_m >= $WS_CUT) && ($ws_o >= $WS_CUT) && ($ws_qc  > $QC_CUT) &&
             ($wd_m > $missing) && ($wd_o > $missing) && ($wd_qc  > $QC_CUT)
             && ($wd_o < 361)) {
#yliu    if(($wd_m > $missing) && ($wd_o > $missing) && ($wd_qc  > $QC_CUT)) {
            $diff_wd=$wd_m-$wd_o;

            $diff_wd += 360 if($diff_wd < -180);
            $diff_wd -= 360 if($diff_wd > 180);

            $sum_wd_b[$index] += $diff_wd;
            $sum_wd_r[$index] += $diff_wd**2;
            $sum_wd_m[$index] += abs($diff_wd);
            $no_wd[$index] += 1;
          }
 
#         if(($ws_m > $missing) && ($ws_o > $missing) && ($ws_qc  > $QC_CUT)) {
          if(($ws_m >= $WS_CUT) && ($ws_o >= $WS_CUT) && ($ws_qc  > $QC_CUT)) {
            $ws_m *= 0.01;
            $ws_o *= 0.01;
            $diff_ws=$ws_m-$ws_o;
            $sum_ws_b[$index] += $diff_ws;
            $sum_ws_r[$index] += $diff_ws**2;
            $sum_ws_m[$index] += abs($diff_ws);
            $no_ws[$index] += 1;
          }

          if(($slp_m > $missing) && ($slp_o > $missing) && ($slp_qc  > $QC_CUT)) {
            $slp_m *= 0.1;
            $slp_o *= 0.1;
            $diff_slp=$slp_m-$slp_o;
            $sum_slp_b[$index] += $diff_slp;
            $sum_slp_r[$index] += $diff_slp**2;
            $sum_slp_m[$index] += abs($diff_slp);
            $no_slp[$index] += 1;
          }

          if(($psfc_m > $missing) && ($psfc_o > $missing) && ($psfc_qc  > $QC_CUT)) {
            $psfc_m *= 0.1;
            $psfc_o *= 0.1;
            $diff_psfc=$psfc_m-$psfc_o;

           #if ((int($psfc_o) != 1013) ||  ($diff_psfc <= 4)) {
            if (abs($diff_psfc) <= 4) {
               $sum_psfc_b[$index] += $diff_psfc;
               $sum_psfc_r[$index] += $diff_psfc**2;
               $sum_psfc_m[$index] += abs($diff_psfc);
               $no_psfc[$index] += 1;
            } else {
               print "Bad psfc obs: lat = $lat; lon = $lon; psfc_o = $psfc_o; psfc_m=$psfc_m\n";
               $psfc_qc = 0;
            }
          }

          if(($q_m > $missing) && ($q_o > $missing) && ($q_qc  > $QC_CUT) && (abs($q_m - $q_o) < 300)) {
            $q_m *= 0.01;
            $q_o *= 0.01;
            $diff_q=$q_m-$q_o;
            $sum_q_b[$index] += $diff_q;
            $sum_q_r[$index] += $diff_q**2;
            $sum_q_m[$index] += abs($diff_q);
            $no_q[$index] += 1;
          }

          if(($psfc_m > $missing) && ($psfc_o > $missing) &&
             ($t2_m > $missing)   && ($t2_o > $missing) &&
             ($q_m > $missing)    && ($q_o > $missing) && 
             ($t2_qc  > $QC_CUT) && ($psfc_qc  > $QC_CUT) && ($q_qc  > $QC_CUT)) {

            $rh_m=rh_from_q($psfc_m,$t2_m,$q_m);
            $rh_o=rh_from_q($psfc_o,$t2_o,$q_o);

            $diff_rh=$rh_m-$rh_o;
            $sum_rh_b[$index] += $diff_rh;
            $sum_rh_r[$index] += $diff_rh**2;
            $sum_rh_m[$index] += abs($diff_rh);
            $no_rh[$index] += 1;
          }
          
      }
 
      close(IN);

      for($i=1; $i <= $index_max; $i++) {
         if($no_t[$i] > 0) {
           $t_bias[$i]=$sum_t_b[$i]/$no_t[$i];
           $t_rmse[$i]=sqrt($sum_t_r[$i]/$no_t[$i]);
           $t_mae[$i]=$sum_t_m[$i]/$no_t[$i];
         } else {
           $t_bias[$i]=-99;
           $t_rmse[$i]=-99;
           $t_mae[$i]=-99;
         }

         if($no_rh[$i] > 0) {
           $rh_bias[$i]=$sum_rh_b[$i]/$no_rh[$i];
           $rh_rmse[$i]=sqrt($sum_rh_r[$i]/$no_rh[$i]);
           $rh_mae[$i]=$sum_rh_m[$i]/$no_rh[$i];
         } else {
           $rh_bias[$i]=-99;
           $rh_rmse[$i]=-99;
           $rh_mae[$i]=-99;
         }

         if($no_ws[$i] > 0) {
           $ws_bias[$i]=$sum_ws_b[$i]/$no_ws[$i];
           $ws_rmse[$i]=sqrt($sum_ws_r[$i]/$no_ws[$i]);
           $ws_mae[$i]=$sum_ws_m[$i]/$no_ws[$i];
         } else {
           $ws_bias[$i]=-99;
           $ws_rmse[$i]=-99;
           $ws_mae[$i]=-99;
         }

         if($no_wd[$i] > 0) {
           $wd_bias[$i]=$sum_wd_b[$i]/$no_wd[$i];
           $wd_rmse[$i]=sqrt($sum_wd_r[$i]/$no_wd[$i]);
           $wd_mae[$i]=$sum_wd_m[$i]/$no_wd[$i];
         } else {
           $wd_bias[$i]=-99;
           $wd_rmse[$i]=-99;
           $wd_mae[$i]=-99;
         }

         if($no_slp[$i] > 0) {
           $slp_bias[$i]=$sum_slp_b[$i]/$no_slp[$i];
           $slp_rmse[$i]=sqrt($sum_slp_r[$i]/$no_slp[$i]);
           $slp_mae[$i]=$sum_slp_m[$i]/$no_slp[$i];
         } else {
           $slp_bias[$i]=-99;
           $slp_rmse[$i]=-99;
           $slp_mae[$i]=-99;
         }

         if($no_psfc[$i] > 0) {
           $psfc_bias[$i]=$sum_psfc_b[$i]/$no_psfc[$i];
           $psfc_rmse[$i]=sqrt($sum_psfc_r[$i]/$no_psfc[$i]);
           $psfc_mae[$i]=$sum_psfc_m[$i]/$no_psfc[$i];
         } else {
           $psfc_bias[$i]=-99;
           $psfc_rmse[$i]=-99;
           $psfc_mae[$i]=-99;
         }

         if($no_q[$i] > 0) {
           $q_bias[$i]=$sum_q_b[$i]/$no_q[$i];
           $q_rmse[$i]=sqrt($sum_q_r[$i]/$no_q[$i]);
           $q_mae[$i]=$sum_q_m[$i]/$no_q[$i];
         } else {
           $q_bias[$i]=-99;
           $q_rmse[$i]=-99;
           $q_mae[$i]=-99;
         }

      }

      printf OUT "%10d\n",$date_min;
      for($i=1; $i <= $index_max; $i++) {
         printf OUT "%2d%7.1f%7.1f%7.1f%7d%7.1f%7.1f%7.1f%7d%7.1f%7.1f%7.1f%7d%7.1f%7.1f%7.1f%7d%7.1f%7.1f%7.1f%7d%7.1f%7.1f%7.1f%7d%7.1f%7.1f%7.1f%7d\n",
         $i,$t_bias[$i],$t_rmse[$i],$t_mae[$i],$no_t[$i],
            $rh_bias[$i],$rh_rmse[$i],$rh_mae[$i],$no_rh[$i],
            $ws_bias[$i],$ws_rmse[$i],$ws_mae[$i],$no_ws[$i],
            $wd_bias[$i],$wd_rmse[$i],$wd_mae[$i],$no_wd[$i],
            $slp_bias[$i],$slp_rmse[$i],$slp_mae[$i],$no_slp[$i],
            $psfc_bias[$i],$psfc_rmse[$i],$psfc_mae[$i],$no_psfc[$i],
            $q_bias[$i],$q_rmse[$i],$q_mae[$i],$no_q[$i];
      }

     close(OUT);
    }

  }

  return @output;

}

#
#
#
#
#

sub rh_from_q {

  my ($p,$t,$q)=@_;
  my ($es,$qs,$rh);

  $q *= 0.001;

  $es=10**(-2937.4/($t+273.15)-4.9283*log($t+273.15)/log(10)+23.5518);
  $qs=0.622*$es/($p-$es);
  $rh=$q/$qs*100;
  $rh=0 if($rh < 0); 
  $rh=100 if($rh > 100);

  return $rh;
}

#
#
#
#
#

sub indexcal {

  my ($date,$date_min)=@_;
  my ($yy_now,$mm_now,$dd_now,$hh_now,$yy_min,$mm_min,$dd_min,$hh_min);
  my ($secs_total,$secs_min);
  my $index;

  $yy_now=int($date/1000000);
  $mm_now=int(($date%1000000)/10000);
  $dd_now=int(($date%10000)/100);
  $hh_now=$date%100;

  $yy_min=int($date_min/1000000);
  $mm_min=int(($date_min%1000000)/10000);
  $dd_min=int(($date_min%10000)/100);
  $hh_min=$date_min%100;

  $secs_total=date2secs($yy_now,$mm_now,$dd_now,$hh_now,0,0,0);
  $secs_min=date2secs($yy_min,$mm_min,$dd_min,$hh_min,0,0,0);

  $index=int($secs_total-$secs_min)/3600;

  return $index;

}

#
#
#
#
#

sub stats_plot {

  my ($var,@files)=@_;
  my ($domain,$title);
  my $fadd;
  my ($b_RANGE,$rm_RANGE);
  my $y_ref;
  my $file;
  my ($color_index,$color);
  my ($time_tag,$time_stamp,$time_stamp_f,$time_min,$time_max);
  my @f;
  my ($n,$ind);
  my $outf;
  my $count;
  my @counts;
  my $n_elements;
  my $ymax;
  my $tenth;
  my $half;
  my $oldCycle;
  my $start_time;
  my $beg;
  my ($yaxis,$ytick);
  my $b_RANGE;
  my $y_no;
  my $record;
  my $max_count;
  my $hour;
  my ($xpos,$xposb,$xposm,$yposm,$xpose,$ypos);
  my $out_gif;

  my @counts = (0) x ($VERI_LENGTH+1);

  if($var eq 't') {
    $fadd=0;
    $b_RANGE=$plot_range{t}{bias};
    $rm_RANGE=$plot_range{t}{rmse};
  } elsif($var eq 'rh') {
    $fadd=4;
    $b_RANGE=$plot_range{rh}{bias};
    $rm_RANGE=$plot_range{rh}{rmse};
  } elsif($var eq 'ws') {
    $fadd=8;
    $b_RANGE=$plot_range{ws}{bias};
    $rm_RANGE=$plot_range{ws}{rmse};
  } elsif($var eq 'wd') {
    $fadd=12;
    $b_RANGE=$plot_range{wd}{bias};
    $rm_RANGE=$plot_range{wd}{rmse};
  } elsif($var eq 'slp') {
    $fadd=16;
    $b_RANGE=$plot_range{slp}{bias};
    $rm_RANGE=$plot_range{slp}{rmse};
  } elsif($var eq 'psfc') {
    $fadd=20;
    $b_RANGE=$plot_range{psfc}{bias};
    $rm_RANGE=$plot_range{psfc}{rmse};
  } elsif($var eq 'q') {
    $fadd=24;
    $b_RANGE=$plot_range{q}{bias};
    $rm_RANGE=$plot_range{q}{rmse};
  } else {
    die "Wrong variable, pick from 't', 'rh', 'q','ws', 'wd', 'slp', and 'psfc'";
  }
 
  $rm_RANGE =~ /(\d+)\/(\d+)/;
  $y_ref = ($1 - $2)*0.25;

#
# BIAS loop
#
  $time_min=date_retro($date,$VERI_LENGTH);

  $n_elements=scalar @files;
  $n=0;
  foreach $file (@files) {

    $file=~ /_d(\d+)/;
    $domain=$1;

    if($file=~ /_F$/) {
      $file=~ /(\d+)/;
      $time_tag=$1;
      $time_stamp_f=$time_tag;
      $color_index = indexcal($date,$time_tag)/$CYC_INT;
      $color=$colors[$color_index];
    } elsif($file=~ /_P$/) {
      $file=~ /(\d+)/;
      $time_tag=$1;
     #$time_stamp_p=$time_tag;
      $color='/0/0/255';
    } else {                             # forecast
      $file=~ /(\d+)/;
      $time_tag=$1;
      $color_index = indexcal($date,$time_tag)/$CYC_INT;
      $color=$colors[$color_index];
    }

    print "color = $color\n" if($DEBUG > 0);

    $start_time=`head -1 $file`;
    $beg=indexcal($start_time,$time_min);
    print "In BIAS loop, beg = $beg\n" if($DEBUG > 0);

    open(STATS,"$file");
    
    #$outf="${date}/${time_stamp_f}_${var}_d${domain}.ps";
    $outf="${date}/${date}_${var}_d${domain}.ps";
    $title="Domain $domain";

    $n++;

    if($n == 1) {

      if($var eq 't') {
        $yaxis='"T  BIAS (K)"';
        $ytick='f0.5a1';
      } elsif($var eq 'rh') {
        $yaxis='"RH  BIAS (%)"';
        $ytick='f2a10';
      } elsif($var eq 'ws') {
        $yaxis='"SPD  BIAS (m s@+-1@+)"';
        $ytick='f0.5a1';
      } elsif($var eq 'wd') {
        $yaxis='"DIR  BIAS (\272)"';
        $ytick='f50a10';
      } elsif($var eq 'slp') {
        $yaxis='"SLP  BIAS (hPa)"';
        $ytick='f1a2';
      } elsif($var eq 'psfc') {
        $yaxis='"PSFC  BIAS (hPa)"';
        $ytick='f1a2';
      } else {
        $yaxis='"Q  BIAS (g kg@+-3@+)"';
        $ytick='f0.1a0.5';
      }

      open(PIPE,"| psxy -JX7.32/2 -R0/${VERI_LENGTH}.4/$b_RANGE -Bf1a6:.\"$title\":/$ytick:$yaxis: -M -W1.5p$color -X1 -Y8.5 -K > $outf");
    } else {
      open(PIPE,"| psxy -JX -R -M -W1.5p$color -O -K >> $outf");
    }

#   open(PIPE1,"| pstext -JX -R -N -O -K >> $outf") if($n == $n_elements);
 
#   $b_RANGE=~ /(\-*\d+)\/(\d+)/;
    $b_RANGE=~ /([0-9\-\.]+)\/([0-9\.]+)/;
    $y_no=($2-$1)*0.05+$1;

    $count=0;
    while ($record=<STATS>) {
      next if($. == 1);
      @f=split " ",$record;

      $ind=$f[0]+$beg;

      if($f[1+$fadd] != -99) {
        print PIPE "$ind $f[1+$fadd]\n";
#       print PIPE1 "$ind $y_no 12 0 0 MC $f[4+$fadd]\n" if($n == $n_elements);
        $count++;
      } else {
        print PIPE '>'," $ind $f[1+$fadd]\n" if($count > 0);
      }

    }

    close(STATS);
    close(PIPE);
    open(PIPE,"| psxy -JX -R -Wta -O -K >> $outf");
    print PIPE "0 0\n ${VERI_LENGTH}.4 0";
    close(PIPE);
#   close(PIPE1) if($n == $n_elements);

  }

#
# RMSE loop
#
  $n=0;
  foreach $file (@files) {

    if($file=~ /_F$/) {
      $file=~ /(\d+)/;
      $time_tag=$1;
      $time_stamp_f=$time_tag;
      $color_index = indexcal($date,$time_tag)/$CYC_INT;
      $color=$colors[$color_index];
    } elsif($file=~ /_P$/) {
      $file=~ /(\d+)/;
      $time_tag=$1;
     #$time_stamp_p=$time_tag;
      $color='/0/0/255';
    } else {                             # forecast
      $file=~ /(\d+)/;
      $time_tag=$1;
      $color_index = indexcal($date,$time_tag)/$CYC_INT;
      $color=$colors[$color_index];
    }

    $start_time=`head -1 $file`;
    $beg=indexcal($start_time,$time_min);
    print "In RMSE loop, beg = $beg\n" if($DEBUG > 0);

    open(STATS,"$file");

    $n++;

    if($n == 1) {

      if($var eq 't') {
        $yaxis='"T  RMSE (K)"';
        $ytick='f0.5a1';
      } elsif($var eq 'rh') {
        $yaxis='"RH  RMSE (%)"';
        $ytick='f2a10';
      } elsif($var eq 'ws') {
        $yaxis='"SPD  RMSE (m s@+-1@+)"';
        $ytick='f0.5a1';
      } elsif($var eq 'wd') {
        $yaxis='"DIR  RMSE (\272)"';
        $ytick='f50a10';
      } elsif($var eq 'slp') {
        $yaxis='"SLP  RMSE (hPa)"';
        $ytick='f1a2';
      } elsif($var eq 'psfc') {
        $yaxis='"PSFC  RMSE (hPa)"';
        $ytick='f1a2';
      } else {
        $yaxis='"Q  RMSE (g kg@+-3@+)"';
        $ytick='f0.1a0.5';
      }

      open(PIPE,"| psxy -JX7.32/2 -R0/${VERI_LENGTH}.4/$rm_RANGE -Bf1a6/$ytick:$yaxis: -M -W1.5p$color -Y-2.5 -K -O >> $outf");
    } else {
      open(PIPE,"| psxy -JX -R -M -W1.5p$color -O -K >> $outf");
    }

    $count=0;

    while ($record=<STATS>) {
      next if($. == 1);
      @f=split " ",$record;

      $ind=$f[0]+$beg;

      if($f[2+$fadd] != -99) {
        print PIPE "$ind $f[2+$fadd]\n";
        $count++;
      } else {
        print PIPE '>'," $ind $f[2+$fadd]\n" if($count > 0);
      }

    }

    close(STATS);
    close(PIPE);

  }
#
# MAE loop
#

  $n=0;
  foreach $file (@files) {

    if($file=~ /_F$/) {
      $file=~ /(\d+)/;
      $time_tag=$1;
      $time_stamp_f=$time_tag;
      $color_index = indexcal($date,$time_tag)/$CYC_INT;
      $color=$colors[$color_index];
    } elsif($file=~ /_P$/) {
      $file=~ /(\d+)/;
      $time_tag=$1;
     #$time_stamp_p=$time_tag;
      $color='/0/0/255';
    } else {                             # forecast
      $file=~ /(\d+)/;
      $time_tag=$1;
      $color_index = indexcal($date,$time_tag)/$CYC_INT;
      $color=$colors[$color_index];
    }

    $start_time=`head -1 $file`;
    $beg=indexcal($start_time,$time_min);

    open(STATS,"$file");

    $n++;

    if($n == 1) {

      if($var eq 't') {
        $yaxis='"T  MAE (K)"';
        $ytick='f0.5a1';
      } elsif($var eq 'rh') {
        $yaxis='"RH  MAE (%)"';
        $ytick='f2a10';
      } elsif($var eq 'ws') {
        $yaxis='"SPD  MAE (m s@+-1@+)"';
        $ytick='f0.5a1';
      } elsif($var eq 'wd') {
        $yaxis='"DIR  MAE (\272)"';
        $ytick='f50a10';
      } elsif($var eq 'slp') {
        $yaxis='"SLP  MAE (hPa)"';
        $ytick='f1a2';
      } elsif($var eq 'psfc') {
        $yaxis='"PSFC  MAE (hPa)"';
        $ytick='f1a2';
      } else {
        $yaxis='"Q  MAE (g kg@+-3@+)"';
        $ytick='f0.1a0.5';
      }

      open(PIPE,"| psxy -JX7.32/2 -R0/${VERI_LENGTH}.4/$rm_RANGE -Bf1a6/$ytick:$yaxis: -M -W1.5p$color -Y-2.5 -K -O >> $outf");
    } else {
      open(PIPE,"| psxy -JX -R -M -W1.5p$color -O -K >> $outf");
    }

    $count=0;

    while ($record=<STATS>) {
      next if($. == 1);
      @f=split " ",$record;

      $ind=$f[0]+$beg;

      if($f[3+$fadd] != -99) {
        print PIPE "$ind $f[3+$fadd]\n";
        $count++;
      } else {
        print PIPE '>'," $ind $f[3+$fadd]\n" if($count > 0);
      }

      $counts[$ind] = $f[4+$fadd];
    }

    close(STATS);
    close(PIPE);

  }

# Label the reference date/time tag

  open(PIPE,"| pstext -JX7.32/2 -R0/${VERI_LENGTH}.4/$rm_RANGE -N -O -K >> $outf");
  print PIPE "0 $y_ref 12 0 0 MC $time_min";
  close(PIPE);

# plot counts

  $max_count=0;
  foreach $hour (0..$VERI_LENGTH-1) {
     $max_count = $counts[$hour] if ($counts[$hour] > $max_count);
  }

  if ($max_count <= 20) {
     $ymax  = 20;
     $ytick = 'f1a10';
  } elsif ($max_count <= 50) {
     $ymax  = 50;
     $ytick = 'f5a25';
  } elsif ($max_count <= 100) {
     $ymax  = 100;
     $ytick = 'f10a50';
  } elsif ($max_count <= 500) {
     $ymax  = 500;
     $ytick = 'f100a250';
  } elsif ($max_count <= 1000) {
     $ymax  = 1000;
     $ytick = 'f100a500';
  } else {
     $ymax  = (int($max_count/1000)+1)*1000;
     $tenth = $ymax/10;
     $half  = $ymax/2;
     $ytick = "f${tenth}a${half}";
  }

  open(PIPE,"| psxy -JX7.32/0.5 -R0/${VERI_LENGTH}.4/0/$ymax -Bf1a6:Hour:/${ytick}:Counts:WS -Sb0.5u -Ggray -Y-1.25 -O -K >> $outf");

  print "Plot counts for variable $var domain $domain\n";
  foreach $hour (0..$VERI_LENGTH) {
     print PIPE "$hour $counts[$hour]\n";
  }
  close(PIPE);

# Annotate

  open(PIPE,"| psxy -JX7/1.25 -R0/${VERI_LENGTH}.4/0/10 -Y-2 -W1.5p$colors[0] -O -K >> $outf");

  $xposb = 0;
  $xpose = $VERI_LENGTH * 0.1;
  $xposm = $xpose * 0.5;
  print PIPE "$xposb 9 \n $xposm 9.5 \n $xpose 9";
  close PIPE;

  $n = 1;
  foreach $oldCycle (@old_cycles) {
     $xposb = ($n%3)*$VERI_LENGTH * 0.33;
     $xpose = $xposb + $VERI_LENGTH * 0.1;
     $xposm = ($xposb + $xpose) * 0.5;
     $ypos = 9 - int($n/3)*2;
     $yposm = $ypos + 0.5;
     $color = $colors[$n];
     open(PIPE,"| psxy -JX -R -W1.5p$color -O -K >> $outf");
     print PIPE "$xposb $ypos \n $xposm $yposm \n $xpose $ypos";
     close(PIPE);
     $n++;
  }

  $xpos = $VERI_LENGTH * 0.12;
  $ypos = 9;
  open(PIPE,"| pstext -JX -R -N -O -K >> $outf");
  print PIPE "$xpos $ypos 12 0 0 ML $date FINAL\n";
  close(PIPE);

  $n = 1;
  foreach $oldCycle (@old_cycles) {
     $xpos = ($n%3)*$VERI_LENGTH * 0.33 + $VERI_LENGTH * 0.12;
     $ypos = 9 - int($n/3)*2;

     if ($n == ($#old_cycles + 1)) {
       open(PIPE,"| pstext -JX -R -N -O >> $outf");
     } else {
       open(PIPE,"| pstext -JX -R -N -O -K >> $outf");
     }

     print PIPE "$xpos $ypos 12 0 0 ML $oldCycle FCST\n";
     close(PIPE);
     $n++;
  }

  $out_gif=$outf;
  $out_gif=~ s/ps$/gif/;

#	system("pwd");
#	system ("ls *.ps");
#	system ("ls $outf\n");
  system("convert -trim +repage -density 112 $outf $out_gif");
#	system ("ls $out_gif");
#	system("cp $out_gif .");

  unlink $outf;  ## needs to be uncommented out later!

}

#
#
#

sub html_create {

my $domain;
my $fn;

foreach $domain (1..$NDOM) {

  $fn="d$domain.html";
  open(OUT,">$fn");

  print OUT "
<HTML>
<HEAD>
<TITLE>RT FDDA Surface Verification Plots Domain $domain</TITLE></HEAD>
<BODY>
";

  print OUT "
<TABLE>
<TR>
<TD>
<H2> 10 m Wind Speed </H2>
<P><IMG SRC=\"${date}_ws_d$domain.gif\" width=\"547\" height=\"547\">
</TD>
<TD>
<H2> 10 m Wind Direction </H2>
<P><IMG SRC=\"${date}_wd_d$domain.gif\" width=\"547\" height=\"547\">
</TD>
</TR>
<TR>
<TD>
<H2> 2 m Temperature </H2>
<P><IMG SRC=\"${date}_t_d$domain.gif\" width=\"547\" height=\"547\">
</TD>
<TD>
<H2> Mixing Ratio </H2>
<P><IMG SRC=\"${date}_q_d$domain.gif\" width=\"547\" height=\"547\">
</TD>
</TR>
<TR>
<TD>
<H2> Relative Humidity </H2>
<P><IMG SRC=\"${date}_rh_d$domain.gif\" width=\"547\" height=\"547\">
</TD>
<TD>
<H2> Surface Pressure <H2>
<P><IMG SRC=\"${date}_psfc_d$domain.gif\" width=\"547\" height=\"547\">
</TD>
</TR>
</TABLE>
</BODY>
</HTML>
";
  close(OUT);
  if ($DEST_SERVER=~ /localhost/i) {
     system("cp $fn $JOB_LOC/veri_images/sfc/$date/.");
  } else {
     system("rsync -e 'ssh -i $KEY' -avzC $fn $DEST_SERVER:$JOB_LOC/veri_images/sfc/$date/.");
  }
     system("cp $fn $date/.");
#  unlink "$fn";
}

}

#
sub html_create_upr {

my $domain;
my $fn;

foreach $domain (1..$NDOM) {

  $fn="d$domain.html";
  open(OUT,">$fn");

  print OUT "
<HTML>
<HEAD>
<TITLE>RT FDDA Upper air Verification Plots Domain $domain</TITLE></HEAD>
<BODY>
";

  print OUT "
<TABLE>
<TR>
<TD>
<H2> 10 m Wind Speed </H2>
<P><IMG SRC=\"ws_${date}_${date}_d$domain.gif\">
</TD>
<TD>
<H2> 10 m Wind Direction </H2>
<P><IMG SRC=\"wd_${date}_${date}_d$domain.gif\">
</TD>
</TR>
<TR>
<TD>
<H2> 2 m Temperature </H2>
<P><IMG SRC=\"t_${date}_${date}_d$domain.gif\">
</TD>
<TD>
<H2> Relative Humidity </H2>
<P><IMG SRC=\"rh_${date}_${date}_d$domain.gif\">
</TD>
</TABLE>
</BODY>
</HTML>
";
  close(OUT);
  if ($DEST_SERVER=~ /localhost/i) {
     system("cp $fn $JOB_LOC/veri_images/upr/upr_plots/$date/.");
  } else {
     system("rsync -e 'ssh -i $KEY' -avzC $fn $DEST_SERVER:$JOB_LOC/veri_images/upr/upr_plots/$date/.");
  }
  unlink "$fn";
}

}
#
#
#
#

sub old_enough {

  my ($date,$tag)=@_;
  my ($yy,$mm,$dd,$hh);
  my ($sec0,$sec1,$sec_diff,$flag);

  $yy=substr($date,0,4);
  $mm=substr($date,4,2);
  $dd=substr($date,6,2);
  $hh=substr($date,8,2);

  $sec0=date2secs($yy,$mm,$dd,$hh,0,0,0);

  $yy=substr($tag,0,4);
  $mm=substr($tag,4,2);
  $dd=substr($tag,6,2);
  $hh=substr($tag,8,2);

  $sec1=date2secs($yy,$mm,$dd,$hh,0,0,0);

  $sec_diff=$sec0-$sec1;

  if($sec_diff >= 86400*7) {
    $flag=1;
  } else {
    $flag=0;
  }

  return $flag;

}

#
#
#
sub save_upr {
 
  my $oldCycle;
  my $new_dir;
  my (@ds,@dates);
  my ($wrfout,$wrf);
  my $d;
  my $dateString;
  my $datetime;
  my ($dPush,$datePush);
  my $fn;
  my @files;
  my $count;
  my $f;
  my @split_files;
  my $hr;
  my $v_upr;
  my $uhr;
  my $VERI_DIR_INTERM;
 
  unlink 'soundings_obs.cleanup' if(-e 'soundings_obs.cleanup');
  system("$EXECUTABLE_ARCHIVE/RT_all.obs_cleanup.2.pl -S -f qc_out.dat ");
  if($VERI_HGT > 0) {
    system("$EXECUTABLE_ARCHIVE/v_rewrite_snd1.exe soundings_obs.cleanup");
  } else {
    system("$EXECUTABLE_ARCHIVE/v_rewrite_snd.exe soundings_obs.cleanup");
  }
# The above system call generates a new soundings ASCII file, fort.61.

  if($VERI_INTERM) {
    $VERI_DIR_INTERM = "$RUNDIR/verify_interm";
    print "cp fort.61 $VERI_DIR_INTERM/fort.61.${oldestCycle}\n";
    system("cp fort.61 $VERI_DIR_INTERM/fort.61.${oldestCycle}");
  }
 
  @files=();
### Final!
  my $date_3 = date_retro($date,3);
  my $date_3 = date_retro($date_3,$CYC_INT);
  my $year3 = substr $date_3, 0, 4;
  my $mons3 = substr $date_3, 4, 2;
  my $days3 = substr $date_3, 6, 2;
  my $hour3 = substr $date_3, 8, 2;
  my $date_3_str="${year3}-${mons3}-${days3}_${hour3}";

  @ds=();
  @dates=();
  foreach $wrfout (<$RUNDIR/$date/wrfout*F>) {
     chomp($wrfout);
     $wrfout=~ /d(\d+)_(\d{4}-\d{2}-\d{2}_\d{2})/;
     $d=$1;
     $dateString=$2;

     $hr=substr($dateString,-2);

     $v_upr = 0;
     if (@UPR_HOURS) {
        foreach $uhr (@UPR_HOURS) {
          $v_upr = 1 if ($hr == $uhr && $dateString ge $date_3_str);
        }
     } else {
        $v_upr = 1;
     }

     if ($v_upr) {
        print "$wrfout will be verified against upper-air obs!\n";
     } else {
       next;
     }

     $dPush=check_element(\@ds,$d);
     $datePush=check_element(\@dates,$dateString);
     push(@ds,$d) if($dPush == 1);
     push(@dates,$dateString) if($datePush == 1);
  }

#  $skip = shift @dates;
#  print "this_cycle = $this_cycle; final; skip = $skip\n";

  foreach $datetime (@dates) {
    foreach $d (@ds) {
      if($MMOUTDIR eq $RUNDIR) {
        $wrf="$RUNDIR/$date/wrfout_d${d}_${datetime}:00:00.${MEMBER}_F";
      } else {
        $wrf="$MMOUTDIR/$MMOUTDIR/wrfout_d${d}_${datetime}:00:00.${MEMBER}_F";
      }
      if($VERI_HGT > 0) {
        system("$EXECUTABLE_ARCHIVE/v_wrf_snd_pairs.exe $wrf -height -add_hr");
      } else {
        system("$EXECUTABLE_ARCHIVE/v_wrf_snd_pairs.exe $wrf");
      }
    }
 
    system("cat snd_pairs_domain* > snd_pairs_all_domains");
    if($VERI_HGT > 0) {
      system("$EXECUTABLE_ARCHIVE/v_wrf_read_snd_pairs.exe snd_pairs_all_domains -height");
    } else {
      system("$EXECUTABLE_ARCHIVE/v_wrf_read_snd_pairs.exe snd_pairs_all_domains");
    }
#   The above system call generates a merged soundings ASCII file, fort.81.
#   system("rsync -e 'ssh -i $KEY' -avzC fort.81 $WebServer:$REMOTE_DIR_UPR/final/$fn");
if ( -e "$VERI_DIR/fort.81" ) {
open(FILE, "<$VERI_DIR/fort.81") || die "File not found";
my @lines = <FILE>;
close(FILE);
#
my @newlines;

foreach(@lines) {
   $_ =~ s/88888888/88  8888/g;
   $_ =~ s/8888888/8  8888/g;
   $_ =~ s/888888/  8888/g;
   push(@newlines,$_);
}

open(FILE, ">$VERI_DIR/fort.81") || die "File not found";
print FILE @newlines;
close(FILE);
}



     @split_files=split_upr('fort.81',$date,'F');
     unlink 'fort.81';
     foreach $f (@split_files) {
##     system("rsync -e 'ssh -i $KEY' -avzC $f $WebServer:$REMOTE_DIR_UPR/final/.");
       if ($VERI_ARCHIVE_ROOT) {
          print "cp $f $VERI_ARCHIVE_ROOT/$ENV{LOGNAME}/$GSJOBID/veri_dat/upr/final/.\n";
          system("cp $f $VERI_ARCHIVE_ROOT/$ENV{LOGNAME}/$GSJOBID/veri_dat/upr/final/.");
       }
       system("mv $f $SAVE_DIR_UPR/final/.");
     }
     system("rm -rf snd_pairs_domain*");

    foreach $f (@split_files) {
      if(-s "$SAVE_DIR_UPR/final/$f") {
        push(@files,"$SAVE_DIR_UPR/final/$f");
      }
    }
  } # end foreach $datetime
 
    print "   Finish Final-fdda_upr of $date cycle at ",&ctime(time);
 
### Forecast!
 
  foreach $oldCycle (@old_cycles) {
    if($MMOUTDIR eq $RUNDIR) {
     $new_dir="$RUNDIR/$oldCycle";
    } else {
     $new_dir=$MMOUTDIR;
    }

    @ds=();
    @dates=();
    foreach $wrfout (<$new_dir/wrfout*P+FCST>) {
       chomp($wrfout);
       $wrfout=~ /d(\d+)_(\d{4}-\d{2}-\d{2}_\d{2})/;
       $d=$1;
       $dateString=$2;

       $hr=substr($dateString,-2);

       $v_upr = 0;
       if (@UPR_HOURS) {
          foreach $uhr (@UPR_HOURS) {
            $v_upr = 1 if ($hr == $uhr);
          }
       } else {
         $v_upr = 1;
       }

       if ($v_upr) {
          print "$wrfout will be verified against upper-air obs!\n";
       } else {
          next;
       }

       $dPush=check_element(\@ds,$d);
       $datePush=check_element(\@dates,$dateString);
       push(@ds,$d) if($dPush == 1);
       push(@dates,$dateString) if($datePush == 1);
    }

#  $skip = shift @dates;
#  print "this_cycle = $this_cycle; fcst; skip = $skip\n";

    if(-e $new_dir) {
      foreach $datetime (@dates) {
        foreach $d (@ds) {
             $wrf="$new_dir/wrfout_d${d}_${datetime}:00:00.${MEMBER}_P+FCST";
             if($VERI_HGT > 0) {
               system("$EXECUTABLE_ARCHIVE/v_wrf_snd_pairs.exe $wrf -height -add_hr");
             } else {
               system("$EXECUTABLE_ARCHIVE/v_wrf_snd_pairs.exe $wrf");
             }
        }
 
        system("cat snd_pairs_domain* > snd_pairs_all_domains");
        if($VERI_HGT > 0) {
          system("$EXECUTABLE_ARCHIVE/v_wrf_read_snd_pairs.exe snd_pairs_all_domains -height");
        } else {
          system("$EXECUTABLE_ARCHIVE/v_wrf_read_snd_pairs.exe snd_pairs_all_domains");
        }
if ( -e "$VERI_DIR/fort.81" ) {
open(FILE, "<$VERI_DIR/fort.81") || die "File not found";
my @lines = <FILE>;
close(FILE);
#
my @newlines;

foreach(@lines) {
   $_ =~ s/88888888/88  8888/g;
   $_ =~ s/8888888/8  8888/g;
   $_ =~ s/888888/  8888/g;
   push(@newlines,$_);
}

open(FILE, ">$VERI_DIR/fort.81") || die "File not found";
print FILE @newlines;
close(FILE);
}
        undef @split_files;
        @split_files=split_upr('fort.81',$oldCycle,'FCST');
        unlink 'fort.81';
        foreach $f (@split_files) {
##        system("rsync -e 'ssh -i $KEY' -avzC $f $WebServer:$REMOTE_DIR_UPR/fcst/.");
          if ($VERI_ARCHIVE_ROOT) {
             print "cp $f $VERI_ARCHIVE_ROOT/$ENV{LOGNAME}/$GSJOBID/veri_dat/upr/fcst/.\n";
             system("cp $f $VERI_ARCHIVE_ROOT/$ENV{LOGNAME}/$GSJOBID/veri_dat/upr/fcst/.");
          }
          system("mv $f $SAVE_DIR_UPR/fcst/.");
        }
        system("rm -rf snd_pairs_domain*");
 
        foreach $f (@split_files) {
          if(-s "$SAVE_DIR_UPR/fcst/$f") {
            push(@files,"$SAVE_DIR_UPR/fcst/$f");
          }
        }
      } # end foreach $datetime
    } # end if -e $new_dir
 
  print "   Finish forecast-fdda_upr of $oldCycle  at ",&ctime(time);

  }
 
  return @files;
 
}

sub stats_upr {
 
#
# This subroutine deals with nested data structure as outlined below:
#
# %snds = ( 'snd' => [
#                      { 'date' => ,
#                        'stid' => ,
#                        'lat'  => ,
#                        'lon'  => ,
#                        'elev' => ,
#                        'id'   => ,
#                        'level'=> [
#                                    { 'p'     => ,
#                                      'tm'    => ,
#                                      'to'    => ,
#                                      'qc_t'  => ,
#                                       .
#                                       .
#                                       .
#
#                                    },
#
#                                    { 'p'     => ,
#                                      'tm'    => ,
#                                      'to'    => ,
#                                      'qc_t'  => ,
#                                       .
#                                       .
#                                       .
#
#                                    },
#                                    .
#                                    .
#                                    .
#                                  ]
#                      }
#                    ],
#
#                    [
#                      { 'date' =>,
#                          .
#                          .
#                          .
#                      },
#
#                      .
#                      .
#                      .
#                    ],
#                    .
#                    .
#                    .
#         )
#
#
#
 
  my ($file,$d)=@_;
  my $outf;
  my %snds;
  my @fields;
  my ($date,$st_id,$lat,$lon,$elevm,$elev,$id);
  my $line;
  my ($nsnd,$l_index);   ## indices for soundings, levels
  my $missing=-8888;
  my ($p,$tm,$to,$qc_t,$qm,$qo,$qc_q,$rhm,$rho,$qc_rh,$wsm,$wso,$qc_ws,
      $wdm,$wdo,$qc_wd,$ghm,$gho,$qc_gh);
  my ($i,$l);
  my (@sum_t,@sum_ta,@sum_tr,@no_t,@bias_t,@rmse_t,@mae_t);
  my (@sum_q,@sum_qa,@sum_qr,@no_q,@bias_q,@rmse_q,@mae_q);
  my (@sum_rh,@sum_rha,@sum_rhr,@no_rh,@bias_rh,@rmse_rh,@mae_rh);
  my (@sum_ws,@sum_wsa,@sum_wsr,@no_ws,@bias_ws,@rmse_ws,@mae_ws);
  my (@sum_wd,@sum_wda,@sum_wdr,@no_wd,@bias_wd,@rmse_wd,@mae_wd);
  my (@sum_gh,@sum_gha,@sum_ghr,@no_gh,@bias_gh,@rmse_gh,@mae_gh);
  my $diff;
  my $latlon;
  my ($x,$y,$skip);
 
# foreach $file ($file_final,$file_fcst) {
    if( -e "$file") {
      $file=~ /(\d+\w+)$/;
      $outf=$1;
      $outf=~ s/upr/upr_stats_d$d/;
      $outf=~ s/_dat//;
      open(IN,"$file");
      open(OUT,"> $outf");
 
      %snds=();
 
      while ($line=<IN>) {
        chomp $line;
        $nsnd=int(($.-1)/41);
        $l_index=($.-1)%41;
        if($l_index == 0) {
           @fields = split " ",$line;
           if($#fields == 6) {
             ($date,$st_id,$lat,$lon,$elevm,$elev,$id)=split " ",$line;
           } elsif ($#fields == 5) {
             ($date,$st_id,$lat,$lon,$elev,$id)=split " ",$line;
           } else {
             ($date,$lat,$lon,$id)=split " ",$line;
           }

           $snds{snd}[$nsnd]{date}=$date;
           $snds{snd}[$nsnd]{stid}=$st_id;
           $snds{snd}[$nsnd]{lat}=$lat;
           $snds{snd}[$nsnd]{lon}=$lon;
           $snds{snd}[$nsnd]{elev}=$elev;
           $snds{snd}[$nsnd]{id}=$id;
        } else {
          if ($VERI_HGT) {
          ($p,$tm,$to,$qc_t,$qm,$qo,$qc_q,$rhm,$rho,$qc_rh,$wsm,$wso,$qc_ws,
           $wdm,$wdo,$qc_wd,$ghm,$gho,$qc_gh)=split " ",$line;
           $snds{snd}[$nsnd]{level}[$l_index]{p}=$p;
           $snds{snd}[$nsnd]{level}[$l_index]{tm}=$tm;
           $snds{snd}[$nsnd]{level}[$l_index]{to}=$to;
           $snds{snd}[$nsnd]{level}[$l_index]{qc_t}=$qc_t;
           $snds{snd}[$nsnd]{level}[$l_index]{qm}=$qm;
           $snds{snd}[$nsnd]{level}[$l_index]{qo}=$qo;
           $snds{snd}[$nsnd]{level}[$l_index]{qc_q}=$qc_q;
           $snds{snd}[$nsnd]{level}[$l_index]{rhm}=$rhm;
           $snds{snd}[$nsnd]{level}[$l_index]{rho}=$rho;
           $snds{snd}[$nsnd]{level}[$l_index]{qc_rh}=$qc_rh;
           $snds{snd}[$nsnd]{level}[$l_index]{wsm}=$wsm;
           $snds{snd}[$nsnd]{level}[$l_index]{wso}=$wso;
           $snds{snd}[$nsnd]{level}[$l_index]{qc_ws}=$qc_ws;
           $snds{snd}[$nsnd]{level}[$l_index]{wdm}=$wdm;
           $snds{snd}[$nsnd]{level}[$l_index]{wdo}=$wdo;
           $snds{snd}[$nsnd]{level}[$l_index]{qc_wd}=$qc_wd;
           $snds{snd}[$nsnd]{level}[$l_index]{ghm}=$ghm;
           $snds{snd}[$nsnd]{level}[$l_index]{gho}=$gho;
           $snds{snd}[$nsnd]{level}[$l_index]{qc_gh}=$qc_gh;
          } else {
          ($p,$tm,$to,$qc_t,$qm,$qo,$qc_q,$rhm,$rho,$qc_rh,$wsm,$wso,$qc_ws,
           $wdm,$wdo,$qc_wd)=split " ",$line;
           $snds{snd}[$nsnd]{level}[$l_index]{p}=$p;
           $snds{snd}[$nsnd]{level}[$l_index]{tm}=$tm;
           $snds{snd}[$nsnd]{level}[$l_index]{to}=$to;
           $snds{snd}[$nsnd]{level}[$l_index]{qc_t}=$qc_t;
           $snds{snd}[$nsnd]{level}[$l_index]{qm}=$qm;
           $snds{snd}[$nsnd]{level}[$l_index]{qo}=$qo;
           $snds{snd}[$nsnd]{level}[$l_index]{qc_q}=$qc_q;
           $snds{snd}[$nsnd]{level}[$l_index]{rhm}=$rhm;
           $snds{snd}[$nsnd]{level}[$l_index]{rho}=$rho;
           $snds{snd}[$nsnd]{level}[$l_index]{qc_rh}=$qc_rh;
           $snds{snd}[$nsnd]{level}[$l_index]{wsm}=$wsm;
           $snds{snd}[$nsnd]{level}[$l_index]{wso}=$wso;
           $snds{snd}[$nsnd]{level}[$l_index]{qc_ws}=$qc_ws;
           $snds{snd}[$nsnd]{level}[$l_index]{wdm}=$wdm;
           $snds{snd}[$nsnd]{level}[$l_index]{wdo}=$wdo;
           $snds{snd}[$nsnd]{level}[$l_index]{qc_wd}=$qc_wd;
          }
        }
      }
 
      close(IN);
 
      for $l (1..$l_index) {

          $sum_t[$l]=0;
          $sum_ta[$l]=0;
          $sum_tr[$l]=0;
          $no_t[$l]=0;
 
          $sum_q[$l]=0;
          $sum_qa[$l]=0;
          $sum_qr[$l]=0;
          $no_q[$l]=0;
 
          $sum_rh[$l]=0;
          $sum_rha[$l]=0;
          $sum_rhr[$l]=0;
          $no_rh[$l]=0;
 
          $sum_ws[$l]=0;
          $sum_wsa[$l]=0;
          $sum_wsr[$l]=0;
          $no_ws[$l]=0;
 
          $sum_wd[$l]=0;
          $sum_wda[$l]=0;
          $sum_wdr[$l]=0;
          $no_wd[$l]=0;
 
          $sum_gh[$l]=0;
          $sum_gha[$l]=0;
          $sum_ghr[$l]=0;
          $no_gh[$l]=0;
 
      }
 
      for $i (0..$nsnd) {
 
        next if($snds{snd}[$i]{id} != $d);

        if ($VERI_LIST) {
           $skip = 1;
           $st_id = $snds{snd}[$i]{stid};
           $st_id =~ s/\s+$//;
           $skip = 0 if(defined($stations{$st_id}));
          #foreach $y (-1..1) {
          #foreach $x (-1..1) {
          #  $lat = $snds{snd}[$i]{lat}+$y*0.01;
          #  $lon = $snds{snd}[$i]{lon}+$x*0.01;
          #  $latlon = "$lat$lon";
          #  $skip = 0 if(defined($stations{$latlon}));
          #}
          #}
           next if($skip);
        }

        for $l (1..$l_index) {
          if(($snds{snd}[$i]{level}[$l]{tm} > $missing) &&
             ($snds{snd}[$i]{level}[$l]{to} > $missing) &&
             ($snds{snd}[$i]{level}[$l]{qc_t}  > $QCU_CUT)) {
            $diff=$snds{snd}[$i]{level}[$l]{tm}-$snds{snd}[$i]{level}[$l]{to};
            $sum_t[$l] += $diff;
            $sum_ta[$l] += abs($diff);
            $sum_tr[$l] += $diff**2;
            $no_t[$l] += 1;
          }
          if(($snds{snd}[$i]{level}[$l]{qm} > $missing) &&
             ($snds{snd}[$i]{level}[$l]{qo} > $missing) &&
             ($snds{snd}[$i]{level}[$l]{qc_q}  > $QCU_CUT)) {
            $diff=$snds{snd}[$i]{level}[$l]{qm}-$snds{snd}[$i]{level}[$l]{qo};
            $sum_q[$l] += $diff;
            $sum_qa[$l] += abs($diff);
            $sum_qr[$l] += $diff**2;
            $no_q[$l] += 1;
          }
          if(($snds{snd}[$i]{level}[$l]{rhm} > $missing) &&
             ($snds{snd}[$i]{level}[$l]{rho} > $missing) &&
             ($snds{snd}[$i]{level}[$l]{qc_rh}  > $QCU_CUT)) {
            $diff=$snds{snd}[$i]{level}[$l]{rhm}-$snds{snd}[$i]{level}[$l]{rho};
            $sum_rh[$l] += $diff;
            $sum_rha[$l] += abs($diff);
            $sum_rhr[$l] += $diff**2;
            $no_rh[$l] += 1;
          }
          if(($snds{snd}[$i]{level}[$l]{wsm} > $missing) &&
             ($snds{snd}[$i]{level}[$l]{wso} > $missing) &&
             ($snds{snd}[$i]{level}[$l]{qc_ws}  > $QCU_CUT)) {
            $diff=$snds{snd}[$i]{level}[$l]{wsm}-$snds{snd}[$i]{level}[$l]{wso};
            $sum_ws[$l] += $diff;
            $sum_wsa[$l] += abs($diff);
            $sum_wsr[$l] += $diff**2;
            $no_ws[$l] += 1;
          }
          if(($snds{snd}[$i]{level}[$l]{wdm} > $missing) &&
             ($snds{snd}[$i]{level}[$l]{wdo} > $missing) &&
             ($snds{snd}[$i]{level}[$l]{qc_wd}  > $QCU_CUT)) {
            $diff=$snds{snd}[$i]{level}[$l]{wdm}-$snds{snd}[$i]{level}[$l]{wdo};
            $diff -= 360 if($diff > 180);
            $diff += 360 if($diff < -180);
            $sum_wd[$l] += $diff;
            $sum_wda[$l] += abs($diff);
            $sum_wdr[$l] += $diff**2;
            $no_wd[$l] += 1;
          }
          if ($VERI_HGT) {
            if(($snds{snd}[$i]{level}[$l]{ghm} > $missing) &&
               ($snds{snd}[$i]{level}[$l]{gho} > $missing) &&
               ($snds{snd}[$i]{level}[$l]{qc_gh}  > $QCU_CUT)) {
              $diff=$snds{snd}[$i]{level}[$l]{ghm}-$snds{snd}[$i]{level}[$l]{gho};
              $sum_gh[$l] += $diff;
              $sum_gha[$l] += abs($diff);
              $sum_ghr[$l] += $diff**2;
              $no_gh[$l] += 1;
            }
          }
        }
      }
 
#    stats for each level
 
     for $l (1..$l_index) {
 
         if($no_t[$l] > 0) {
           $bias_t[$l]=$sum_t[$l]/$no_t[$l];
           $mae_t[$l]=$sum_ta[$l]/$no_t[$l];
           $rmse_t[$l]=sqrt($sum_tr[$l]/$no_t[$l]);
         } else {
           $bias_t[$l]=-99;
           $rmse_t[$l]=-99;
           $mae_t[$l]=-99;
         }
 
         if($no_q[$l] > 0) {
           $bias_q[$l]=$sum_q[$l]/$no_q[$l];
           $mae_q[$l]=$sum_qa[$l]/$no_q[$l];
           $rmse_q[$l]=sqrt($sum_qr[$l]/$no_q[$l]);
         } else {
           $bias_q[$l]=-99;
           $rmse_q[$l]=-99;
           $mae_q[$l]=-99;
         }
 
         if($no_rh[$l] > 0) {
           $bias_rh[$l]=$sum_rh[$l]/$no_rh[$l];
           $mae_rh[$l]=$sum_rha[$l]/$no_rh[$l];
           $rmse_rh[$l]=sqrt($sum_rhr[$l]/$no_rh[$l]);
         } else {
           $bias_rh[$l]=-99;
           $rmse_rh[$l]=-99;
           $mae_rh[$l]=-99;
         }
 
         if($no_ws[$l] > 0) {
           $bias_ws[$l]=$sum_ws[$l]/$no_ws[$l];
           $mae_ws[$l]=$sum_wsa[$l]/$no_ws[$l];
           $rmse_ws[$l]=sqrt($sum_wsr[$l]/$no_ws[$l]);
         } else {
           $bias_ws[$l]=-99;
           $rmse_ws[$l]=-99;
           $mae_ws[$l]=-99;
         }
 
         if($no_wd[$l] > 0) {
           $bias_wd[$l]=$sum_wd[$l]/$no_wd[$l];
           $mae_wd[$l]=$sum_wda[$l]/$no_wd[$l];
           $rmse_wd[$l]=sqrt($sum_wdr[$l]/$no_wd[$l]);
         } else {
           $bias_wd[$l]=-99;
           $rmse_wd[$l]=-99;
           $mae_wd[$l]=-99;
         }
 
         if ($VERI_HGT) {
           if($no_gh[$l] > 0) {
             $bias_gh[$l]=$sum_gh[$l]/$no_gh[$l];
             $mae_gh[$l]=$sum_gha[$l]/$no_gh[$l];
             $rmse_gh[$l]=sqrt($sum_ghr[$l]/$no_gh[$l]);
           } else {
             $bias_gh[$l]=-99;
             $rmse_gh[$l]=-99;
             $mae_gh[$l]=-99;
           }
         }
 
         if(($snds{snd}[0]{level}[$l]{p} == 1000) ||
            ($snds{snd}[0]{level}[$l]{p} == 925) ||
            ($snds{snd}[0]{level}[$l]{p} == 850) ||
            ($snds{snd}[0]{level}[$l]{p} == 700) ||
            ($snds{snd}[0]{level}[$l]{p} == 500) ||
            ($snds{snd}[0]{level}[$l]{p} == 400) ||
            ($snds{snd}[0]{level}[$l]{p} == 300) ||
            ($snds{snd}[0]{level}[$l]{p} == 250) ||
            ($snds{snd}[0]{level}[$l]{p} == 200) ||
            ($snds{snd}[0]{level}[$l]{p} == 150) ||
            ($snds{snd}[0]{level}[$l]{p} == 100)) {
 
           if ($VERI_HGT) {
             printf OUT "%4d%6.1f%6.1f%6.1f%6d%6.1f%6.1f%6.1f%6d%6.1f%6.1f%6.1f%6d%6.1f%6.1f%6.1f%6d%7.1f%6.1f%6.1f%6d%6.1f%6.1f%6.1f%6d\n",
                    $snds{snd}[0]{level}[$l]{p},
                    $bias_t[$l],$rmse_t[$l],$mae_t[$l],$no_t[$l],
                    $bias_q[$l],$rmse_q[$l],$mae_q[$l],$no_q[$l],
                    $bias_rh[$l],$rmse_rh[$l],$mae_rh[$l],$no_rh[$l],
                    $bias_ws[$l],$rmse_ws[$l],$mae_ws[$l],$no_ws[$l],
                    $bias_wd[$l],$rmse_wd[$l],$mae_wd[$l],$no_wd[$l],
                    $bias_gh[$l],$rmse_gh[$l],$mae_gh[$l],$no_gh[$l];
           } else {
             printf OUT "%4d%6.1f%6.1f%6.1f%6d%6.1f%6.1f%6.1f%6d%6.1f%6.1f%6.1f%6d%6.1f%6.1f%6.1f%6d%7.1f%6.1f%6.1f%6d\n",
                    $snds{snd}[0]{level}[$l]{p},
                    $bias_t[$l],$rmse_t[$l],$mae_t[$l],$no_t[$l],
                    $bias_q[$l],$rmse_q[$l],$mae_q[$l],$no_q[$l],
                    $bias_rh[$l],$rmse_rh[$l],$mae_rh[$l],$no_rh[$l],
                    $bias_ws[$l],$rmse_ws[$l],$mae_ws[$l],$no_ws[$l],
                    $bias_wd[$l],$rmse_wd[$l],$mae_wd[$l],$no_wd[$l];
           }
         }
     }
 
     close(OUT);
 
    }
# }
 
  return $outf;
}
#
#
#
sub html_table_upr {
 
  my $file=$_[0];
  my $cycle;
  my $snd_time;
  my ($x,$y,$i,$n);
  my (@row,@table);
  my ($time_tag,$cycle,$domain,$type,$type_ext,$color);
  my $htmlf;
 
# foreach $file (@files) {
 
  $file=~ /(\d+)_(\d+)_veri_upr_stats_d(\d+)_${MEMBER}_(\w+)/;
  $cycle=$1;
  $snd_time=$2;
  $domain=$3;
  $type=$4;
  $htmlf="${cycle}_${snd_time}_d${domain}.html";

  open(OUT,">$htmlf");
  print OUT "<HTML>\n";
  print OUT "<BODY>\n";
 
 
  if($type eq 'F') {
    $type_ext='FINAL';
    $color='Green';
  } elsif($type eq 'P') {
    $type_ext='PRELIMINARY';
    $color='Blue';
  } else {
    $type_ext='FORECAST';
    $color='Red';
  }
 
  open(IN,"$file");

  while (<IN>) {
    $y=$.;
    chomp;
    @row=split;
    $x=scalar @row;
    foreach $i (0..$x-1) {
      $table[$y-1][$i]=$row[$i];
    }
  }
 
  close(IN);
 
# print OUT "Content-type: text/html\n\n";
  print OUT "<H4>Date/Time: $snd_time Cycle: $cycle <FONT COLOR=\'$color\'>$type_ext</FONT> Domain $domain</H4>\n";
  print OUT "<TABLE BORDER WIDTH=70% CELLSPACING=0 CELLPADDING=5 COLS=25>\n";
  print OUT "<TR>\n";
  print OUT "<TH ROWSPAN=2>Pressure (hPa)</TH>\n";
  print OUT "<TH COLSPAN=4>Temperature (K)</TH>\n";
  print OUT "<TH COLSPAN=4>Mixing Ratio (g/kg)</TH>\n";
  print OUT "<TH COLSPAN=4>Relative Humidity (%)</TH>\n";
  print OUT "<TH COLSPAN=4>Wind Speed (m/s)</TH>\n";
  print OUT "<TH COLSPAN=4>Wind Direction (deg)</TH>\n";
  print OUT "<TH COLSPAN=4>Height (m)</TH>\n" if($VERI_HGT);
  print OUT "</TR>\n";
 
  print OUT "<TR>\n";
  foreach $i (1..5) {
    print OUT "<TH>Bias</TH>\n";
    print OUT "<TH>RMSE</TH>\n";
    print OUT "<TH>MAE</TH>\n";
    print OUT "<TH>Count</TH>\n";
  }
  if ($VERI_HGT) {
    print OUT "<TH>Bias</TH>\n";
    print OUT "<TH>RMSE</TH>\n";
    print OUT "<TH>MAE</TH>\n";
    print OUT "<TH>Count</TH>\n";
  }

  print OUT "</TR>\n";
 
  foreach $i (0..$y-1) {
    print OUT "<TR ALIGN=CENTER>\n";
    foreach $n (0..$x-1) {
      print OUT "<TD>$table[$i][$n]</TD>\n";
    }
    print OUT "</TR>\n";
  }
 
  print OUT "</TABLE>\n";
 
# }

  print OUT "</BODY></HTML>\n";
 
  close(OUT);
 
  return $htmlf;
}
#
#
#
sub split_upr {

  my ($file_orig,$cycle_tag,$stage)=@_;
  my @files;
  my $date;
  my @f;
  my $fn;

  open(IN,"$file_orig");

  while (<IN>) {

    if($.%41 == 1) {
      ($date,@f)=split;
      $fn="${cycle_tag}_${date}_veri_dat_upr_${MEMBER}_${stage}";
      if(! -e $fn) {
        open(OUT,">$fn");
        push(@files,$fn);
      } else {
        open(OUT,">>$fn");
      }
      print OUT $_;
    } elsif($.%41 == 0) {  
      print OUT $_;
      close(OUT);
    } else {
      print OUT $_;
    }

  }

  close(IN);
  return @files;
}
#
#
#
sub check_element {
  my ($r_array,$var)=@_;
  my $element;
  my $push;

  $push=1;
  foreach $element (@$r_array) {
    if($var eq $element) {
      $push=0;
      last;
    }
  }

  return $push;
}
#
#
#
sub parse_stations {

  my ($lstation,$llevel,$lfcst);
  my ($sid,$lat,$lon);
  my $key;

  open(STATION,"$VERI_LIST");

  while (<STATION>) {

    chomp;

    next if(length == 0);

    if (/STATIONS/) {
       $lstation = 1;
       $llevel = 0;
       $lfcst = 0;
       next;
    } elsif (/LEVELS/) {
       $lstation = 0;
       $llevel = 1;
       $lfcst = 0;
       next;
    } elsif (/FCST/) {
       $lstation = 0;
       $llevel = 0;
       $lfcst = 1;
       next;
    } elsif (/UPR/) {
       $lstation = 0;
       $llevel = 0;
       $lfcst = 0;
       next;
    } elsif (/BIN/) {
       $lstation = 0;
       $llevel = 0;
       $lfcst = 0;
       next;
    }

    if($lstation) {
      ($sid,$lat,$lon) = split;
      $lat = sprintf "%.2f",$lat;
      $lon = sprintf "%.2f",$lon;
     #$key = "$lat$lon";
      $key = $sid;
      $stations{$key} = 1;
    }
  } 

  return;
}
#
#
#
sub stats_plot_upr {

  my $file = $_[0];
  my $cycle;
  my $snd_time;
  my $type;
  my $base;
  my @fields;
  my $var;
  my $ps;
  my $gif;
  my @gifs;

  my @levels;
  our (@t_bs,@t_rs,@t_ms,@t_ns);
  our (@rh_bs,@rh_rs,@rh_ms,@rh_ns);
  our (@ws_bs,@ws_rs,@ws_ms,@ws_ns);
  our (@wd_bs,@wd_rs,@wd_ms,@wd_ns);
  our (@gh_bs,@gh_rs,@gh_ms,@gh_ns);

  my %plot_title = ( 't' => { 'bias' => 'T bias (K)',
                              'rmse' => 'T RMSE (K)',
                              'mae' => 'T MAE (K)' },
                     'rh' => { 'bias' => 'RH bias (%)',
                              'rmse' => 'RH RMSE (%)',
                              'mae' => 'RH MAE (%)' },
                     'ws' => { 'bias' => 'Wind Speed bias (m s@+-1@+)',
                               'rmse' => 'Wind Speed RMSE (m s@+-1@+)',
                               'mae' => 'Wind Speed MAE (m s@+-1@+)' },
                     'wd' => { 'bias' => 'Wind Direction bias (deg)',
                               'rmse' => 'Wind Direction RMSE (deg)',
                               'mae' => 'Wind Direction MAE (deg)' },
                     'gh' => { 'bias' => 'Height bias (m)',
                               'rmse' => 'Height RMSE (m)',
                               'mae' => 'Height MAE (m)' } );

  my %xtick = ( 't' => { 'bias' => 'f1a5',
                         'rmse' => 'f1a5',
                         'mae' => 'f1a5' },
                'rh' => { 'bias' => 'f1a5',
                          'rmse' => 'f1a5',
                          'mae' => 'f1a5' },
                'ws' => { 'bias' => 'f1a5',
                          'rmse' => 'f1a5',
                          'mae' => 'f1a5' },
                'wd' => { 'bias' => 'f5a20',
                          'rmse' => 'f10a50',
                          'mae' => 'f10a50' },
                'gh' => { 'bias' => 'f10a10',
                          'rmse' => 'f10a10',
                          'mae' => 'f10a10' } ); 

  $file=~ /(\d+)_(\d+)_veri_upr_stats_d(\d+)_${MEMBER}_(\w+)/;

  $cycle=$1;
  $snd_time=$2;
  $domain=$3;
  $type=$4;
  $base="${cycle}_${snd_time}_d${domain}";

  @levels = ();
  @t_bs = ();
  @t_rs = ();
  @t_ms = ();
  @t_ns = ();
  @rh_bs = ();
  @rh_rs = ();
  @rh_ms = ();
  @rh_ns = ();
  @ws_bs = ();
  @ws_rs = ();
  @ws_ms = ();
  @ws_ns = ();
  @wd_bs = ();
  @wd_rs = ();
  @wd_ms = ();
  @wd_ns = ();
  @gh_bs = ();
  @gh_rs = ();
  @gh_ms = ();
  @gh_ns = ();

  open(STATS,"$file");
  while (<STATS>) {
     @fields = split;

     push(@levels,$fields[0]);
     push(@t_bs,$fields[1]);
     push(@t_rs,$fields[2]);
     push(@t_ms,$fields[3]);
     push(@t_ns,$fields[4]);
     push(@rh_bs,$fields[9]);
     push(@rh_rs,$fields[10]);
     push(@rh_ms,$fields[11]);
     push(@rh_ns,$fields[12]);
     push(@ws_bs,$fields[13]);
     push(@ws_rs,$fields[14]);
     push(@ws_ms,$fields[15]);
     push(@ws_ns,$fields[16]);
     push(@wd_bs,$fields[17]);
     push(@wd_rs,$fields[18]);
     push(@wd_ms,$fields[19]);
     push(@wd_ns,$fields[20]);
     push(@gh_bs,$fields[21]);
     push(@gh_rs,$fields[22]);
     push(@gh_ms,$fields[23]);
     push(@gh_ns,$fields[24]);
  }
  close(STATS);

  @gifs = ();

  foreach $var ('t','rh','ws','wd','gh') { 

     $ps = "${var}_$base.ps";

  #  bias:

     open(GMT,"| psxy -JX2/-3p0.1 -R$plot_range{$var}{bias}/100/1000 -B$xtick{$var}{bias}:\"$plot_title{$var}{bias}\":/a100g100:\"Pressure (hPa)\":WSen -W1.5p/0/255/0 -X1 -Y5 -K > $ps");
     for $i (0..$#levels) {
         print GMT "${\"${var}_bs\"}[$i] $levels[$i]\n" if ($levels[$i] < 1050 && ${"${var}_ns"}[$i] > 0);
     }
     close(GMT);

     open(GMT,"| psxy -JX -R -Wta -O -K >> $ps");
     print GMT "0 1000\n0 100";
     close(GMT);

  #  rmse:

     open(GMT,"| psxy -JX2/-3p0.1 -R$plot_range{$var}{rmse}/100/1000 -B$xtick{$var}{rmse}:\"$plot_title{$var}{rmse}\":/a100g100:\"Pressure (hPa)\":wSen -W1.5p/0/255/0 -X2.35 -O -K >> $ps");
     for $i (0..$#levels) {
         print GMT "${\"${var}_rs\"}[$i] $levels[$i]\n" if ($levels[$i] < 1050 && ${"${var}_ns"}[$i] > 0);
     }
     close(GMT);

  #  mae:

     open(GMT,"| psxy -JX2/-3p0.1 -R$plot_range{$var}{rmse}/100/1000 -B$xtick{$var}{mae}:\"$plot_title{$var}{mae}\":/a100g100:\"Pressure (hPa)\":wSen -W1.5p/0/255/0 -X2.35 -O -K >> $ps");
     for $i (0..$#levels) {
         print GMT "${\"${var}_ms\"}[$i] $levels[$i]\n" if ($levels[$i] < 1050 && ${"${var}_ns"}[$i] > 0);
     }
     close(GMT);

  # annotate:

     open(GMT,"| pstext -JX6.7/1 -R0/100/-10/10 -X-4.7 -Y-1.5 -O >> $ps");
     print GMT "0 0 12 0 5 ML Cycle: $cycle ; Valid Time: $snd_time ; Domain $domain";
     close(GMT);

     $gif = $ps;
     $gif =~ s/ps$/gif/;

     system("convert -trim +repage $ps $gif");

     push(@gifs,"$gif");

     unlink "$ps" if (defined($DEBUG) && $DEBUG < 10);

  } # end for each $var

  return @gifs;

}
