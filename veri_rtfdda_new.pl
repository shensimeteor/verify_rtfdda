#!/usr/bin/perl

$HOMEDIR=$ENV{HOME};
$ENSPROCS="$ENV{CSH_ARCHIVE}/ncl";
require "$ENSPROCS/common_tools.pl";

%hash_opt = &tool_get_cmdopt("no_value");
$VALID = $hash_opt{v};
$GMID = $hash_opt{id};
$MEMBER = $hash_opt{m};

if( ! ($VALID && $GMID && $MEMBER)) {
    print "<usage> $0 -id <GMID> -m <MEMBER> -v <VALID_TIME>  \n";
    exit(-1);
}

$GSJOBDIR = "$HOMEDIR/data/GMODJOBS/$GMID/";
$CYCLEDIR = "$HOMEDIR/data/cycles/$GMID/";
require "$GSJOBDIR/flexinput.pl";
require "$GSJOBDIR/verifyinput.pl"; #need add: OBS_MEMBER, VERI_INTERNAL

$OBSDIR = "$CYCEDIR/$OBS_MEMBER/";
($OBS_GFS, $OBS_NML) = split(/_/, $OBS_MEMBER);
if ( $MEMBER eq $OBS_MEMBER ){
    system("$GSJOBDIR/run_qcout.replace_qc_value.csh $HOMEDIR $GMID $VALID $OBS_GFS");
}




