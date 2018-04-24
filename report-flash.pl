#!/usr/bin/perl

BEGIN {
        die "ORACLE_HOME not set\n" unless $ENV{ORACLE_HOME};
        unless ($ENV{OrAcLePeRl}) {
                $ENV{OrAcLePeRl} = "$ENV{ORACLE_HOME}/perl";
                $ENV{PERL5LIB} = "$ENV{PERL5LIB}:$ENV{OrAcLePeRl}/lib:$ENV{OrAcLePeRl}/lib/site_perl";
                $ENV{LD_LIBRARY_PATH} = "$ENV{LD_LIBRARY_PATH}:$ENV{ORACLE_HOME}/lib32:$ENV{ORACLE_HOME}/lib";
                ##exec "$ENV{OrAcLePeRl}/bin/perl", $0, @ARGV;
        }
}

system(clear);

use Time::Local;
use Getopt::Long;
use Pod::Usage;
use Term::ANSIColor qw( colored );

sub set_column_widths {
    my $h         = shift;
    my $databases = shift;
    my $names     = shift;
    my $operator  = shift;

    for my $key (keys %$h){
        for my $db (@$names){
            my $l = length $h->{$key}{$db}{$operator};
            $databases->{$db} = $databases->{$db} > $l ? $databases->{$db} : $l;
        }
    }
    for my $db (@$names){
        my $l = length $db;
        $databases->{$db} = $databases->{$db} > $l ? $databases->{$db} : $l;
    }
}

sub sort_array {
  my( $array, $column, $order ) = @_;
  # my @filtered = grep { not /UNKNOWN/ } @$array ;
  if( $order eq 'desc' ){
    return sort { return( (split ' ', $b)[$column] <=> (split ' ', $a)[$column] ); } @$array;
  } else {
    return sort { return( (split ' ', $a)[$column] <=> (split ' ', $b)[$column] ); } @$array;
  }
}

sub round {
  $_[0] > 0 ? int($_[0] + .5) : -int(-$_[0] + .5)
}

sub max {
    my ($max, @vars) = @_;
    for (@vars) {
        $max = $_ if $_ > $max;
    }
    return $max;
}

sub min {
    my ($min, @vars) = @_;
    for (@vars) {
        $min = $_ if $_ < $min;
    }
    return $min;
}

## Command line argument handling ##
my ($help,$man);
my %optctl=();
my ($lower_day, $upper_day) = (1, 15);
my ($lower_hour, $upper_hour) = (1, 360);

Getopt::Long::GetOptions(
        \%optctl      ,
        'cell=s'      ,
        'cell_group=s',
        'topn=i'      ,
        'detail'      ,
        'ago_unit=s'  ,
        'ago_value=i' ,
        'h|help|?' => \$help, man => \$man
) or pod2usage(2) ;

pod2usage(1) if $help;
pod2usage(-verbose => 2) if $man;

die pod2usage(2)  if  ( defined $optctl{cell} && defined $optctl{cell_group} );
die pod2usage(2)  if  ( (!defined $optctl{cell}) && (!defined $optctl{cell_group}) );
die pod2usage(2)  if  ( defined($optctl{detail}) ) and ( (!defined $optctl{ago_unit}) || (!defined $optctl{ago_value}) );
die pod2usage(2)  if  ( defined($optctl{detail}) ) and ( (lc($optctl{ago_unit}) ne lc("DAY")) && (lc($optctl{ago_unit}) ne lc("HOUR")) );
die pod2usage(2)  if  ( defined($optctl{detail}) ) and ( $optctl{ago_value} !~ (/^\d+$/) );
die pod2usage(2)  if  ( defined($optctl{detail}) ) and ( $optctl{ago_unit} !~ /^[a-zA-Z]+$/ );

$topnum   = defined($optctl{topn}) ? $optctl{topn} : 5 ;
$dclipref = "-g $optctl{'cell_group'} --serial" if defined $optctl{'cell_group'} ;
$dclipref = "-c $optctl{'cell'} --serial" if defined $optctl{'cell'} ;
$dclitail = "attributes cachedSize,cachedWriteSize,cachedKeepSize,dbUniqueName,hitCount,missCount,objectNumber";

if (lc($optctl{ago_unit}) eq lc("DAY")) {
         my $is_between = (sort {$a <=> $b} $lower_day, $upper_day, $optctl{ago_value})[1] == $optctl{ago_value} ;
        die pod2usage(2) if ($is_between!=1) ;
}
elsif (lc($optctl{ago_unit}) eq lc("HOUR")) {
        my $is_between = (sort {$a <=> $b} $lower_hour, $upper_hour, $optctl{ago_value})[1] == $optctl{ago_value} ;
         die pod2usage(2) if ($is_between!=1) ;
}
## End Command line argument handling ##

## Build ARRAYS ##
open(F,"dcli ${dclipref} cellcli -e list flashcache attributes name,size|") or die "Can't run dcli command: $!";
while (<F>) {
        @words= split ' ';
        $words[2]=~s/G//;
        $cbytes=1024*1024*1024*$words[2];
        $cell{$words[0]}+=$cbytes; #Array for total flashcache size by cell
}
close(F) or die "Cannot close dcli command: $!";

open(F,"dcli ${dclipref} cellcli -e list flashcachecontent ${dclitail}|") or die "Can't run dcli command: $!";
while (<F>) {
        @words= split ' ' ;
        $cached{$words[0]          }+=$words[1] + $words[2] + $words[3]; # Array for storage by cell
        $r_cached{$words[0]        }+=$words[1]; # Array for read storage by cell
        $w_cached{$words[0]        }+=$words[2]; # Array for write storage by cell
        $k_cached{$words[0]        }+=$words[3]; # Array for keep read storage by cell
        $r_db{$words[4]            }+=$words[1]; # Array for read storage by DB
        $w_db{$words[4]            }+=$words[2]; # Array for write storage by DB
        $k_db{$words[4]            }+=$words[3]; # Array for keep read storage by DB
        $r_mb{$words[4]}{$words[7] }+=round($words[1]/1024/1024);
        $w_mb{$words[4]}{$words[7] }+=round($words[2]/1024/1024);
        $k_mb{$words[4]}{$words[7] }+=round($words[3]/1024/1024);
        $h_cnt{$words[4]}{$words[7]}+=$words[5];
        $m_cnt{$words[4]}{$words[7]}+=$words[6];

        $db{$words[4]     } = 1;
        $obj_num{$words[7]} = 1;
}
close(F) or die "Cannot close dcli command: $!";

foreach $db_name ( sort keys %db ) {
        foreach $objnum ( sort keys %obj_num ) {
          $tot{$db_name}{$objnum} =($h_cnt{$db_name}{$objnum} + $m_cnt{$db_name}{$objnum});
          $hit_pct{$db_name}{$objnum} = round(($tot{$db_name}{$objnum} ? $h_cnt{$db_name}{$objnum} / $tot{$db_name}{$objnum} : 0) * 100);

          push @ARRY, join ' ',$r_mb{$db_name}{$objnum},
                               $w_mb{$db_name}{$objnum},
                               $k_mb{$db_name}{$objnum},
                               $db_name,
                               $h_cnt{$db_name}{$objnum},
                               $m_cnt{$db_name}{$objnum},
                               $hit_pct{$db_name}{$objnum},
                               $objnum ;
        }
}
## End of building ARRAYS ##

## Main code ##
$tcellused=0;

print "\n"; print (colored( "Flash Read/Write usage breakdown at Cell level",'BOLD white on_blue'));print "\n";
printf "%-15s %12s %12s %12s %12s %12s %12s\n", "Cell Server", "Avail", "Read_Used", "%Read_used", "Write_Used", "%Write_Used", "%Total_Used";
printf "%-15s %-12s %-12s %-12s %-12s %-12s %12s\n", "-"x15, "-"x12, "-"x12, "-"x12, "-"x12, "-"x12, "-"x12;
foreach my $key (sort keys %cell) {
        $celltot    =$cell{$key}/1024/1024/1024;
        $cellused   =$cached{$key}/1024/1024/1024;
        $r_cellused =($r_cached{$key} + $k_cached{$key})/1024/1024/1024;
        $w_cellused =$w_cached{$key}/1024/1024/1024;
        $tcellused  =$tcellused + $r_cellused + $w_cellused;
        $r_pctused  =100 * ($r_cellused / $celltot);
        $w_pctused  =100 * ($w_cellused / $celltot);
        $pctused    =100 * ($cellused / $celltot);
        chop($key) ;
        printf "%-15s %12.2f %12.2f %12.2f %12.2f %12.2f %12.2f\n", "$key", $celltot, $r_cellused, $r_pctused, $w_cellused, $w_pctused, $pctused;
}
#printf "\n%20s %-8.2f\n\n", "Total GB used:", $tcellused;

print "\n"; print (colored( "Flash Read/Write usage breakdown at Database level",'BOLD white on_blue')); print "\n";
printf "%-15s %13s %13s %13s %13s %13s\n", "DB", "Read_DBUsed", "%Read_DBUsed", "Write_DBUsed", "%Write_DBUsed", "%Total_DBUsed";
printf "%-15s %13s %13s %13s %13s %13s\n", "-"x15, ,"-"x13, "-"x13, "-"x13, "-"x13, "-"x13;
foreach my $key (sort keys %r_db) {
        $r_dbused    =round(($r_db{$key} + $k_db{$key})/1024/1024/1024);
        $w_dbused    =round($w_db{$key}/1024/1024/1024);
        $rdb_pctused =round(100 * ($r_dbused / $tcellused));
        $wdb_pctused =round(100 * ($w_dbused / $tcellused));
        $tot_pctused =round(100 * (($r_dbused + $w_dbused) / $tcellused));
        $objd2 = join (' ',$key,$r_dbused,$rdb_pctused,$w_dbused,$wdb_pctused,$tot_pctused) ;
        push @ARRY2, $objd2 ;
}
@sort_ARRY2= sort_array(\@ARRY2,5,'desc');
foreach my $line ( @sort_ARRY2 ) {
    @TARRY2 = split ' ', $line ;
        printf "%-15s %13s %13s %13s %13s %13s\n", $TARRY2[0],$TARRY2[1],$TARRY2[2],$TARRY2[3],$TARRY2[4],$TARRY2[5], ;
}

print "\n"; print (colored( "Top $topnum objects consuming Flash memory for Read",'BOLD white on_blue')); print "\n";
printf "%-15s %15s %15s %15s %15s %15s %15s %15s\n", "Read_CachedMB", "Write_CacheMB", "Keep_CachedMB", "HitCount", "MissCount", "Hit_Percentage", "Database", "Object ID";
printf "%-15s %15s %15s %15s %15s %15s %15s %15s\n", "-"x15, "-"x15, "-"x15, "-"x15, "-"x15, "-"x15, "-"x15, "-"x15 ;
$cnt=0;
@sort_R_CachedMB= sort_array(\@ARRY,0,'desc');
foreach my $line (@sort_R_CachedMB) {
        last if $cnt eq $topnum;
        @TARRY = split ' ', $line ;
        printf "%-15s %15s %15s %15s %15s %15s %15s %15s\n", $TARRY[0],$TARRY[1],$TARRY[2],$TARRY[4],$TARRY[5],$TARRY[6],$TARRY[3],$TARRY[7] ;
        $cnt++;
}

print "\n"; print (colored( "Top $topnum objects consuming Flash memory for Write",'BOLD white on_blue')); print "\n";
printf "%-15s %15s %15s %15s %15s %15s %15s %15s\n", "Write_CachedMB", "Read_CacheMB", "Keep_CachedMB", "HitCount", "MissCount", "Hit_Percentage", "Database", "Object ID";
printf "%-15s %15s %15s %15s %15s %15s %15s %15s\n", "-"x15, "-"x15, "-"x15, "-"x15, "-"x15, "-"x15, "-"x15, "-"x15 ;
$cnt=0;
@sort_W_CachedMB= sort_array(\@ARRY,1,'desc');
foreach my $line (@sort_W_CachedMB) {
        last if $cnt eq $topnum;
        @TARRY = split ' ', $line ;
        printf "%-15s %15s %15s %15s %15s %15s %15s %15s\n", $TARRY[1],$TARRY[0],$TARRY[2],$TARRY[4],$TARRY[5],$TARRY[6],$TARRY[3],$TARRY[7] ;
        $cnt++;
}

print "\n"; print (colored( "Top $topnum objects having highest Flash Hit Percentage",'BOLD white on_blue')); print "\n";
printf "%-15s %15s %15s %15s %15s %15s %15s %15s\n", "Hit_Percentage", "HitCount", "MissCount", "Write_CachedMB", "Read_CacheMB", "Keep_CachedMB", "Database", "Object ID";
printf "%-15s %15s %15s %15s %15s %15s %15s %15s\n", "-"x15, "-"x15, "-"x15, "-"x15, "-"x15, "-"x15, "-"x15, "-"x15 ;
$cnt=0;
@sort_Hit_Pct= sort_array(\@ARRY,6,'desc');
foreach my $line (@sort_Hit_Pct) {
        last if $cnt eq $topnum;
        @TARRY = split ' ', $line ;
        printf "%-15s %15s %15s %15s %15s %15s %15s %15s\n", $TARRY[6],$TARRY[4],$TARRY[5],$TARRY[0],$TARRY[1],$TARRY[2],$TARRY[3],$TARRY[7] ;
        $cnt++;
}

###################################################
############## Detail section start################
###################################################
if ( defined($optctl{detail}) ) {

        my $fromtime = `date --date=\" $optctl{ago_value} $optctl{ago_unit} ago \" +%Y-%m-%dT%H:%M:%S%z` ;
        chomp ($fromtime);

        #################### IOPS between Disk and Flash ##################
        my %df; my %hf; my %dd; my %hd;

        open(F,"dcli ${dclipref} \"cellcli -e list metrichistory attributes collectionTime,name,metricObjectName,metricValue where name like \\'CD_IO_RQ_.?_.?.?_SEC\\' and collectionTime \\> \\'$fromtime\\'\"|") or die "Can't run dcli command: $!";
        while (<F>) {
                next unless /\w/;

                my($datetime,$met_name,$met_obj,$speed) = (split)[1,2,3,4];
                my $ddhh   = substr $datetime,0,13;
                my $day    = substr $datetime,0,10;

                $df{$day   }{flash}{read}  += $speed if ($met_obj=~/^FD/ && $met_name=~/CD_IO_RQ_R_.._SEC/);
                $hf{$ddhh  }{flash}{read}  += $speed if ($met_obj=~/^FD/ && $met_name=~/CD_IO_RQ_R_.._SEC/);

                $df{$day   }{flash}{write} += $speed if ($met_obj=~/^FD/ && $met_name=~/CD_IO_RQ_W_.._SEC/);
                $hf{$ddhh  }{flash}{write} += $speed if ($met_obj=~/^FD/ && $met_name=~/CD_IO_RQ_W_.._SEC/);

                $dd{$day   }{disk}{read}   += $speed if ($met_obj=~/^CD/ && $met_name=~/CD_IO_RQ_R_.._SEC/);
                $hd{$ddhh  }{disk}{read}   += $speed if ($met_obj=~/^CD/ && $met_name=~/CD_IO_RQ_R_.._SEC/);

                $dd{$day   }{disk}{write}  += $speed if ($met_obj=~/^CD/ && $met_name=~/CD_IO_RQ_W_.._SEC/);
                $hd{$ddhh  }{disk}{write}  += $speed if ($met_obj=~/^CD/ && $met_name=~/CD_IO_RQ_W_.._SEC/);
        }

        if ( $optctl{ago_unit} eq "DAY" ) {
                print "\n"; print (colored( "Breakdown of Read/Write I/O operations(IOPS) between Disk and Flash",'BOLD white on_blue')); print "\n";
                print "      Time            Disk Reads          Flash Reads          Disk Writes         Flash Writes      %Flash Read PCT     %Flash Write PCT"; print "\n";
                print "----------  --------------------  -------------------  -------------------  -------------------  -------------------  -------------------";
                print "\n";

                for my $key (sort keys %dd){
                        print "$key ";
                        printf " %20s", round($dd{$key}{disk}{read});
                        printf " %20s", round($df{$key}{flash}{read});
                        printf " %20s", round($dd{$key}{disk}{write});
                        printf " %20s", round($df{$key}{flash}{write});

                        my $flash_wpct = 100*$df{$key}{flash}{write}/($dd{$key}{disk}{write} + $df{$key}{flash}{write}) ;
                        my $flash_rpct = 100*$df{$key}{flash}{read}/($dd{$key}{disk}{read} + $df{$key}{flash}{read}) ;

                        printf "%20s", round($flash_rpct); print "%";
                        printf "%20s", round($flash_wpct); print "%";
                        print "\n";
                }
        }

        if ( $optctl{ago_unit} eq "HOUR" ) {
                print "\n\n";
                print "      Time            Disk Reads          Flash Reads          Disk Writes         Flash Writes      %Flash Read PCT     %Flash Write PCT"; print "\n";
                print "----------  --------------------  -------------------  -------------------  -------------------  -------------------  -------------------";
                print "\n";

                for my $key (sort keys %hd){
                        print "$key ";
                        printf " %20s", round($hd{$key}{disk}{read});
                        printf " %20s", round($hf{$key}{flash}{read});
                        printf " %20s", round($hd{$key}{disk}{write});
                        printf " %20s", round($hf{$key}{flash}{write});

                        my $flash_wpct = 100*$hf{$key}{flash}{write}/($hd{$key}{disk}{write} + $hf{$key}{flash}{write}) ;
                        my $flash_rpct = 100*$hf{$key}{flash}{read}/($hd{$key}{disk}{read} + $hf{$key}{flash}{read}) ;

                        printf "%20s", round($flash_rpct); print "%";
                        printf "%20s", round($flash_wpct); print "%";
                        print "\n";
                }
        }

        ############## Read/Write BYTES performed on Flash ##########################
        my %h; my %d; my %sr; my %met;

        open(F,"dcli ${dclipref} \"cellcli -e list metrichistory attributes collectionTime,Name,metricValue where name like \\'FC_IO_BY_.?\\' and collectionTime \\> \\'$fromtime\\'\"|") or die "Can't run dcli command: $!";
        while(<F>){
                next unless /\w/ ;
                my($server,$datetime,$metric,$speed) = (split)[0,1,2,3] ;
                $speed =~ s/,//g ;
                my $dd     = substr $datetime,0,10 ;
                my $ddhh   = substr $datetime,0,13 ;
                if (!defined $h{$ddhh  }{$server}{$metric}{min} ) { $h{$ddhh  }{$server}{$metric}{min} = $speed; } ;
                if (!defined $d{$dd    }{$server}{$metric}{min} ) { $d{$dd    }{$server}{$metric}{min} = $speed; } ;
                $h{$ddhh  }{$server}{$metric}{max}  = max(($h{$ddhh  }{$server}{$metric}{max} || 0),$speed) ;
                $d{$dd    }{$server}{$metric}{max}  = max(($d{$dd    }{$server}{$metric}{max} || 0),$speed) ;
                $h{$ddhh  }{$server}{$metric}{min}  = min(($h{$ddhh  }{$server}{$metric}{min}),$speed) ;
                $d{$dd    }{$server}{$metric}{min}  = min(($d{$dd    }{$server}{$metric}{min}),$speed) ;
                $sr{$server  } = 1;
                $met{$metric } = 1;
        }

        my @sr = sort keys %sr;
        my @met = sort keys %met;
        my $count = keys %sr;

        print "\n"; print (colored( "Breakdown of Read/Write BYTES performed on Flash",'BOLD white on_blue')); print "\n";
        print "      Time           FC_IO_BY_R           FC_IO_BY_W"; print "\n";
        print "----------   ------------------   ------------------";
        #printf "%30s",$_ for (@met);
        print "\n";

        if ( $optctl{ago_unit} eq "HOUR" ) {
          for my $key (sort keys %h){
            print "$key";
             for my $met_key (sort keys %met){
               $h{$key}{$met_key}{total} +=  ( $h{$key}{$_}{$met_key}{max} - $h{$key}{$_}{$met_key}{min} ) for (@sr) ;
               printf "%21s", $h{$key}{$met_key}{total} ;
              }
            print "\n";
          }
        }

        if ( $optctl{ago_unit} eq "DAY" ) {
          for my $key (sort keys %d){
            print "$key";
             for my $met_key (sort keys %met){
               $d{$key}{$met_key}{total} +=  ( $d{$key}{$_}{$met_key}{max} - $d{$key}{$_}{$met_key}{min} ) for (@sr) ;
               printf "%21s", $d{$key}{$met_key}{total} ;
             }
            print "\n";
          }
        }

        ############ Average Throughput for each Database ####################

        my %h; my %d; my %db; my %sr;

        open(F,"dcli ${dclipref} \"cellcli -e list metrichistory attributes collectionTime,metricObjectName,metricValue where name like \\'DB_FC_IO_BY_SEC\\' and collectionTime \\> \\'$fromtime\\'\"|") or die "Can't run dcli command: $!";
        while(<F>){
                next unless /\w/;
                my($server,$datetime,$database,$speed) = (split)[0,1,2,3];
                my $ddhh = substr $datetime,0,13;
                my $dd   = substr $datetime,0,10;
                $h{$ddhh  }{$database}{total} += $speed;
                $d{$dd    }{$database}{total} += $speed;
                $h{$ddhh  }{$database}{max} = max(($h{$ddhh  }{$database}{max} || 0),$speed);
                $d{$dd    }{$database}{max} = max(($d{$dd    }{$database}{max} || 0),$speed);
                $db{$database} = 1;
                $sr{$server  } = 1;
        }

        my @db = sort keys %db;
        my $count = keys %sr;

        print "\n"; print (colored( "Throughput(MB/Sec) breakdown at each Database level",'BOLD white on_blue')); print "\n";

        if ( $optctl{ago_unit} eq "DAY" ) {
                # DAY SECTION START - AVG
                for my $key (sort keys %d){
                        for (@db) { $d{$key}{$_}{avg} = round($d{$key}{$_}{total} / ($count * 60 * 24))} ;
                }
                set_column_widths(\%d, \%db, \@db, "avg");
                print (colored( "Average Throughput:-",'BOLD white on_blue')); print "\n";
                print "      Time";
                printf " %$db{$_}s", $_ for (@db);
                print "\n----------"; printf " %$db{$_}s", "-"x$db{$_} for (@db); print "\n";

                for my $key (sort keys %d){
                        print $key;
                        printf " %$db{$_}s", $d{$key}{$_}{avg} for (@db);
                        print "\n";
                }
                # DAY SECTION END - AVG

                # DAY SECTION START - MAX
                set_column_widths(\%d, \%db, \@db, "max");
                print (colored( "Maximum Throughput:-",'BOLD white on_blue')); print "\n";
                print "      Time";
                printf " %$db{$_}s", $_ for (@db);
                print "\n----------"; printf " %$db{$_}s", "-"x$db{$_} for (@db); print "\n";

                for my $key (sort keys %d){
                        print $key;
                        printf " %$db{$_}s", $d{$key}{$_}{max} for (@db);
                        print "\n";
                }
                # DAY SECTION END - MAX
        }

        if ( $optctl{ago_unit} eq "HOUR" ) {
                # HOUR SECTION START - AVG
                for my $key (sort keys %h){
                        for (@db) { $h{$key}{$_}{avg} = round($h{$key}{$_}{total} / ($count * 60))} ;
                }
                set_column_widths(\%h, \%db, \@db, "avg");
                print (colored( "Average Throughput:-",'BOLD white on_blue')); print "\n";
                print "         Time";
                printf " %$db{$_}s", $_ for (@db);
                print "\n----------"; printf " %$db{$_}s", "-"x$db{$_} for (@db); print "\n";

                for my $key (sort keys %h){
                        print "$key ";
                        printf " %$db{$_}s", $h{$key}{$_}{avg} for (@db);
                        print "\n";
                }
                # HOUR SECTION END - AVG

                # HOUR SECTION START - MAX
                set_column_widths(\%h, \%db, \@db, "max");
                print (colored( "Maximum Throughput:-",'BOLD white on_blue')); print "\n";
                print "         Time";
                printf " %$db{$_}s", $_ for (@db);
                print "\n----------"; printf " %$db{$_}s", "-"x$db{$_} for (@db); print "\n";

                for my $key (sort keys %h){
                        print "$key ";
                        printf " %$db{$_}s", $h{$key}{$_}{max} for (@db);
                        print "\n";
                }
                # HOUR SECTION END - MAX
        }

        ############ Average IOPS for each Database ####################

        my %h; my %d; my %db; my %sr;

        open(F,"dcli ${dclipref} \"cellcli -e list metrichistory attributes collectionTime,metricObjectName,metricValue where name like \\'DB_FD_IO_RQ_.?.?_SEC\\' and collectionTime \\> \\'$fromtime\\'\"|") or die "Can't run dcli command: $!";
        while(<F>){
                next unless /\w/;
                my($server,$datetime,$database,$speed) = (split)[0,1,2,3];
                my $ddhh = substr $datetime,0,13;
                my $dd   = substr $datetime,0,10;
                $h{$ddhh  }{$database}{total} += $speed;
                $d{$dd    }{$database}{total} += $speed;
                $h{$ddhh  }{$database}{max} = max(($h{$ddhh  }{$database}{max} || 0),$speed);
                $d{$dd    }{$database}{max} = max(($d{$dd    }{$database}{max} || 0),$speed);
                $db{$database} = 1;
                $sr{$server  } = 1;
        }

        my @db = sort keys %db;
        my $count = keys %sr;

        print "\n"; print (colored( "IOPS breakdown at each Database level",'BOLD white on_blue')); print "\n";

        if ( $optctl{ago_unit} eq "DAY" ) {
                # DAY SECTION START - AVG
                for my $key (sort keys %d){
                        for (@db) { $d{$key}{$_}{avg} = round($d{$key}{$_}{total} / ($count * 60 * 24 ))} ;
                }
                set_column_widths(\%d, \%db, \@db, "avg");
                print (colored( "Average IOPS:-",'BOLD white on_blue')); print "\n";
                print "      Time";
                printf " %$db{$_}s", $_ for (@db);
                print "\n----------"; printf " %$db{$_}s", "-"x$db{$_} for (@db); print "\n";

                for my $key (sort keys %d){
                        print $key;
                        printf " %$db{$_}s", $d{$key}{$_}{avg} for (@db);
                        print "\n";
                }
                # DAY SECTION END - AVG

                # DAY SECTION START - MAX
                set_column_widths(\%d, \%db, \@db, "max");
                print (colored( "Maximum IOPS:-",'BOLD white on_blue')); print "\n";
                print "      Time";
                printf " %$db{$_}s", $_ for (@db);
                print "\n----------"; printf " %$db{$_}s", "-"x$db{$_} for (@db); print "\n";

                for my $key (sort keys %d){
                        print $key;
                        printf " %$db{$_}s", $d{$key}{$_}{max} for (@db);
                        print "\n";
                }
                # DAY SECTION END - MAX
        }

        if ( $optctl{ago_unit} eq "HOUR" ) {
                # HOUR SECTION START - AVG
                for my $key (sort keys %h){
                        for (@db) { $h{$key}{$_}{avg} = round($h{$key}{$_}{total} / ($count * 60 ))} ;
                }
                set_column_widths(\%h, \%db, \@db, "avg");
                print (colored( "Average IOPS:-",'BOLD white on_blue')); print "\n";
                print "         Time";
                printf " %$db{$_}s", $_ for (@db);
                print "\n----------"; printf " %$db{$_}s", "-"x$db{$_} for (@db); print "\n";

                for my $key (sort keys %h){
                        print "$key ";
                        printf " %$db{$_}s", $h{$key}{$_}{avg} for (@db);
                        print "\n";
                }
                # HOUR SECTION END - AVG

                # HOUR SECTION START - MAX
                set_column_widths(\%h, \%db, \@db, "max");
                print (colored( "Maximum IOPS:-",'BOLD white on_blue')); print "\n";
                print "         Time";
                printf " %$db{$_}s", $_ for (@db);
                print "\n----------"; printf " %$db{$_}s", "-"x$db{$_} for (@db); print "\n";

                for my $key (sort keys %h){
                        print "$key ";
                        printf " %$db{$_}s", $h{$key}{$_}{max} for (@db);
                        print "\n";
                }
                # HOUR SECTION END - MAX
        }





}

__END__

=head1 NAME

report-flash.pl

This script will report details of Flash at either individual cell server or for multiple cell servers. Password less connection by user celladmin to all the Cell servers should be configured to use this script. It may take about 5-8 minutes on Full Exadata Rack.


=head1 SYNOPSIS

report-flash.pl [options]

 Options:
  --help       =>brief help message
  --man        =>full documentation
  --cell       =>name of cell server, can be used specify multiple cell servers delimited by commas
  --cell_group =>file name containing cell server names
  --topn       =>limit top results to specified number. Default:5
  --detail     =>enables extended detail output to include IOPS, Throughput and many more.
  --ago_unit   =>detail report window in DAY or HOUR
  --ago_value  =>detail report window value

  report-flash.pl --cell cellserver1

  report-flash.pl --cell_group cell_group_file.txt --topn 10

  report-flash.pl --cell_group cell_group_file.txt --topn 10 --detail --ago_unit DAY --ago_value 3

=head1 OPTIONS

=over 8

=item B<-help>

Print a brief help message and exits.

=item B<-man>

Prints the manual page and exits.

=item B<--cell>

Pass Cell server name, multiple Cell server names can be passed by using comma(,) as delimiter.

=item B<--cell_group>

Pass filename containing Cell server names.

=item B<--topn>

Pass number to restrict the topn result set, by default top 5 results will be shown.

=item B<--detail>

This option enables the detail reporting, if used then specifying ago_unit and ago_value parameter are mandatory.

=item B<--ago_unit>

Pass unit as either DAY or HOUR.

=item B<--ago_value>

Pass unit value. Due to default retention in Exadata is 15 days, max HOUR value can be 360 and max DAY value can be 15.

=back

=head1 DESCRIPTION

B<report-flash.pl> This script will report details of Flashcache at either individual cell server or for multiple cell servers. Password less ssh connection by celladmin user to all the cell servers is the prerequisite for this script to work. Report is mainly focused on Flashcache usage, IOPS, MBPS at Cell server level and also at individual database level. One of the important feature of this script is to report Flashcache used for write purpose when Writeback Flashcache mode is enabled, this will be helpful in finding the top most objects occupying the Flashcache for write purpose. Due to unaccounting of write usage in Flashcache usage, when calculating total percentage of Flashcache usage by including both read and write rounding error will occur. Which means you may find more than 100% usage due to write space used accounted in read space. This script caculates the Flashcache usage by using 'list flashcachecontent' which will be always less then what FC_BY_USED reports, due to accounting of Flashcache used by any other clients(RMAN) in FC_BY_USED metric.

"Flash Read/Write usage breakdown at Cell level" ==> Provides Flashcache read and write usage at each Cell server level. Please note that Total usage percentage may show beyond 100% as write usage is not accounted seperately, it usually resides in Read usage.

"Flash Read/Write usage breakdown at Database level" ==> Provides Flashcache read and write usage at each Database level. Please note that Total usage percentage may show beyond 100% as write usage is not accounted seperately, it usually resides in Read usage.

"Top <N> objects consuming Flash memory for Read" ==> Provides top n objects consuming Flashcache for read purpose. Please note that some objects may not belong to any database, one of the reason can be due to dropped objects still residing in Flashcache.

"Top <N> objects consuming Flash memory for Write" ==> Provides top n objects consuming Flashcache for write purpose. Please note that some objects may not belong to any database, one of the reason can be due to dropped objects still residing in Flashcache.

"Top <N> objects having highest Flash Hit Percentage" ==> Provides top n objects having higest hit percentage. Please note that these are cummulative, so it may not report current hit ratio accurately, but still gives pretty good idea.

"Breakdown of Read/Write I/O operations(IOPS) between Disk and Flash" ==> Provides distribution of I/O operations among Disk and Flash, can be used to see how much percentage of I/O are satisfied through Flashcache when compared with Grid Disk.

"Breakdown of Read/Write BYTES performed on Flash" ==> Provides amount of Read and Write BYTES distribution, can be used to see the trend of peak Read and Write over the period of time.

"Throughput(MB/Sec) breakdown at each Database level" ==> Provides average and maximum Throughput at each database level, no matter how many database we hosts the column width will set automatically according to longest database name or metric value.

"IOPS breakdown at each Database level" ==> Provides average and maximum IOPS at each database level, no matter how many database we hosts the column width will set automatically according to longest database name or metric value.


Metrics used by this scripts:

   Calculate Object level Flashcache usage
   FLASHCACHECONTENT

   Calculate breakdown of IOPS between Flashcache and
   CD_IO_RQ_R_LG_SEC
   CD_IO_RQ_R_SM_SEC
   CD_IO_RQ_W_LG_SEC
   CD_IO_RQ_W_SM_SEC

   Calculate breakdown of Read/Write on Flashcache
   FC_IO_BY_R
   FC_IO_BY_W

   Calculate Total MB/s of Flashcache
   DB_FC_IO_BY_SEC (DB_FD_IO_BY_SEC and DB_FL_IO_BY_SEC are not used)

   Calculate Total IOPS of Flashcache
   DB_FD_IO_RQ_LG_SEC
   DB_FD_IO_RQ_SM_SEC


=head1 EXAMPLES

 report-flash.pl --help

 report-flash.pl --man

 report-flash.pl --cell cellserver1

 report-flash.pl --cell cellserver1,cellserver2

 report-flash.pl --cell_group cell_group_file.txt --topn 10

 report-flash.pl --cell_group cell_group_file.txt --topn 10

 report-flash.pl --cell_group cell_group_file.txt --topn 10 --detail --ago_unit DAY --ago_value 3

 report-flash.pl --cell_group cell_group_file.txt --topn 10 --detail --ago_unit HOUR --ago_value 10

=cut

