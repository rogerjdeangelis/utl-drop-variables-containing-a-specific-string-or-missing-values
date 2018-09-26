Drop variables containing a specific string or missing

This is not a big data problem (100gb is not big data)

Plese provide (system ie EG server)
proc options group=performance group=memory

A 12gb dataset took 74 seconds. A 100gb dataset
should (8.12*74) = 600 seconds or 10 minutes.


INPUT
=====

 2,000,000 Observations
     6,000 Variables
     1,000 Variables with at least one missing or "?"

                                                    | RULES
G.TENGB   (12gb dataset 200,000 obs 6001 variables) |
                                                    |
   Obs    X1    X2    X3    X7    X8    X9    X13   | Drop X1, X7 and X13
                                                    |
 90500          X     X     ?     X     X      ?    |
 90501          X     X     ?     X     X      ?    |
 90502    ?     X     X           X     X      ?    |
 90503    ?     X     X           X     X           |
 90504    ?     X     X           X     X           |
 90505          X     X           X     X      ?    |
 90506          X     X           X     X      ?    |
 90507          X     X           X     X      ?    |
 90508    ?     X     X     ?     X     X           |
 90509    ?     X     X     ?     X     X      ?    |
 90510          X     X     ?     X     X      ?    |


EXAMPLE OUTPUT
--------------

WORK.WANT  (Note X1, X7 and X13 have been dropped)

   Obs    X2    X3    X4    X5    X6    X8    X9    X10    X11    X12    X14

 90500    X     X     X     X     X     X     X      X      X      X      X
 90501    X     X     X     X     X     X     X      X      X      X      X
 90502    X     X     X     X     X     X     X      X      X      X      X
 90503    X     X     X     X     X     X     X      X      X      X      X
 90504    X     X     X     X     X     X     X      X      X      X      X


PROCESS
=======

data varDrp;
   array beenthere[0:6000] _temporary_;
   set g.tengb;
   array xs[0:6000] $8 x1-x6001;
   do idx=0 to 6000 by 6;
      if (beenthere[idx] ne 1) and xs[idx] in ("?"," ")  then do;
        beenthere[idx]=1;
        nam=vname(xs[idx]);
        output;
      end;
   end;
   keep idx nam;
run;quit;

NOTE: There were 200,000 observations read from the data set G.TENGB.
NOTE: The data set WORK.VARDRP has 1,001 observations and 2 variables.

NOTE: DATA statement used (Total process time):
      real time           30.20 seconds

/*
 WORK.VARDRP total obs=1,001

   IDX    NAM

     0    X1
     6    X7
    12    X13
   ,,,
*/

data want;
 set g.tenGb(drop=&nams);
run;quit;

/*
NOTE: There were 200,000 observations read from the data set G.TENGB.
NOTE: The data set WORK.WANT has 200,000 observations and 5,000 variables.
NOTE: DATA statement used (Total process time):
NOTE: real time           44.40 seconds

Note 10001 varibles dropped;

*                _              _       _
 _ __ ___   __ _| | _____    __| | __ _| |_ __ _
| '_ ` _ \ / _` | |/ / _ \  / _` |/ _` | __/ _` |
| | | | | | (_| |   <  __/ | (_| | (_| | || (_| |
|_| |_| |_|\__,_|_|\_\___|  \__,_|\__,_|\__\__,_|

;


libname g "g:/wrk";

data g.tenGb;
  array xs[0:6000] $8 x1-x6001 (6001*"X");
  do rec=1 to 200000;
    do idx=0 to 6000 by 6;
       if 90000 < rec < 110000 then
          xs[idx] = ifc(uniform(1234)<.5," ","?");
    end;
    output;
  end;
  drop rec idx;

run;quit;

