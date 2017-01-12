    
/*Name your library.  Note change directory to your own*/
libname mydata "/home/josephsloan20140/sasuser.v94/Output/498Data";

/*Use Proc Import to convert each file of the dataset to a SAS Dataset*/
proc import datafile="/home/josephsloan20140/sasuser.v94/Output/498Data/app_events.csv" out=mydata.app_events dbms=dlm replace;
   delimiter=",";
   getnames=yes;
run;

proc import datafile="/home/josephsloan20140/sasuser.v94/Output/498Data/app_labels.csv" out=mydata.app_labels dbms=dlm replace;
   delimiter=",";
   getnames=yes;
run;

proc import datafile="/home/josephsloan20140/sasuser.v94/Output/498Data/events.csv" out=mydata.events dbms=dlm replace;
   delimiter=",";
   getnames=yes;
run;

proc import datafile="/home/josephsloan20140/sasuser.v94/Output/498Data/gender_age_train.csv" out=mydata.gender_age_train dbms=dlm replace;
   delimiter=",";
   getnames=yes;
run;

proc import datafile="/home/josephsloan20140/sasuser.v94/Output/498Data/gender_age_test.csv" out=mydata.gender_age_test dbms=dlm replace;
   delimiter=",";
   getnames=yes;
run;

proc import datafile="/home/josephsloan20140/sasuser.v94/Output/498Data/label_categories.csv" out=mydata.label_categories dbms=dlm replace;
   delimiter=",";
   getnames=yes;
run;

proc import datafile="/home/josephsloan20140/sasuser.v94/Output/498Data/phone_brand_device_model.csv" out=mydata.phone_brand_device_model dbms=dlm replace;
   delimiter=",";
   getnames=yes;
run;

proc import datafile="/home/josephsloan20140/sasuser.v94/Output/498Data/sample_submission.csv" out=mydata.sample_submission dbms=dlm replace;
   delimiter=",";
   getnames=yes;
run;


*List the contents of app_events;
 proc contents data= mydata.app_events;
 run;
 
 
*Start of Proc FREQ statement for CATEGORICAL VARIABLES;
*This does not include a table for 'event_id' or 'app_id' as this would create a huge table;

title "Categorical Frequency Tables - event_id and app_id not included because of size of table";
proc freq data = mydata.app_events;
tables is_installed is_active / missing;
run;

title "Boxplots and histograms";
proc sgplot data= mydata.app_events;
histogram app_id;
run;


title "Boxplots and histograms";
proc sgplot data= mydata.app_events;
histogram event_id;
run;


title "Boxplots and histograms";
proc sgplot data= mydata.app_events;
histogram is_installed;
run;

title "Boxplots and histograms";
proc sgplot data= mydata.app_events;
histogram is_active;
run;


 
 *List the contents of app_labels;
 proc contents data= mydata.app_labels;
 run;
 

title "Categorical Frequency Tables of app_labels";
proc freq data = mydata.app_labels;
tables label_id / missing;
run;

title "Boxplots and histograms";
proc sgplot data= mydata.app_labels;
histogram label_id;
run;



*List the contents of events;
proc contents data= mydata.events;
run;


title "Means of Event Data - Notice the latitudes and longitudes have modes of Zero, and device_id also has a mode";
proc means data=mydata.events n nmiss mean std median mode min max p1 p5 p10 p25 p75 p90 p95 p99 sum uss css;
var event_id device_id timestamp longitude latitude;
run;

title "Boxplots and histograms";
proc sgplot data= mydata.events;
histogram event_id;
run;

title "Boxplots and histograms";
proc sgplot data= mydata.events;
histogram device_id;
run;


title "Boxplots and histograms";
proc sgplot data= mydata.events;
histogram timestamp;
run;

title "Boxplots and histograms";
proc sgplot data= mydata.events;
histogram longitude;
run;

title "Boxplots and histograms";
proc sgplot data= mydata.events;
histogram latitude;
run;

title "Categorical Frequency Tables of Events data";
proc freq data = mydata.events;
tables latitude longitude / missing;
run;


*List the contents of gender_age_train;
proc contents data= mydata.gender_age_train;
run;


title "Means of gender_age_train Data";
proc means data=mydata.gender_age_train n nmiss mean std median mode min max p1 p5 p10 p25 p75 p90 p95 p99 sum uss css;
var age device_id;
run;

title "Boxplots and histograms";
proc sgplot data= mydata.gender_age_train;
histogram age;
run;

title "Boxplots and histograms";
proc sgplot data= mydata.gender_age_train;
histogram device_id;
run;

title "Categorical Frequency Tables of gender_age_train data";
proc freq data = mydata.gender_age_train;
tables gender group / missing;
run;


title "Bar Chart";
proc sgplot data= mydata.gender_age_train;
vbar gender;
run;


title "Bar Chart";
proc sgplot data= mydata.gender_age_train;
vbar group;
run;


*List the contents of phone_brand_device_model;
proc contents data= mydata.phone_brand_device_model;
run;


title "Means of phone_brand_device_model Data";
proc means data=mydata.phone_brand_device_model n nmiss mean std median mode min max p1 p5 p10 p25 p75 p90 p95 p99 sum uss css;
var device_id;
run;


title "Boxplots and histograms";
proc sgplot data= mydata.phone_brand_device_model;
histogram device_id;
run;

title "Categorical Frequency Tables of phone_brand_device_model data";
proc freq data = mydata.phone_brand_device_model;
tables device_model phone_brand / missing;
run;


title "Bar Chart - No output for device_model because of too many device models";
proc sgplot data= mydata.phone_brand_device_model;
vbar phone_brand;
run;
