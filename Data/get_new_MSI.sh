#!/bin/bash

#OS: sets your operating system: choices are 'Linux', 'OSx', 'Windows'
OS='Linux'

if [ "$OS" == "OSx" ]; then
  date_cmd='gdate'
else
  date_cmd='date'
fi


#CMD_PATH: this is the path to where the dhusget.sh command is. dhusget.sh (or variant) is
#the ROOT_CMD (the downloader we are going to call).
CMD_PATH='/home/ubuntu/project/data'
ROOT_CMD='dhusget.sh'

#USERNAME: your CODA username
USERNAME='nathansun'

#PASSWORD: your CODA password
PASSWORD='nathansun'

# this is where you specify the path where you want data downloaded: e.g. /test/test. The actual storage
# path will be /test/test/<SENSOR>/<YEAR>/<MONTH>/<DAY>/
OUT_ROOT='/home/ubuntu/project/data'

# this is where you select which sensor you want: choices are 'OLCI','SLSTR' or 'SRAL'
SENSOR='MSI'

#MISSION='Sentinel-2'

# this is where you set the dates you want to download: start and end in the following format: 'YYYY-MM-DD'
# file latest.txt in the running directory holds the last downloaded date. The end date is now inclusive, but begin date is not
#DATE_START=$(cat "latest.txt")
DATE_START='2021-06-10'

# for testing. Will replace with current date eventually
#DATE_END='2020-08-31'
DATE_END=$($date_cmd +%Y'-'%m'-'%d)
ROLL_END=$($date_cmd -d "$DATE_END + 1 day" +%Y-%m-%d)
printf "%s" $DATE_END

# this sets the number of downloads to attempt at the same time; CONCUR > 1 only for DPROD='DATA'
CONCUR='4'

# this sets the number of download retries
RETRIES='5'

# this sets the maximum number of products to download per day; it should be left at 100
NUM_PROD='100'

# this sets the geographical coordinates for the download area. Any tile that touches this box will
# be downloaded, even if most of the data is outside of the box. Unavoidable at this stage, but may be
# improved in the future. Format should be 'lon1,lat1:lon2,lat2'. E.g. for west africa: -10.0,-4.0:12.5, 8.0'
#COORDS='-10.0,-4.0:12.5,8.0'
#COORDS='-129,51:-122.8,48.2'
#LOCATION='Victoria'
#COORDS='-123.8,48.7:-123.1,48.3'
LOCATION='Tofino'
COORDS='-125.57587267273438,49.052362345863:-124.92414837065104,48.662031981524734'
#LOCATION='PortHardy'
#COORDS='-125.475917,49.008685:-124.964481,48.744633'

# this option decides if you should download data or just test if it is present"
# DOPTION='DATA' < downloads all data
# DOPTION='TEST' < download manifests only
# DOPTION='PROD' < downloads specific products only
DOPTION='DATA'

# use if PROD is selected for products
#DPROD=('xfdumanifest.xml' 'chl_nn.nc' 'tie_geo_coordinates.nc' 'wqsf.nc' 'time_coordinates.nc' 'instrument_data.nc')

# additional string options: run dhusget.sh -help to see how to use these options.
# some examples of how these can be used:
# TOPTION is used to quickly refine the data you want to download, e.g. Level 1 or NRT only
# -----------
# TOPTION='*NT*'   : will only download NRT data
# TOPTION='*OL_2*' : will only download OLCI Level 2 data
TOPTION='S2MSI2A'

# -----------
# FOPTION is used to construct more specific arguments for downloads, again, see
# dhusget.sh -help for more information
# FOPTION='filename:S3A_OL_2*WRR*NT*'
FOPTION='platformname:Sentinel-2'

#####################################
# NO USER PARAMETERS BELOW HERE; DO NOT EDIT
#####################################
OPTIONS=" -u $USERNAME -p $PASSWORD -c $COORDS -n $CONCUR -l $NUM_PROD -N $RETRIES"

if [ "$DOPTION" == "DATA" ]; then
  OPTIONS=$OPTIONS' -o product'
else
  OPTIONS=$OPTIONS' -o manifest'
fi

if [ "$TOPTION" == "" ]; then
  echo 'No extra -T options'
else
  OPTIONS=$OPTIONS" -T $TOPTION"
fi

if [ "$FOPTION" == "" ]; then
  echo 'No extra -F options'
else
  OPTIONS=$OPTIONS" -F $FOPTION"
fi


echo "-------------------------------------------------------------------------"
echo "---------------------------------OPTIONS---------------------------------"
echo $OPTIONS
echo "-------------------------------------------------------------------------"
echo "-------------------------------------------------------------------------"

if [ "$DOPTION" != "PROD" ]; then
  DPROD=('All')
fi

DATE_ROLL=$($date_cmd -d "$DATE_START + 1 day" +%Y-%m-%d)

printf "DATE-ROLL - %s\n" $DATE_ROLL

#DATE_END=$($date_cmd -d "$DATE_END + 1 day")
#YEAR=$($date_cmd -d "$DATE_END" '+%Y')
#MONTH=$($date_cmd -d "$DATE_END" '+%m')
#DAY=$($date_cmd -d "$DATE_END" '+%d')
#DATE_END=$YEAR'-'$MONTH'-'$DAY


# loop through days
while [ "$DATE_ROLL" != "$ROLL_END" ]; do
  echo $DATE_ROLL
  YEAR=$($date_cmd -d "$DATE_ROLL" '+%Y')
  MONTH=$($date_cmd -d "$DATE_ROLL" '+%m')
  DAY=$($date_cmd -d "$DATE_ROLL" '+%d')
  OUT_DIR=$OUT_ROOT'/'$SENSOR'/'$LOCATION'/'$YEAR'/'$MONTH'/'$DAY'/'

  # create output directory if it does not exist
  if [ ! -d $OUT_DIR ]; then
    mkdir -p $OUT_DIR
  fi
  # launch command for this date
  for prod in "${DPROD[@]}"; do
    echo "-------------------------------------------------------------------------"
    echo "-----------------------------------PROD----------------------------------"
    echo $prod
    echo "-------------------------------------------------------------------------"
    echo "-------------------------------------------------------------------------"
    $CMD_PATH'/'$ROOT_CMD $OPTIONS -S $YEAR'-'$MONTH'-'$DAY'T00:00:00.000Z' -E $YEAR'-'$MONTH'-'$DAY'T23:59:59.000Z' -O $OUT_DIR -Z $prod
  done

  DATE_ROLL=$($date_cmd -d "$DATE_ROLL + 1 day")
  # reconstruct date format at -I option not always available
  YEAR=$($date_cmd -d "$DATE_ROLL" '+%Y')
  MONTH=$($date_cmd -d "$DATE_ROLL" '+%m')
  DAY=$($date_cmd -d "$DATE_ROLL" '+%d')
  DATE_ROLL=$YEAR'-'$MONTH'-'$DAY
done

