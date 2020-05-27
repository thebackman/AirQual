# AirQual

Air quality monitoring with the raspberry pi.

 - calls the sensor hourly
 - stores the data in sqlite
 - Does not send a csv at the end of the day to dropbox, maybe implement as
 separate job
 
 
# Note
 
 To control the _SDS011_ sensor that I am using I have "borrowed" the
 control code from https://github.com/ikalchev/py-sds011.
 