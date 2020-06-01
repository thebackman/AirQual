""" puts sensor to sleep when pi reboots """


## CRON ########################################################################


# @reboot python3 /home/pi/Projects/AirStuff/sleep_at_startup.py


## RUN #########################################################################

import control_functions
sensor = control_functions.SDS011("/dev/ttyUSB0", use_query_mode=True)
sensor.sleep()
