"""
This module contains methods related to finding the number
of hours worked based on a start and end time.


__licence__ = ""
__revision__ = " "
__docformat__ = 'reStructuredText"
"""
from datetime import datetime, timedelta
import pytz

END_OF_DAY_HOUR = 17
START_OF_DAY_HOUR = 8
LENGTH_OF_BREAKS = 1

def _match_tz(dt1, dt2):
    """
    Make both times have the same timezone
    
    TODO put in utilities file
    """
    if str(dt1.tz) != str(dt2.tz):
        dt2 = dt2.astimezone(dt1.tz)
    return dt1, dt2  

def hours_worked(start,end):
    """
    returns the number of hours worked between the start and end date

    The number of hours worked between two dates ...

    param start: The start time to calculate from
    param end: The end time to calculate to

    :Example:
    >> from time_fncs import find_hours_worked
    >> from datetime import datetime, timedelta
    >> start_time = datetime.now() - timdelta(days=1)
    >> end_time = datetime.now()
    >> hours = hours_worked(start_time, end_time)
    """
    # Assume start of day is 8:00
    # Assume end of day is 17:00
    if start == None and end == None:
        # Error????
        return 0.0
    elif start == None:
        num_hours = end.hour-START_OF_DAY_HOUR
        start = end.replace(hour=end.hour-num_hours)
    elif end == None:
         num_hours = END_OF_DAY_HOUR - start.hour
         end = start.replace(hour=start.hour+num_hours)
    start, end = _match_tz(start,end)

    if (end-start).seconds/(60*60) < 10:
        # If a days work just save the time worked
        #print('difference is ' + str((end-start).seconds/(60*60)) + ' hours')
        return (end-start).seconds / (60.0*60)
    else:
        # Handle two partial days, first day and last day
        tmp_day = datetime(start.year, start.month, start.day, END_OF_DAY_HOUR, 0, 0)
        tmp_day = tmp_day.astimezone(start.tz)
        start_day_hours = 0
        if tmp_day > start:
            start_day_hours = (tmp_day-start).seconds / (60.0*60.0)
        #print('Partial first day', start_day_hours)

        tmp_day = datetime(end.year, end.month, end.day, START_OF_DAY_HOUR, 0, 0)
        tmp_day.replace(hour=8, minute=0, second=0)
        tmp_day = tmp_day.astimezone(start.tz)
        end_day_hours = 0
        if tmp_day < end:
            end_day_hours = (end-tmp_day).seconds / (60.0*60.0)
        #print('Partial End Day', end_day_hours)

        # Now loop through all the in between days and assume 8 hours
        start += timedelta(hours=24)
        full_days = 0
        while start < end:
            #print('Start', start)
            #print('End', end)
            if start.weekday() <= 4:
                # If mon-Fri then add 8
                #total_hours += 8
                full_days += 1
            else:
                print('skipping', start.strftime( '%A, %B %d, %Y %I:%M:%S %p'), start.weekday())
            start += timedelta(hours=24)

        #print('Num full days', full_days)
        #print('Total Hours ',start_day_hours + end_day_hours + (8*full_days))
        return start_day_hours + end_day_hours + (8*full_days)



if __name__ == '__main__':
    # Test #1
    start_time = datetime.strptime('2020-04-16 13:26:00', '%Y-%m-%d %H:%M:%S')
    end_time = datetime.strptime('2020-05-01 10:49:44', '%Y-%m-%d %H:%M:%S')
    print('Test 1 Hours Worked', hours_worked(start_time, end_time))
    print(' ')

    # Test #2
    start_time = datetime.strptime('2019-03-28 17:11:04', '%Y-%m-%d %H:%M:%S')
    end_time = datetime.strptime('2019-03-29 13:09:53', '%Y-%m-%d %H:%M:%S')
    print('Test 1 Hours Worked', hours_worked(start_time, end_time))
    print(' ')
