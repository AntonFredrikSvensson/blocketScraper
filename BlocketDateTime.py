import datetime
import logging
import os

# setting logging config: time, logginglevel, message
logging.basicConfig(filename=os.environ.get('BLOCKET_SCRAPER_LOG_PATH') + 'blocket_datetime.log', 
                    level=logging.DEBUG,
                    format='%(asctime)s:%(levelname)s:%(message)s',
                    force=True)

blocket_weekdays = {
    "måndags":0,
    "tisdags":1,
    "onsdags":2,
    "torsdags":3,
    "fredags":4,
    "lördags":5,
    "söndags":6,
}

blocket_months = {
    "jan.":1,
    "feb.":2,
    "mars":3,
    "apr.":4,
    "maj":5,
    "juni":6,
    "juli":7,
    "aug.":8,
    "sep.":9,
    "okt":10,
    "nov.":11,
    "dec":12,
}

def blocket_datetime_to_datetime(blocket_date):
    # Desc: coverts date and time given at blocket search page to a datetime object
    # @Parms blocket_date:string
    # exempel_dagar = ["Idag 21:46", "Igår 08:25", 
    # "I söndags 14:32", "I fredags 15:12", "I tisdags 8:22","I torsdags 11:11", 
    # "10 apr. 18:42"]
    # @output converted_date:datetime object

    logging.debug('---Start of blocket_datetime_to_datetime()---')
    parsed_date = blocket_date.split(" ")
    #today and yesterday
    if parsed_date[0] == "Idag":
        converted_date = datetime.datetime.now().date()
    elif parsed_date[0] == "Igår":
        converted_date = datetime.datetime.now() - datetime.timedelta(1)
        converted_date = converted_date.date()
    else:
        #last week - example "I fredags 15:12"
        try:
            article_weekday_number = blocket_weekdays[parsed_date[1]]
            today_weekday_number = datetime.datetime.now().weekday()
            if article_weekday_number < today_weekday_number:
                datediff = today_weekday_number - article_weekday_number
            else:
                datediff = today_weekday_number + (7 - article_weekday_number)
            converted_date = datetime.datetime.now() - datetime.timedelta(datediff)
            converted_date = converted_date.date()
        #earlier - example "10 apr. 18:42"]
        except:
            try:
                article_month = blocket_months[parsed_date[1]]
                article_day = int(parsed_date[0])
                current_month = datetime.datetime.now().month
                current_year = datetime.datetime.now().year
                if current_month > article_month:
                    article_year = current_year
                else:
                    article_year = current_year - 1
                converted_date = datetime.datetime(article_year,article_month,article_day).date()
            except:
                #date could not be set
                logging.warning('Date could not be set. Blocket date: {}'.format(blocket_date))
                return None
    #fetching time
    parsed_length = len(parsed_date)
    hours_and_minutes = parsed_date[parsed_length-1].split(":")
    #creating datetime object
    converted_date = datetime.datetime(converted_date.year,converted_date.month, converted_date.day, int(hours_and_minutes[0]), int(hours_and_minutes[1]))
    logging.debug('---End of blocket_datetime_to_datetime()---')
    return converted_date
