import datetime

    
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
    #TODO comment code
    #TODO break out code into functions
    parsed_date = blocket_date.split(" ")
    if parsed_date[0] == "Idag":
        converted_date = datetime.datetime.now().date()
    elif parsed_date[0] == "Igår":
        converted_date = datetime.datetime.now() - datetime.timedelta(1)
        converted_date = converted_date.date()
    else:
        try:
            article_weekday_number = blocket_weekdays[parsed_date[1]]
            today_weekday_number = datetime.datetime.now().weekday()
            if article_weekday_number < today_weekday_number:
                datediff = today_weekday_number - article_weekday_number
            else:
                datediff = today_weekday_number + (7 - article_weekday_number)
            converted_date = datetime.datetime.now() - datetime.timedelta(datediff)
            converted_date = converted_date.date()
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
                #TODO add errorlog
                return None
    parsed_length = len(parsed_date)
    hours_and_minutes = parsed_date[parsed_length-1].split(":")
    converted_date = datetime.datetime(converted_date.year,converted_date.month, converted_date.day, int(hours_and_minutes[0]), int(hours_and_minutes[1]))
    return converted_date


# #Test
# exempel_dagar = ["Idag 21:46", "Igår 08:25", 
# "I söndags 14:32", "I fredags 15:12", "I tisdags 8:22","I torsdags 11:11", 
# "10 apr. 18:42"]

# blocket_date = exempel_dagar[6]
# blocket_datetime_to_datetime(blocket_date)
