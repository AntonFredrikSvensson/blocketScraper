import BlocketDateTime

def test_blocket_datetime_to_datetime(list_of_days):
    converted_days = []
    for day in list_of_days:
        converted_day = BlocketDateTime.blocket_datetime_to_datetime(day)
        converted_days.append(converted_day)
    
    for conv_day in converted_days:
        print(conv_day)


exempel_dagar = ["Idag 21:46", "Igår 08:25", 
"I söndags 14:32", "I fredags 15:12", "I tisdags 8:22","I torsdags 11:11", 
"10 apr. 18:42"]

# test_blocket_datetime_to_datetime(exempel_dagar)