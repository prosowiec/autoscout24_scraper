Program scrapes data from otomoto offers and save it into csv file. It uses BeautifulSoup, urlopen and regex to do so.

1 row contains those headers -  'brand','model', 'production_date','price','currency', 'mileage','mileage_unit', 'engine_power','engine_power_unit', 'fuel_type', 'batch_date'

Scraper to work needs link which contains brand and model of given car - https://www.autoscout24.com/lst/bmw/1-series-(all) .

Those are provided in links.py whitch I did append manually.

links.csv -> scraper.py -> autoscout24.csv

After running the script, data need to be further validetad, usualy becouse of duplicates, that been showed due to paid promotion. Colums can have also empty values, or be incorrect(date in brand colums ect.), all of those rows must be deleted, I use excel sheet to do so.

Power from electric cars offers is may be incorect, but rest of data is so I left it as it is, but I acknowledge this problem and in later version i shoud fix it
