## NASDAQ TOTALVIEW ITCH 5.0 Parser and VWAP calculator
This project aims to parse the nasdaq totalview itch 5.0 feed and calculate a running volume weighted average price for each security at every hour including market close.

### <u>**How it works**</u>
Use below command to run the python file.
``` 
python nasdaq_vwap.py
```

If the itch file is not present in the itchdir directory, it will create a new directory and download the zip file from the source url.
Then unzip the file and convert it into a binary file.
The parser will parse different message types and output the vwap for each security at every hour including market close.
It also keeps a counter on number of messages that have been parsed so far.

### <u>**Output**</u>
The output is present in vwap.txt which contains vwap for each security at every hour. The format for storing is a python dictionary where the key is the stock name and value is a tuple with two elements, the hour and the vwap value.

