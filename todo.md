# DESCRIPTIONs AND TODOs
---

## DESCRIPTION
Download (ticker)data from Yahoo and store values in a Database.

### Database
Need two tables. 
One for storing tickers with company names, ticker and exchange.
One for storing data from the stock exchange.

### Download
Downloading from Yahoo.
Filter bars by first and last date from bars table
Bulk insert filtered bars

## TODO
### Database
* Make classes (models)
    - [ ] Company class
    - [ ] Bar class
* Make database module
    - [x] Sql to make company table
    - [x] Sql to make bar table
    - [ ] Sql for company class
    - [ ] Sql for bar class
* Make a new companies table
* Make a sector table
* Make a industri table
* Make a exchange table

### Download
* Download ticker
    - [x] Download one ticker
    - [x] Add ticker bars to DB
* Add companies to db
    - [x] Prepare csv files
    - [x] Bulk insert to db
    - [ ] Add a single companie

### User inputs
* Commandline menu
    - [ ] Make menu
    - [x] Update one ticker
    - [x] Make function for update bars
    - [x] Filter witch tickers to update
    - [ ] Make function for insert a new companies file
    - [x] Add a single companie manuel
* Web menu