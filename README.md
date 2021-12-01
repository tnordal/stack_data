# Stock Data
Download tickers and store data in a database

## Running from a Docker container
Run temporary docker container:
docker run -i --rm -e DATABASE_URI=postgres://user:password@ipaddress/stock_data tnordal/stock_data:latest

Get DATABASE_URI from .env file
docker run -i --rm --env-file .env tnordal/stock_data:latest
