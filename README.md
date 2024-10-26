# Exploring Italian government financials with AI

This repo contains all the scripts that I used to gather data I used for the talk "Exploring Italian government financials with AI".

### `requirements.txt`

Requirements, duh. Only thing not written there is the required Python version, I used `3.12`, I'd expect this to work with `3.13` too though.

### `async_ckan.py`

A small async re-implementation of the `RemoteCKAN` class from the `ckanapi` library.

### `fetch_data.py`

Script that downloads CSVs from BDAP, all files will be stored in the `dataset` folder.

This will try to download ALL CSVs from BDAP, that's around 50 Gb. Be prepared.

You might need to run this multiple times, some times it might fail for stupid reasons. I didn't feel like trying to handle all of them.

This will also download some metadata files.

### `fix_chars.py`

Some files will contain some corrupted UTF chars, this scripts finds and removes all of them.

### `docker-compose.yml`

Definitions of the local Postgres DB and PGAdmin interface, just run `docker compose up -d` to start it up.

You're going to need this to save the downloaded CSVs in the DB.

I don't remember whether you need to manually create the actual DB and admin user before saving stuff or not. You're smart, you can figure it out.

Just remember to use the same credentials saved in `save_to_db.py`.

### `save_to_db.py`

Saves all downloaded CSVs in Postgres.

The script also cleans the data a bit so that it's actually possible to save it.

Some stuff can still fail. 🤷
