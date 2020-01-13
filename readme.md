# Danbooru dump meta importer

This script is to import the danbooru metadata from the Danbooru 2019 dataset from gwern https://www.gwern.net/Danbooru2019

## Background
The metadata files from the danbooru dump are list of very large files, each line including a full json representation of the metadata for an image. For machine learning purposes, in order to get better interface for getting batch and metadata, we want to store this in a database format so it can be easily queried. Using sqlite will provide us portability so it can be shared easily.

## Usage
```
python main.py /folder/including/metafiles database.db
```

## Import time
On my machine, it takes 1 hour 56 minutes to finish the import, building index takes another hour.

## Size
The unzipped raw metadata is 21GB on disk, after importing to sqlite, the database is 3.1GB, with index 14.4GB
