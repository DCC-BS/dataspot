# README.md

## How to do regular updates (not yet implemented):
<!-- Frequent updates of details of already published datasets (e.g. last_updated field) are not updated directly through dataspot. but instead through a file managed by the Data Competence Center DCC. This means that fields that should be updated outside of the workflow are written to the centrally managed file instead of dataspot directly. These changes are then regularly updated by a script from the DCC to dataspot. The key should always be the dataspot-internal UUID. Dates should be provided as Unix timestamps in in UTC timezone. Times should be provided in Unix timestamps aswell in a ??? format (TBD; the same as is used internally in dataspot.). TODO: Add examples -->

Frequent updates of details of already published datasets (e.g. last_updated field) are not updated directly through dataspot, as this does not work with the workflow. Instead, the changes are pushed to a non-public dataset on [opendatasoft](data.bs.ch). Please [get in touch](mailto:opendata@bs.ch) with us for the setup.

The columns should be (so far): uuid,lastactl,lastpub

This is put on hold for the moment, as lastactl does not really need to be in dataspot.

## Notes (Renato)
### Colors for top bar background:
Not explicitely defined, this is written here just so that I know what I used before. You can use whatever you want.
- dev: 167a3e
- int: ff0000
