# data-tools
simple tools for in memory profiling and etl of small sql databases

`snowflake_utils` contains functions for moving between pandas df and snowflake tables
`gcs_utils` has one way functions for moving from google cloud storage to dataframes

`profile_db` has functions and a script for creating a data dictionary, summary statistics and erd for a sql DB (only tested on mysql so far). Also does some basic cleaning

`google_play_to_sf` is a script for pulling data from dev console of google play store and moving to SF
