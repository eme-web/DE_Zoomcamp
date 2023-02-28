{{ config(materialized='view') }}



select * from {{ source('staging','fhv_table') }}
limit 100