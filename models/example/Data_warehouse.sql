
{{ config(
    materialized="view"
) }}

with customer_orders as ...
from {{ ref('Database') }}
