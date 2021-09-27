
{{ config(
    materialized="view",
    schema="marketing"
) }}

with customer_orders as ...
from {{ ref('Database') }}
