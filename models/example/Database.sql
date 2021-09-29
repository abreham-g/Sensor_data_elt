create view dbt_alice.Database as (
 


    from jaffle.Database

    left join Data_base using (Database_id)

)