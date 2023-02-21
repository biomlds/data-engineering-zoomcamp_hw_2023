


select 
    locationid, 
    borough, 
    zone, 
    replace(service_zone,'Boro','Green') as service_zone
from `hip-watch-375918`.`dez_ny_taxi`.`taxi_zone_lookup`