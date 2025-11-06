use default;
ADD JAR /opt/hive/lib/opencsv-5.9.jar;
ADD JAR /opt/hive/lib/hive-hcatalog-core-4.1.0.jar;

-- !sh hadoop fs -mkdir -p /tmp/output_mr_clean
-- !sh hadoop fs -cat ${hivevar:mapreduce_input}/* | sed 's/,\t/,/g' | hadoop fs -put - /tmp/output_mr_clean/part-00000
-- !sh hadoop fs -rm -r -f ${hivevar:mapreduce_input}
-- !sh hadoop fs -mv /tmp/output_mr_clean ${hivevar:mapreduce_input}

-- !sh mkdir /tmp/source
-- !sh mkdir /tmp/source/rentals
-- !sh cp merged_output.csv /tmp/source/rentals/
-- !sh hadoop fs -mkdir -p /tmp/source/rentals
-- !sh hadoop fs -put /tmp/source/rentals/merged_output.csv /tmp/source/rentals/
-- !sh mkdir /tmp/source/cars
-- !sh cp cars.csv /tmp/source/cars/
-- !sh hadoop fs -mkdir -p /tmp/source/cars
-- !sh hadoop fs -put /tmp/source/cars/cars.csv /tmp/source/cars/

drop table if exists rentals_raw;
create external table if not exists rentals_raw (
    car_id string,
    year int,
    total_rentals int,
    completed_ratio float
)
row format delimited
fields terminated by ','
stored as textfile
location '${hivevar:mapreduce_input}';

drop table if exists rentals;
create table if not exists rentals (
    car_id string,
    year int,
    total_rentals int,
    completed_ratio float
)
clustered by (car_id) into 20 buckets
stored as orc;

insert overwrite table rentals
select car_id, year, sum(total_rentals), sum(completed_ratio * total_rentals) / sum(total_rentals) 
from rentals_raw 
group by car_id, year;

drop table if exists cars_raw;
create external table if not exists cars_raw (
    car_id string,
    make string,
    model string,
    year int,
    features string,
    category string
)
row format serde 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
with serdeproperties (
    "separatorChar" = ",",
    "quoteChar"     = "\"" --"
)
location '${hivevar:hive_input}'
tblproperties ("skip.header.line.count"="1");

drop table if exists cars;
create table if not exists cars (
    car_id string,
    make string,
    model string,
    year int,
    features string,
    category string
)
clustered by (car_id) into 20 buckets
stored as orc;


insert overwrite table cars
select car_id, make, model, year, features, category
from cars_raw
where car_id not like 'car_id';

drop table if exists feature_year_stats;
create table if not exists feature_year_stats (
    feature string,
    year int,
    feature_year_avg_rentals float,
    feature_year_avg_completed_ratio float,
    above_avg_rentals boolean
);

with cars_exploded as (
    select 
        c.car_id,
        f.feature
    from cars c
    lateral view explode(split(c.features, ';')) f as feature
),
feature_global_avg AS (
    select
        ce.feature,
        AVG(r.total_rentals) AS global_avg_rentals
    from cars_exploded ce
    join rentals r
        on ce.car_id = r.car_id
    group by ce.feature
)
insert overwrite table feature_year_stats
select
    ce.feature,
    r.year,
    avg(r.total_rentals) as feature_year_avg_rentals,
    avg(r.completed_ratio) as feature_year_avg_completed_ratio,
    case
        when avg(r.total_rentals) > fga.global_avg_rentals then true
        else false
    end as above_avg_rentals
from cars_exploded ce
join rentals r
    on ce.car_id = r.car_id
join feature_global_avg fga
    on ce.feature = fga.feature
group by ce.feature, r.year, fga.global_avg_rentals;

drop table if exists feature_year_stats_json;
create external table if not exists feature_year_stats_json (
  feature string,
  year int,
  feature_year_avg_rentals float,
  feature_year_avg_completed_ratio float,
  above_avg_rentals boolean
)
row format serde 'org.apache.hive.hcatalog.data.JsonSerDe'
stored as textfile
location '${hivevar:hive_output}';

insert overwrite table feature_year_stats_json
select
  feature,
  year,
  feature_year_avg_rentals,
  feature_year_avg_completed_ratio,
  above_avg_rentals
from feature_year_stats;