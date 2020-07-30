source ~/business-algo/global/bin/global.sh

sql1="
select q[0] as q25, q[1] as q50, q[2] as q75, q[3] as q90, std, addf_num, num
from
(
select percentile_approx(addf_1, array(0.25,0.5,0.75,0.9)) as q, stddev_pop(addf_1) as std, addf_num, count(1) as num
from arc_eight_dev.score_train where ds <= '2020-07-26' and ds >= '2020-07-14'
group by addf_num) a
order by addf_num limit 20;"
echo ${sql1}
$HIVE -e"$sql1">./feature_ana/addf_1.csv

sql8="
select q[0] as q25, q[1] as q50, q[2] as q75, q[3] as q90, std, addf_num, num
from
(
select percentile_approx(addf_8, array(0.25,0.5,0.75,0.9)) as q, stddev_pop(addf_8) as std, addf_num, count(1) as num
from arc_eight_dev.score_train where ds <= '2020-07-26' and ds >= '2020-07-14'
group by addf_num) a
order by addf_num limit 20;"
echo ${sql8}
$HIVE -e"$sql8">./feature_ana/addf_8.csv

sql15="
select q[0] as q25, q[1] as q50, q[2] as q75, q[3] as q90, std, addf_num, num
from
(
select percentile_approx(addf_15, array(0.25,0.5,0.75,0.9)) as q, stddev_pop(addf_15) as std, addf_num, count(1) as num
from arc_eight_dev.score_train where ds <= '2020-07-26' and ds >= '2020-07-14'
group by addf_num) a
order by addf_num limit 20;"
echo ${sql15}
$HIVE -e"$sql15">./feature_ana/addf_15.csv

sqlavg="
select q[0] as q25, q[1] as q50, q[2] as q75, q[3] as q90, std, addf_num, num
from
(
select percentile_approx(avg_addf_score, array(0.25,0.5,0.75,0.9)) as q, stddev_pop(avg_addf_score) as std, addf_num, count(1) as num
from arc_eight_dev.score_train where ds <= '2020-07-26' and ds >= '2020-07-14'
group by addf_num) a
order by addf_num limit 20;"
echo ${sqlavg}
$HIVE -e"$sqlavg">./feature_ana/avg.csv
