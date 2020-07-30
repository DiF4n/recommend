#!/bin/bash

#  -------------------------------------------------------------------------------
# Revision:    1.0
# Date:        2020-07-14
# Author:      fandi
# Email:       fandi@kanzhun.com
# Website:     www.kanzhun.com.cn
# jobType:     日
# executuser:  HIVE
# Description: sample表取样本,train表扩字段
#  -------------------------------------------------------------------------------

BASE_DIR_LOCAL="`readlink -f $0 | xargs dirname | xargs dirname| xargs dirname| xargs dirname`"
echo $BASE_DIR_LOCAL
echo $BASE_DIR_LOCAL/global/bin/global.sh
source ~/business-algo/global/bin/global.sh

#定义job类型名称
JOB_TYPE="fetch_bossrec_score_train_ctr_daily"
RUN_TAG=0
main(){
    #######main方法入口，获取信息配置,主要获取时间$date等
    today=$1
    date=$1
    isrun $JOB_TYPE ${date}
    if [ ${RUN_TAG} -eq 1 ];then
        echo "${date}------------${JOB_TYPE}--------------已经执行过了"
    fi
    echo "开始执行程序。===============时间:${date}=========Job:${JOB_TYPE}=============="
    ystd=`date -d "$1 -1days" +%Y-%m-%d`
     #######调用业务方法完成具体逻辑

    fetch_bossrec_score_train_ctr $ystd

    echo "执行结束，将执行成功的状态写入文件"
    echo ${date}====${JOB_TYPE}====success >> ${LOCAL_JOB_STATUS_FILE_PATH}/${date}
}

fetch_bossrec_score_train_ctr(){
    thres=300
    thres_low=150
    ystd=$1
    two_day=`date -d "$1 -1days" +%Y-%m-%d`
    three_day=`date -d "$1 -2days" +%Y-%m-%d`
    four_day=`date -d "$1 -3days" +%Y-%m-%d`
    ystd_format=`date -d "$1" +%Y%m%d`

    info_sql="
set
    hive.exec.dynamic.partition.mode = nonstrict;

INSERT
    overwrite table arc_eight_dev.score_sample_daily_ctrinfo partition(ds = '$ystd')
SELECT
    a.boss_id,
    a.job_id,
    a.geek_id,
    a.exp_id,
    a.sessionid,
    a.page,
    a.rank,
    list_time,
    boss_position,
    boss_city,
    ctcvr_score,
    ctr_score,
    b.deal_type
from
    (
        select
            distinct *
        from
            arc_eight_dev.xuhy_rank_scores_daily
        where
            ds = '$ystd'
    ) a
    join (
        select
            boss_id,
            job_id,
            geek_id,
            exp_id,
            sessionid,
            deal_type,
            boss_position,
            boss_city,
            list_time
        from
            arc_eight_dev.bossrec_train_daily
        where
            ds = '$ystd'
    ) b on a.boss_id = b.boss_id
    and a.job_id = b.job_id
    and a.geek_id = b.geek_id
    and a.exp_id = b.exp_id
    and a.sessionid = b.sessionid;
"

feature_pct_sql="
drop table if exists arc_eight_dev.score_feature_pct1_cp_ratio;
create table arc_eight_dev.score_feature_pct1_cp_ratio as
select
    sum(
        if(
            deal_type in ('addf', 'success', 'p_success'),
            1,
            0
        )
    ) / count(1) as addf_ratio,
    cp,
    count(1) as num
from
    (
        select
            concat_ws('-', boss_city, boss_position) as cp,
            deal_type,
            ctr_score
        from
            arc_eight_dev.score_sample_daily_ctrinfo
        where
            ds in ('$four_day', '$two_day', '$three_day')
            and ctr_score < 0.1
    ) a
group by
    cp
having
    num > $thres;


drop table if exists arc_eight_dev.score_feature_pct2_cp_ratio;
create table arc_eight_dev.score_feature_pct2_cp_ratio as
select
    sum(
        if(
            deal_type in ('addf', 'success', 'p_success'),
            1,
            0
        )
    ) / count(1) as addf_ratio,
    cp,
    count(1) as num
from
    (
        select
            concat_ws('-', boss_city, boss_position) as cp,
            deal_type,
            ctr_score
        from
            arc_eight_dev.score_sample_daily_ctrinfo
        where
            ds in ('$four_day', '$two_day', '$three_day')
            and ctr_score < 0.2
            and ctr_score >= 0.1
    ) a
group by
    cp
having
    num > $thres;


drop table if exists arc_eight_dev.score_feature_pct3_cp_ratio;
create table arc_eight_dev.score_feature_pct3_cp_ratio as
select
    sum(
        if(
            deal_type in ('addf', 'success', 'p_success'),
            1,
            0
        )
    ) / count(1) as addf_ratio,
    cp,
    count(1) as num
from
    (
        select
            concat_ws('-', boss_city, boss_position) as cp,
            deal_type,
            ctr_score
        from
            arc_eight_dev.score_sample_daily_ctrinfo
        where
            ds in ('$four_day', '$two_day', '$three_day')
            and ctr_score < 0.3
            and ctr_score >= 0.2
    ) a
group by
    cp
having
    num > $thres;


drop table if exists arc_eight_dev.score_feature_pct4_cp_ratio;
create table arc_eight_dev.score_feature_pct4_cp_ratio as
select
    sum(
        if(
            deal_type in ('addf', 'success', 'p_success'),
            1,
            0
        )
    ) / count(1) as addf_ratio,
    cp,
    count(1) as num
from
    (
        select
            concat_ws('-', boss_city, boss_position) as cp,
            deal_type,
            ctr_score
        from
            arc_eight_dev.score_sample_daily_ctrinfo
        where
            ds in ('$four_day', '$two_day', '$three_day')
            and ctr_score < 0.4
            and ctr_score >= 0.3
    ) a
group by
    cp
having
    num > $thres_low;


drop table if exists arc_eight_dev.score_feature_pct5_cp_ratio;
create table arc_eight_dev.score_feature_pct5_cp_ratio as
select
    sum(
        if(
            deal_type in ('addf', 'success', 'p_success'),
            1,
            0
        )
    ) / count(1) as addf_ratio,
    cp,
    count(1) as num
from
    (
        select
            concat_ws('-', boss_city, boss_position) as cp,
            deal_type,
            ctr_score
        from
            arc_eight_dev.score_sample_daily_ctrinfo
        where
            ds in ('$four_day', '$two_day', '$three_day')
            and ctr_score < 0.5
            and ctr_score >= 0.4
    ) a
group by
    cp
having
    num > $thres_low;


drop table if exists arc_eight_dev.score_feature_pct6_cp_ratio;
create table arc_eight_dev.score_feature_pct6_cp_ratio as
select
    sum(
        if(
            deal_type in ('addf', 'success', 'p_success'),
            1,
            0
        )
    ) / count(1) as addf_ratio,
    cp,
    count(1) as num
from
    (
        select
            concat_ws('-', boss_city, boss_position) as cp,
            deal_type,
            ctr_score
        from
            arc_eight_dev.score_sample_daily_ctrinfo
        where
            ds in ('$four_day', '$two_day', '$three_day')
            and ctr_score < 0.6
            and ctr_score >= 0.5
    ) a
group by
    cp
having
    num >$thres_low;



drop table if exists arc_eight_dev.score_feature_pct7_cp_ratio;
create table arc_eight_dev.score_feature_pct7_cp_ratio as
select
    sum(
        if(
            deal_type in ('addf', 'success', 'p_success'),
            1,
            0
        )
    ) / count(1) as addf_ratio,
    cp,
    count(1) as num
from
    (
        select
            concat_ws('-', boss_city, boss_position) as cp,
            deal_type,
            ctr_score
        from
            arc_eight_dev.score_sample_daily_ctrinfo
        where
            ds in ('$four_day', '$two_day', '$three_day')
            and ctr_score < 0.7
            and ctr_score >= 0.6
    ) a
group by
    cp
having
    num > $thres_low;


drop table if exists arc_eight_dev.score_feature_pct8_cp_ratio; 
create table arc_eight_dev.score_feature_pct8_cp_ratio as
select
    sum(
        if(
            deal_type in ('addf', 'success', 'p_success'),
            1,
            0
        )
    ) / count(1) as addf_ratio,
    cp,
    count(1) as num
from
    (
        select
            concat_ws('-', boss_city, boss_position) as cp,
            deal_type,
            ctr_score
        from
            arc_eight_dev.score_sample_daily_ctrinfo
        where
            ds in ('$four_day', '$two_day', '$three_day')
            and ctr_score >= 0.7
    ) a
group by
    cp
having
    num > $thres_low;
"

feature_job_sql="
drop table if exists arc_eight_dev.generate_score_feature_temp_job_${ystd_format};
create table arc_eight_dev.generate_score_feature_temp_job_${ystd_format} as
select
    job_id,
    percentile_approx(ctr_score, 0.25) as addf_job_score_q1_3days,
    percentile_approx(ctr_score, 0.5) as addf_job_score_q2_3days,
    percentile_approx(ctr_score, 0.75) as addf_job_score_q3_3days,
    count(1) as num
from
    (
        select
            job_id,
            ctr_score
        from
            arc_eight_dev.score_sample_daily_ctrinfo
        where
            ds in ('$four_day', '$two_day', '$three_day')
            and deal_type in ('addf', 'success')
    ) a
group by
    job_id;"


feature_boss_sql="
drop table if exists arc_eight_dev.generate_score_feature_temp_boss_${ystd_format};
create table arc_eight_dev.generate_score_feature_temp_boss_${ystd_format} as
select
    boss_id,
    percentile_approx(ctr_score, 0.25) as addf_boss_score_q1_3days,
    percentile_approx(ctr_score, 0.5) as addf_boss_score_q2_3days,
    percentile_approx(ctr_score, 0.75) as addf_boss_score_q3_3days,
    count(1) as num
from
    (
        select
            boss_id,
            ctr_score
        from
            arc_eight_dev.score_sample_daily_ctrinfo
        where
            ds in ('$four_day', '$two_day', '$three_day')
            and deal_type in ('addf', 'success')
    ) a
group by
    boss_id;"

feature_cp_sql="
drop table if exists arc_eight_dev.generate_score_feature_temp_cp_${ystd_format};
create table arc_eight_dev.generate_score_feature_temp_cp_${ystd_format} as
select
    cp,
    percentile_approx(ctr_score, 0.25) as addf_cp_score_q1_3days,
    percentile_approx(ctr_score, 0.5) as addf_cp_score_q2_3days,
    percentile_approx(ctr_score, 0.75) as addf_cp_score_q3_3days,
    count(1) as num
from
    (
        select
            concat_ws('-', boss_city, boss_position) as cp,
            ctr_score
        from
            arc_eight_dev.score_sample_daily_ctrinfo
        where
            ds in ('$four_day', '$two_day', '$three_day')
            and deal_type in ('addf', 'success')
    ) a
group by
    cp;"

    sample_sql="
set
    hive.exec.dynamic.partition.mode = nonstrict;

INSERT
    overwrite table arc_eight_dev.score_sample_daily_ctr partition(ds = '$ystd')
SELECT
    boss_id,
    job_id,
    page,
    sessionid,
    list_hour,
    date,
    weekday,
    city,
    pos,
    cp,
    pct1,
    pct2,
    pct3,
    pct4,
    pct5,
    pct6,
    pct7,
    pct8,
    addf_num,
    suc_num,
    num,
    avg_addf_score,
    avg_suc_score,
    addf_score_list [0] as addf_1,
    addf_score_list [1] as addf_2,
    addf_score_list [2] as addf_3,
    addf_score_list [3] as addf_4,
    addf_score_list [4] as addf_5,
    addf_score_list [5] as addf_6,
    addf_score_list [6] as addf_7,
    addf_score_list [7] as addf_8,
    addf_score_list [8] as addf_9,
    addf_score_list [9] as addf_10,
    addf_score_list [10] as addf_11,
    addf_score_list [11] as addf_12,
    addf_score_list [12] as addf_13,
    addf_score_list [13] as addf_14,
    addf_score_list [14] as addf_15,
    suc_score_list [0] as suc_1,
    suc_score_list [1] as suc_2,
    suc_score_list [2] as suc_3,
    suc_score_list [3] as suc_4,
    suc_score_list [4] as suc_5,
    suc_score_list [5] as suc_6,
    suc_score_list [6] as suc_7,
    suc_score_list [7] as suc_8,
    suc_score_list [8] as suc_9,
    suc_score_list [9] as suc_10,
    suc_score_list [10] as suc_11,
    suc_score_list [11] as suc_12,
    suc_score_list [12] as suc_13,
    suc_score_list [13] as suc_14,
    suc_score_list [14] as suc_15
from
    (
        select
            boss_id,
            job_id,
            sessionid,
            page,
            list_hour,
            date,
            weekday,
            city,
            pos,
            concat_ws('-', city, pos) as cp,
            sum(addf_deal_type) as addf_num,
            sum(suc_deal_type) as suc_num,
            sort_array(collect_list(addf_score)) as addf_score_list,
            sort_array(collect_list(suc_score)) as suc_score_list,
            avg(addf_score) as avg_addf_score,
            avg(suc_score) as avg_suc_score,
            sum(if(addf_score < 0.1, 1, 0)) as pct1,
            sum(if(addf_score >= 0.1 and addf_score < 0.2, 1, 0)) as pct2,
            sum(if(addf_score >= 0.2 and addf_score < 0.3, 1, 0)) as pct3,
            sum(if(addf_score >= 0.3 and addf_score < 0.4, 1, 0)) as pct4,
            sum(if(addf_score >= 0.4 and addf_score < 0.5, 1, 0)) as pct5,
            sum(if(addf_score >= 0.5 and addf_score < 0.6, 1, 0)) as pct6,
            sum(if(addf_score >= 0.6 and addf_score < 0.7, 1, 0)) as pct7,
            sum(if(addf_score >= 0.7, 1, 0)) as pct8,
            count(1) as num
        from
            (
                select
                    boss_id,
                    job_id,
                    sessionid,
                    page,
                    ctr_score as addf_score,
                    ctcvr_score as suc_score,
                    to_date(list_time) as date,
                    hour(list_time) as list_hour,
                    pmod(datediff(list_time, '1920-01-01') - 3, 7) as weekday,
                    case
                        when deal_type in ('addf', 'success', 'p_success') then 1
                        else 0
                    end as addf_deal_type,
                    CASE
                        WHEN deal_type in ('success', 'p_success') then 1
                        else 0
                    end as suc_deal_type,
                    boss_city as city,
                    boss_position as pos
                from
                    arc_eight_dev.score_sample_daily_ctrinfo
                where
                    ctr_score >= 0
                    and ds = '$ystd'
            ) a
        group by
            boss_id,
            job_id,
            sessionid,
            page,
            list_hour,
            date,
            weekday,
            city,
            pos
        having
            num = 15
    ) b;"
    
    train_sql="INSERT
    overwrite table arc_eight_dev.bossrec_train_score_ctr_test partition(ds = '$ystd')
SELECT
    a.boss_id,
    a.job_id,
    a.sessionid,
    a.page,
    list_hour,
    a.date,
    weekday,
    city,
    pos,
    a.cp,
    a.num,
    pct1,
    pct2,
    pct3,
    pct4,
    pct5,
    pct6,
    pct7,
    pct8,
    f_pct1.addf_ratio as pct1_ratio,
    f_pct2.addf_ratio as pct2_ratio,
    f_pct3.addf_ratio as pct3_ratio,
    f_pct4.addf_ratio as pct4_ratio,
    f_pct5.addf_ratio as pct5_ratio,
    f_pct6.addf_ratio as pct6_ratio,
    f_pct7.addf_ratio as pct7_ratio,
    f_pct8.addf_ratio as pct8_ratio,
    addf_num,
    suc_num,
    round(avg_addf_score, 3) as avg_addf_score,
    round(avg_suc_score, 3) as avg_suc_score,
    suc_1,
    suc_2,
    suc_3,
    suc_4,
    suc_5,
    suc_6,
    suc_7,
    suc_8,
    suc_9,
    suc_10,
    suc_11,
    suc_12,
    suc_13,
    suc_14,
    suc_15,
    addf_1,
    addf_2,
    addf_3,
    addf_4,
    addf_5,
    addf_6,
    addf_7,
    addf_8,
    addf_9,
    addf_10,
    addf_11,
    addf_12,
    addf_13,
    addf_14,
    addf_15,
    j.addf_job_score_q1_3days,
    j.addf_job_score_q2_3days,
    j.addf_job_score_q3_3days,
    bos.addf_boss_score_q1_3days,
    bos.addf_boss_score_q2_3days,
    bos.addf_boss_score_q3_3days,
    cipo.addf_cp_score_q1_3days,
    cipo.addf_cp_score_q2_3days,
    cipo.addf_cp_score_q3_3days,
    nvl(j.addf_job_score_q1_3days, bos.addf_boss_score_q1_3days) / cipo.addf_cp_score_q1_3days - 1 as q1_strict,
    nvl(j.addf_job_score_q2_3days, bos.addf_boss_score_q2_3days) / cipo.addf_cp_score_q2_3days - 1 as q2_strict,
    nvl(j.addf_job_score_q3_3days, bos.addf_boss_score_q3_3days) / cipo.addf_cp_score_q3_3days - 1 as q3_strict,

case
when nvl(addf_job_score_q1_3days, nvl(addf_boss_score_q1_3days, addf_cp_score_q1_3days))  is null  then 0
when addf_15 < nvl(addf_job_score_q1_3days, nvl(addf_boss_score_q1_3days, addf_cp_score_q1_3days)) then 0
when addf_14 < nvl(addf_job_score_q1_3days, nvl(addf_boss_score_q1_3days, addf_cp_score_q1_3days)) then 1
when addf_13 < nvl(addf_job_score_q1_3days, nvl(addf_boss_score_q1_3days, addf_cp_score_q1_3days)) then 2
when addf_12 < nvl(addf_job_score_q1_3days, nvl(addf_boss_score_q1_3days, addf_cp_score_q1_3days)) then 3
when addf_11 < nvl(addf_job_score_q1_3days, nvl(addf_boss_score_q1_3days, addf_cp_score_q1_3days)) then 4
when addf_10 < nvl(addf_job_score_q1_3days, nvl(addf_boss_score_q1_3days, addf_cp_score_q1_3days)) then 5
when addf_9 < nvl(addf_job_score_q1_3days, nvl(addf_boss_score_q1_3days, addf_cp_score_q1_3days)) then 6
when addf_8 < nvl(addf_job_score_q1_3days, nvl(addf_boss_score_q1_3days, addf_cp_score_q1_3days)) then 7
when addf_7 < nvl(addf_job_score_q1_3days, nvl(addf_boss_score_q1_3days, addf_cp_score_q1_3days)) then 8
when addf_6 < nvl(addf_job_score_q1_3days, nvl(addf_boss_score_q1_3days, addf_cp_score_q1_3days)) then 9
when addf_5 < nvl(addf_job_score_q1_3days, nvl(addf_boss_score_q1_3days, addf_cp_score_q1_3days)) then 10
when addf_4 < nvl(addf_job_score_q1_3days, nvl(addf_boss_score_q1_3days, addf_cp_score_q1_3days)) then 11
when addf_3 < nvl(addf_job_score_q1_3days, nvl(addf_boss_score_q1_3days, addf_cp_score_q1_3days)) then 12
when addf_2 < nvl(addf_job_score_q1_3days, nvl(addf_boss_score_q1_3days, addf_cp_score_q1_3days)) then 13
when addf_1 < nvl(addf_job_score_q1_3days, nvl(addf_boss_score_q1_3days, addf_cp_score_q1_3days)) then 14
else 15 end as q1_base,

case
when nvl(addf_job_score_q2_3days, nvl(addf_boss_score_q2_3days, addf_cp_score_q2_3days))  is null  then 0
when addf_15 < nvl(addf_job_score_q2_3days, nvl(addf_boss_score_q2_3days, addf_cp_score_q2_3days)) then 0
when addf_14 < nvl(addf_job_score_q2_3days, nvl(addf_boss_score_q2_3days, addf_cp_score_q2_3days)) then 1
when addf_13 < nvl(addf_job_score_q2_3days, nvl(addf_boss_score_q2_3days, addf_cp_score_q2_3days)) then 2
when addf_12 < nvl(addf_job_score_q2_3days, nvl(addf_boss_score_q2_3days, addf_cp_score_q2_3days)) then 3
when addf_11 < nvl(addf_job_score_q2_3days, nvl(addf_boss_score_q2_3days, addf_cp_score_q2_3days)) then 4
when addf_10 < nvl(addf_job_score_q2_3days, nvl(addf_boss_score_q2_3days, addf_cp_score_q2_3days)) then 5
when addf_9 < nvl(addf_job_score_q2_3days, nvl(addf_boss_score_q2_3days, addf_cp_score_q2_3days)) then 6
when addf_8 < nvl(addf_job_score_q2_3days, nvl(addf_boss_score_q2_3days, addf_cp_score_q2_3days)) then 7
when addf_7 < nvl(addf_job_score_q2_3days, nvl(addf_boss_score_q2_3days, addf_cp_score_q2_3days)) then 8
when addf_6 < nvl(addf_job_score_q2_3days, nvl(addf_boss_score_q2_3days, addf_cp_score_q2_3days)) then 9
when addf_5 < nvl(addf_job_score_q2_3days, nvl(addf_boss_score_q2_3days, addf_cp_score_q2_3days)) then 10
when addf_4 < nvl(addf_job_score_q2_3days, nvl(addf_boss_score_q2_3days, addf_cp_score_q2_3days)) then 11
when addf_3 < nvl(addf_job_score_q2_3days, nvl(addf_boss_score_q2_3days, addf_cp_score_q2_3days)) then 12
when addf_2 < nvl(addf_job_score_q2_3days, nvl(addf_boss_score_q2_3days, addf_cp_score_q2_3days)) then 13
when addf_1 < nvl(addf_job_score_q2_3days, nvl(addf_boss_score_q2_3days, addf_cp_score_q2_3days)) then 14
else 15 end as q2_base,

case
when nvl(addf_job_score_q3_3days, nvl(addf_boss_score_q3_3days, addf_cp_score_q3_3days))  is null  then 0
when addf_15 < nvl(addf_job_score_q3_3days, nvl(addf_boss_score_q3_3days, addf_cp_score_q3_3days)) then 0
when addf_14 < nvl(addf_job_score_q3_3days, nvl(addf_boss_score_q3_3days, addf_cp_score_q3_3days)) then 1
when addf_13 < nvl(addf_job_score_q3_3days, nvl(addf_boss_score_q3_3days, addf_cp_score_q3_3days)) then 2
when addf_12 < nvl(addf_job_score_q3_3days, nvl(addf_boss_score_q3_3days, addf_cp_score_q3_3days)) then 3
when addf_11 < nvl(addf_job_score_q3_3days, nvl(addf_boss_score_q3_3days, addf_cp_score_q3_3days)) then 4
when addf_10 < nvl(addf_job_score_q3_3days, nvl(addf_boss_score_q3_3days, addf_cp_score_q3_3days)) then 5
when addf_9 < nvl(addf_job_score_q3_3days, nvl(addf_boss_score_q3_3days, addf_cp_score_q3_3days)) then 6
when addf_8 < nvl(addf_job_score_q3_3days, nvl(addf_boss_score_q3_3days, addf_cp_score_q3_3days)) then 7
when addf_7 < nvl(addf_job_score_q3_3days, nvl(addf_boss_score_q3_3days, addf_cp_score_q3_3days)) then 8
when addf_6 < nvl(addf_job_score_q3_3days, nvl(addf_boss_score_q3_3days, addf_cp_score_q3_3days)) then 9
when addf_5 < nvl(addf_job_score_q3_3days, nvl(addf_boss_score_q3_3days, addf_cp_score_q3_3days)) then 10
when addf_4 < nvl(addf_job_score_q3_3days, nvl(addf_boss_score_q3_3days, addf_cp_score_q3_3days)) then 11
when addf_3 < nvl(addf_job_score_q3_3days, nvl(addf_boss_score_q3_3days, addf_cp_score_q3_3days)) then 12
when addf_2 < nvl(addf_job_score_q3_3days, nvl(addf_boss_score_q3_3days, addf_cp_score_q3_3days)) then 13
when addf_1 < nvl(addf_job_score_q3_3days, nvl(addf_boss_score_q3_3days, addf_cp_score_q3_3days)) then 14
else 15 end as q3_base,

    j.num as job_addf_num_3days,
    bos.num as boss_addf_num_3days,
    cipo.num as cp_addf_num_3days,
case 
    when a.page <=2 then 2.65 
    when a.page <= 4 then 2.35 
    when a.page <= 7 then 2
    when a.page <= 10 then 1.8
    when a.page <= 12 then 1.5
    when a.page <= 15 then 1.35
    when a.page <= 20 then 1.1
    else 1 end as page_base,

    nvl(block.business, 0) as business_type,

    boss_title_type,
    job_overseas_tag,
    boss_cmp_level,
    job_degree,
    job_workyears,
    boss_l1code,
    boss_l2code,
    boss_addf_pchat_rate_2d,
    boss_addf_pchat_times_2d,
    boss_addf_rate_2d,
    boss_addf_success_rate_2d,
    boss_addf_success_times_2d,
    boss_addf_times_2d,
    boss_addfchat_rate_2d,
    boss_addfchat_times_2d,
    boss_chat_s2_num_1d7,
    boss_cmp_pas_addf_rate,
    boss_comp_industry,
    boss_comp_scale,
    boss_comp_stage,
    boss_company,
    boss_company_kwid,
    boss_det_geek_14d,
    boss_det_times_2d,
    boss_district_code,
    boss_min_chat_tdiff,
    boss_notify_num_1d3,
    boss_paddf_pchat_rate_2d,
    boss_paddf_pchat_times_2d,
    boss_paddf_rate_2d,
    boss_paddf_success_rate_2d,
    boss_paddf_success_times_2d,
    boss_paddf_times_2d,
    boss_paddfchat_rate_2d,
    boss_paddfchat_times_2d,
    boss_pdet_geek_14d,
    boss_pdet_times_2d,
    boss_psuccess_rate_2d,
    boss_psuccess_times_2d,
    boss_pview_geek_14d,
    boss_ret_num_1d3,
    boss_view_geek_14d,
    job_addf_pchat_rate_2d,
    job_addf_pchat_rate_7d,
    job_addf_pchat_times_2d,
    job_addf_pchat_times_7d,
    job_addf_rate_2d,
    job_addf_rate_7d,
    job_addf_success_rate_2d,
    job_addf_success_rate_7d,
    job_addf_success_times_2d,
    job_addf_success_times_7d,
    job_addf_times_2d,
    job_addf_times_7d,
    job_addfchat_rate_2d,
    job_addfchat_rate_7d,
    job_addfchat_times_2d,
    job_addfchat_times_7d,
    job_det_num_24h,
    job_det_times_2d,
    job_det_times_7d,
    job_f1_pas_cp_addf_rate_1d7,
    job_high_salary,
    job_list_num_1d3,
    job_low_salary,
    job_min_active_tdiff,
    job_overseas_level,
    job_paddf_pchat_rate_2d,
    job_paddf_pchat_rate_7d,
    job_paddf_pchat_times_2d,
    job_paddf_pchat_times_7d,
    job_paddf_rate_14d,
    job_paddf_rate_2d,
    job_paddf_rate_7d,
    job_paddf_success_rate_2d,
    job_paddf_success_rate_7d,
    job_paddf_success_times_2d,
    job_paddf_success_times_7d,
    job_paddf_times_14d,
    job_paddf_times_2d,
    job_paddf_times_7d,
    job_paddfchat_rate_2d,
    job_paddfchat_rate_7d,
    job_paddfchat_times_2d,
    job_paddfchat_times_7d,
    job_pas_addf_num_24h,
    job_pdet_rate_14d,
    job_pdet_times_14d,
    job_pdet_times_2d,
    job_pdet_times_7d,
    job_plist_times_14d,
    job_psuccess_rate_2d,
    job_psuccess_rate_7d,
    job_psuccess_times_2d,
    job_psuccess_times_7d,
    job_register_tdiff,
    job_success_rate_2d,
    job_success_rate_7d,
    job_success_times_2d,
    job_success_times_7d
from
    (
        select
            *
        from
            arc_eight_dev.score_sample_daily_ctr
        where
            ds = '$ystd'
    ) a
    left join (
        select addf_ratio, cp from arc_eight_dev.score_feature_pct1_cp_ratio
    ) f_pct1 on a.cp = f_pct1.cp
    left join (
        select addf_ratio, cp from arc_eight_dev.score_feature_pct2_cp_ratio
    ) f_pct2 on a.cp = f_pct2.cp
    left join (
        select addf_ratio, cp from arc_eight_dev.score_feature_pct3_cp_ratio
    ) f_pct3 on a.cp = f_pct3.cp
    left join (
        select addf_ratio, cp from arc_eight_dev.score_feature_pct4_cp_ratio
    ) f_pct4 on a.cp = f_pct4.cp
    left join (
        select addf_ratio, cp from arc_eight_dev.score_feature_pct5_cp_ratio
    ) f_pct5 on a.cp = f_pct5.cp
    left join (
        select addf_ratio, cp from arc_eight_dev.score_feature_pct6_cp_ratio
    ) f_pct6 on a.cp = f_pct6.cp
    left join (
        select addf_ratio, cp from arc_eight_dev.score_feature_pct7_cp_ratio
    ) f_pct7 on a.cp = f_pct7.cp
    left join (
        select addf_ratio, cp from arc_eight_dev.score_feature_pct8_cp_ratio
    ) f_pct8 on a.cp = f_pct8.cp
    left join (
        select distinct boss_id, job_id, business from ods_boss_business.ods_block_job_state
    ) block on a.boss_id = block.boss_id and a.job_id = block.job_id
    left join (
        select
            job_id,
            addf_job_score_q1_3days,
            addf_job_score_q2_3days,
            addf_job_score_q3_3days,
            num
        from
            arc_eight_dev.generate_score_feature_temp_job_${ystd_format}
    ) j on a.job_id = j.job_id
    left join (
        select
            boss_id,
            addf_boss_score_q1_3days,
            addf_boss_score_q2_3days,
            addf_boss_score_q3_3days,
            num
        from
            arc_eight_dev.generate_score_feature_temp_boss_${ystd_format}
    ) bos on a.boss_id = bos.boss_id
    left join (
        select
            cp,
            addf_cp_score_q1_3days,
            addf_cp_score_q2_3days,
            addf_cp_score_q3_3days,
            num
        from
            arc_eight_dev.generate_score_feature_temp_cp_${ystd_format}
    ) cipo on concat_ws('-', a.city, a.pos) = cipo.cp
    left join (
        select
            distinct boss_id,
            job_id,
            sessionid,
            page,
            rank,
            to_date(list_time) as date,
            boss_title_type,
            job_overseas_tag,
            boss_cmp_level,
            job_degree,
            job_workyears,
            boss_l1code,
            boss_l2code,
            boss_addf_pchat_rate_2d,
            boss_addf_pchat_times_2d,
            boss_addf_rate_2d,
            boss_addf_success_rate_2d,
            boss_addf_success_times_2d,
            boss_addf_times_2d,
            boss_addfchat_rate_2d,
            boss_addfchat_times_2d,
            boss_chat_s2_num_1d7,
            boss_cmp_pas_addf_rate,
            boss_comp_industry,
            boss_comp_scale,
            boss_comp_stage,
            boss_company,
            boss_company_kwid,
            boss_det_geek_14d,
            boss_det_times_2d,
            boss_district_code,
            boss_min_chat_tdiff,
            boss_notify_num_1d3,
            boss_paddf_pchat_rate_2d,
            boss_paddf_pchat_times_2d,
            boss_paddf_rate_2d,
            boss_paddf_success_rate_2d,
            boss_paddf_success_times_2d,
            boss_paddf_times_2d,
            boss_paddfchat_rate_2d,
            boss_paddfchat_times_2d,
            boss_pdet_geek_14d,
            boss_pdet_times_2d,
            boss_psuccess_rate_2d,
            boss_psuccess_times_2d,
            boss_pview_geek_14d,
            boss_ret_num_1d3,
            boss_view_geek_14d,
            job_addf_pchat_rate_2d,
            job_addf_pchat_rate_7d,
            job_addf_pchat_times_2d,
            job_addf_pchat_times_7d,
            job_addf_rate_2d,
            job_addf_rate_7d,
            job_addf_success_rate_2d,
            job_addf_success_rate_7d,
            job_addf_success_times_2d,
            job_addf_success_times_7d,
            job_addf_times_2d,
            job_addf_times_7d,
            job_addfchat_rate_2d,
            job_addfchat_rate_7d,
            job_addfchat_times_2d,
            job_addfchat_times_7d,
            job_det_num_24h,
            job_det_times_2d,
            job_det_times_7d,
            job_f1_pas_cp_addf_rate_1d7,
            job_high_salary,
            job_list_num_1d3,
            job_low_salary,
            job_min_active_tdiff,
            job_overseas_level,
            job_paddf_pchat_rate_2d,
            job_paddf_pchat_rate_7d,
            job_paddf_pchat_times_2d,
            job_paddf_pchat_times_7d,
            job_paddf_rate_14d,
            job_paddf_rate_2d,
            job_paddf_rate_7d,
            job_paddf_success_rate_2d,
            job_paddf_success_rate_7d,
            job_paddf_success_times_2d,
            job_paddf_success_times_7d,
            job_paddf_times_14d,
            job_paddf_times_2d,
            job_paddf_times_7d,
            job_paddfchat_rate_2d,
            job_paddfchat_rate_7d,
            job_paddfchat_times_2d,
            job_paddfchat_times_7d,
            job_pas_addf_num_24h,
            job_pdet_rate_14d,
            job_pdet_times_14d,
            job_pdet_times_2d,
            job_pdet_times_7d,
            job_plist_times_14d,
            job_psuccess_rate_2d,
            job_psuccess_rate_7d,
            job_psuccess_times_2d,
            job_psuccess_times_7d,
            job_register_tdiff,
            job_success_rate_2d,
            job_success_rate_7d,
            job_success_times_2d,
            job_success_times_7d
        from
            arc_eight_dev.bossrec_train_daily
        where
            ds = '$ystd'
            and source = 'f1_rec'
            and rank = '1'
    ) b on a.boss_id = b.boss_id
    and a.job_id = b.job_id
    and a.sessionid = b.sessionid
    and a.page = b.page
    and a.date = b.date;"

del_temp_table_sql="
drop table if exists arc_eight_dev.generate_score_feature_temp_job_${ystd_format};
drop table if exists arc_eight_dev.generate_score_feature_temp_boss_${ystd_format};
drop table if exists arc_eight_dev.generate_score_feature_temp_cp_${ystd_format};
drop table if exists arc_eight_dev.score_feature_pct1_cp_ratio;
drop table if exists arc_eight_dev.score_feature_pct2_cp_ratio;
drop table if exists arc_eight_dev.score_feature_pct3_cp_ratio;
drop table if exists arc_eight_dev.score_feature_pct4_cp_ratio;
drop table if exists arc_eight_dev.score_feature_pct5_cp_ratio;
drop table if exists arc_eight_dev.score_feature_pct6_cp_ratio;
drop table if exists arc_eight_dev.score_feature_pct7_cp_ratio;
drop table if exists arc_eight_dev.score_feature_pct8_cp_ratio;"

flush_sql="
set
    hive.exec.dynamic.partition.mode = nonstrict;

insert
    overwrite table arc_eight_dev.score_train partition(ds = '$ystd')
select
    boss_id,
    job_id,
    sessionid,
    page,
    list_hour,
    date,
    weekday,
    city,
    pos,
    cp,
    num,
    pct1,
    pct2,
    pct3,
    pct4,
    pct5,
    pct6,
    pct7,
    pct8,
    pct1_ratio,
    pct2_ratio,
    pct3_ratio,
    pct4_ratio,
    pct5_ratio,
    pct6_ratio,
    pct7_ratio,
    pct8_ratio,
    nvl(pct1 * pct1_ratio, 0) as multi_pct1,
    nvl(pct2 * pct2_ratio, 0) as multi_pct2,
    nvl(pct3 * pct3_ratio, 0) as multi_pct3,
    nvl(pct4 * pct4_ratio, 0) as multi_pct4,
    nvl(pct5 * pct5_ratio, 0) as multi_pct5,
    nvl(pct6 * pct6_ratio, 0) as multi_pct6,
    nvl(pct7 * pct7_ratio, 0) as multi_pct7,
    nvl(pct8 * pct8_ratio, 0) as multi_pct8,
    nvl(pct1 * pct1_ratio, 0) + nvl(pct2 * pct2_ratio, 0) + nvl(pct3 * pct3_ratio, 0) + nvl(pct4 * pct4_ratio, 0) + nvl(pct5 * pct5_ratio, 0) + nvl(pct6 * pct6_ratio, 0) + nvl(pct7 * pct7_ratio, 0) + nvl(pct8 * pct8_ratio, 0) as pct_sum,
    addf_num,
    suc_num,
    avg_addf_score,
    avg_suc_score,
    suc_1,
    suc_2,
    suc_3,
    suc_4,
    suc_5,
    suc_6,
    suc_7,
    suc_8,
    suc_9,
    suc_10,
    suc_11,
    suc_12,
    suc_13,
    suc_14,
    suc_15,
    addf_1,
    addf_2,
    addf_3,
    addf_4,
    addf_5,
    addf_6,
    addf_7,
    addf_8,
    addf_9,
    addf_10,
    addf_11,
    addf_12,
    addf_13,
    addf_14,
    addf_15,
    addf_job_score_q1_3days,
    addf_job_score_q2_3days,
    addf_job_score_q3_3days,
    addf_boss_score_q1_3days,
    addf_boss_score_q2_3days,
    addf_boss_score_q3_3days,
    addf_cp_score_q1_3days,
    addf_cp_score_q2_3days,
    addf_cp_score_q3_3days,
    case
        when q1_strict < -0.325 then 1
        when q1_strict < 0.12259 then 2
        when q1_strict > 0.12259 then 3
        else 0
    end as q1_strict,
    case
        when q2_strict < -0.391388 then 1
        when q2_strict < -0.00117 then 2
        when q2_strict > -0.00117 then 3
        else 0
    end as q2_strict,
    case
        when q1_strict < -0.4243949 then 1
        when q3_strict < -0.087867 then 2
        when q3_strict > -0.087867 then 3
        else 0
    end as q3_strict,
    q1_base,
    q2_base,
    q3_base,
    job_addf_num_3days,
    boss_addf_num_3days,
    cp_addf_num_3days,
    page_base,
    business_type,
    boss_title_type,
    job_overseas_tag,
    boss_cmp_level,
    job_degree,
    job_workyears,
    boss_l1code,
    boss_l2code,
    boss_addf_pchat_rate_2d,
    boss_addf_pchat_times_2d,
    boss_addf_rate_2d,
    boss_addf_success_rate_2d,
    boss_addf_success_times_2d,
    boss_addf_times_2d,
    boss_addfchat_rate_2d,
    boss_addfchat_times_2d,
    boss_chat_s2_num_1d7,
    boss_cmp_pas_addf_rate,
    boss_comp_industry,
    boss_comp_scale,
    boss_comp_stage,
    boss_company,
    boss_company_kwid,
    boss_det_geek_14d,
    boss_det_times_2d,
    boss_district_code,
    boss_min_chat_tdiff,
    boss_notify_num_1d3,
    boss_paddf_pchat_rate_2d,
    boss_paddf_pchat_times_2d,
    boss_paddf_rate_2d,
    boss_paddf_success_rate_2d,
    boss_paddf_success_times_2d,
    boss_paddf_times_2d,
    boss_paddfchat_rate_2d,
    boss_paddfchat_times_2d,
    boss_pdet_geek_14d,
    boss_pdet_times_2d,
    boss_psuccess_rate_2d,
    boss_psuccess_times_2d,
    boss_pview_geek_14d,
    boss_ret_num_1d3,
    boss_view_geek_14d,
    job_addf_pchat_rate_2d,
    job_addf_pchat_rate_7d,
    job_addf_pchat_times_2d,
    job_addf_pchat_times_7d,
    job_addf_rate_2d,
    job_addf_rate_7d,
    job_addf_success_rate_2d,
    job_addf_success_rate_7d,
    job_addf_success_times_2d,
    job_addf_success_times_7d,
    job_addf_times_2d,
    job_addf_times_7d,
    job_addfchat_rate_2d,
    job_addfchat_rate_7d,
    job_addfchat_times_2d,
    job_addfchat_times_7d,
    job_det_num_24h,
    job_det_times_2d,
    job_det_times_7d,
    job_f1_pas_cp_addf_rate_1d7,
    job_high_salary,
    job_list_num_1d3,
    job_low_salary,
    job_min_active_tdiff,
    job_overseas_level,
    job_paddf_pchat_rate_2d,
    job_paddf_pchat_rate_7d,
    job_paddf_pchat_times_2d,
    job_paddf_pchat_times_7d,
    job_paddf_rate_14d,
    job_paddf_rate_2d,
    job_paddf_rate_7d,
    job_paddf_success_rate_2d,
    job_paddf_success_rate_7d,
    job_paddf_success_times_2d,
    job_paddf_success_times_7d,
    job_paddf_times_14d,
    job_paddf_times_2d,
    job_paddf_times_7d,
    job_paddfchat_rate_2d,
    job_paddfchat_rate_7d,
    job_paddfchat_times_2d,
    job_paddfchat_times_7d,
    job_pas_addf_num_24h,
    job_pdet_rate_14d,
    job_pdet_times_14d,
    job_pdet_times_2d,
    job_pdet_times_7d,
    job_plist_times_14d,
    job_psuccess_rate_2d,
    job_psuccess_rate_7d,
    job_psuccess_times_2d,
    job_psuccess_times_7d,
    job_register_tdiff,
    job_success_rate_2d,
    job_success_rate_7d,
    job_success_times_2d,
    job_success_times_7d
from
    arc_eight_dev.bossrec_train_score_ctr_test
where
    ds = '$ystd';
"

####
    echo "Step1/6 --- 开始运行info_sql"
    echo "$info_sql"
    $HIVE -e "$info_sql"
    checkError $? $JOB_TYPE "info_sql"
    wait

    echo "Step2/6 --- 开始生产feature"
    $HIVE -e "$feature_job_sql" &
    $HIVE -e "$feature_boss_sql" &
    $HIVE -e "$feature_cp_sql" &
    $HIVE -e "$feature_pct_sql"

    wait
    echo "Step3/6 --- 开始运行sample_sql"
    echo "$sample_sql"
    $HIVE -e "$sample_sql"
    checkError $? $JOB_TYPE "sample_sql"
    wait
    echo "Step4/6 --- 开始运行train_sql"
    echo "$train_sql"
    $HIVE -e "$train_sql"
    checkError $? $JOB_TYPE "train_sql"
    
    echo "Step5/6 -- 开始删除临时表"
    echo "$del_temp_table_sql"
    $HIVE -e "$del_temp_table_sql"
    checkError $? $JOB_TYPE "del_temp_table_sql"
    
    echo "Step6/6 -- 开始flush"
    echo "$flush_sql"
    $HIVE -e "$flush_sql"
    checkError $? $JOB_TYPE "flush_sql"
}

dailyrun $@ -mmain -l $BUSINESS_LOG_DIR/$JOB_TYPE
