#!/bin to editbash
project_path=$(cd `dirname $0`; pwd)
starttime=`date +'%Y-%m-%d %H:%M:%S'`
today=`date +'%Y-%m-%d'`
echo ${starttime}


sh ${project_path}/fetch_bossrec_score_train_ctr_daily.sh -d ${today}


if [[ $? -eq 1 ]];then
exit 1
fi
endtime=`date +'%Y-%m-%d %H:%M:%S'`
start_seconds=$(date --date="$starttime" +%s);
end_seconds=$(date --date="$endtime" +%s);
echo "本次运行时间： "$((end_seconds-start_seconds))"s">>${project_path}/daily.log
