#!/bin/bash
MY_HOSTNAME=`hostname`
loop=`python send_task_msg.py "6=$MY_HOSTNAME=$JOBID"`
while [ "$loop" == "0" ] ; do
    start=$(date +%s)
    end=$(date +%s)
    tdiff=$(( end - start ))
    twait=`shuf -i 1-300 -n 1`
    while [ $tdiff -le $twait ]; do
        end=$(date +%s)
        tdiff=$(( end - start ))
        #echo "$start $end $tdiff"
    done
    echo  "checking to start..."
    loop=`python send_task_msg.py "6=$MY_HOSTNAME=$JOBID"`

done;

loop=true
while [ "$loop" = true ] ; do
	start=$(date +%s)
	end=$(date +%s)
	tdiff=$(( end - start ))
	twait=`shuf -i 1-300 -n 1`
	while [ $tdiff -le $twait ]; do
		end=$(date +%s)
		tdiff=$(( end - start ))
		#echo "$start $end $tdiff"
	done
	echo  "checking for new tasks..."
	mv job_${JOBID}.sh job_${JOBID}_old.sh
	MY_HOSTNAME=`hostname`
	#python get_task.py ${MY_HOSTNAME}
	#python run_task.py "0=${MY_HOSTNAME}=${JOBID}"
    python send_task_msg.py "7=${MY_HOSTNAME}=${JOBID}"
    if [ -f job_${JOBID}.sh ]; then
    	chmod +x job_${JOBID}.sh
    	ls -alt
    	run_diff=`diff job_${JOBID}.sh job_${JOBID}_old.sh`
    	if [ "x$run_diff"  != "x" ]; then
            loop=`python send_task_msg.py "6=$MY_HOSTNAME=$JOBID"`
            while [ "$loop" == "0" ] ; do
                start=$(date +%s)
                end=$(date +%s)
                tdiff=$(( end - start ))
                twait=`shuf -i 300-600 -n 1`
                while [ $tdiff -le $twait ]; do
                    end=$(date +%s)
                    tdiff=$(( end - start ))
                    #echo "$start $end $tdiff"
                done
                echo  "checking to start..."
                loop=`python send_task_msg.py "6=$MY_HOSTNAME=$JOBID"`

            done;
    		loop=false
    	fi
    else
        exit
    fi
done;
bash job_${JOBID}.sh