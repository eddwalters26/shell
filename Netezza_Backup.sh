function build_groom_tables {
        db=$1
        echo "Generate table list from... $db" >> nz_backup_log.log
        if [ -e groom_tables.tmp ]; then rm groom_tables.tmp; fi
        nzsql -db $db -r -q -t -F $'\t' -c "select SCHEMA, TABLENAME from _v_table where SCHEMA not in ('ADMIN') and OBJTYPE in ('TABLE', 'SECURE TABLE') order by random();" -o groom_tables.tmp2
        head -n-1 groom_tables.tmp2 > ${db}_groom_tables.tmp
        rm groom_tables.tmp2
}

function split_tables {
        db=$1
        numberStreams=$2
        total_lines=$(wc -l < ${db}_groom_tables.tmp)
        echo "Total Lines $total_lines" >> nz_backup_log.log
        lines_per_file=$((($total_lines + $numberStreams - 1) / $numberStreams))
        split --lines=$lines_per_file ${db}_groom_tables.tmp ${db}_groomtables.
        rm ${db}_groom_tables.tmp
}

function groom_list {
        db=$1;
        echo "Grooming tables on... $db" >> nz_backup_log.log
        while read s t
        do
            nzsql -db $db -q -c "GROOM TABLE $s.$t RECORDS ALL;"

            if [ $? -ne 0 ]
            then
                nzsql -db $db -q -c "GROOM TABLE $s.$t RECORDS ALL RECLAIM BACKUPSET NONE;"
            fi

        done < <(cat $2)
	echo "Groom tables on $db finished at " $(date) >> nz_backup_log.log
}

function generate_stats {
        db=$1
        groomFile=$2
        echo "Generating statistics on... $db" >> nz_backup_log.log
        while read s t
        do
            nzsql -db $db -q -c "GENERATE STATISTICS ON $s.$t;"
        done < <(cat $2)
	echo "Generating statistics on $db finished at " $(date) >> nz_backup_log.log 
}

function groom_and_stat {
        db=$1
        build_groom_tables $db
        split_tables $db 10 

        FILES=${db}_groomtables.a*

        for f in $FILES
        do
            groom_list $db $f &
        done
        wait
		for f in $FILES
        do
            generate_stats $db $f &
        done
        wait

        rm ${db}_groomtables.a*
}

function groom_extra {
	echo "Starting groom on " $1 " at " $(date) >> nz_backup_log.log
	groom_and_stat $1
	echo "Groom finished on " $1 " at " $(date) >> nz_backup_log.log
}

function backup_database {
    db=$1
    echo "Starting backup of " $db  >> nz_backup_log.log
    nzbackup -db $db -dir /home/nz/Netezza_Backup >> nz_backup_log.log
  	echo $db " Databse Backup Finished at:        " $(date) >> nz_backup_log.log
}

function check_backup_database {
        db=$1
		attempts=$2
        echo "Checking backup of " $db >> nz_backup_log.log
        backupStatus=$(nzbackup -history -db $db | tail -n 1 | grep FAILED | wc -l)
        echo $backupStatus >> nz_backup_log.log
        if [ $backupStatus -eq 0 ]
        then
            echo $db " - Backup process was successful. Starting Groom" >> nz_backup_log.log
            #groom_extra $db
        elif [ $attempts -lt 5 ]
		then
            echo $db " - Backup process was unsucessful retrying..." >> nz_backup_log.log
            backup_database $db
			((attempts++))
			check_backup_database $db $attempts
        fi
}

if [ $# -gt 0 ]
then
    if [[ -e nz_backup_log.log ]]; then rm nz_backup_log.log; fi

    echo "Backup process started at:        "  $(date) > nz_backup_log.log
    echo "The following databases will be included in the backup" >> nz_backup_log.log
    echo $@ >> backup.log
    echo "Backup location - vbdwhapp0437 - /Netezza_Backup" >> nz_backup_log.log

    for db in "$@"
    do
        backup_database $db &
    done
    wait
	
	for db in "$@"
	do
		check_backup_database $db 1 &
	done
	
	echo "Backup process finished at:	" $(date) >> nz_backup_log.log	

else
    echo "You must provide at least one database argument"
    exit 0
fi

