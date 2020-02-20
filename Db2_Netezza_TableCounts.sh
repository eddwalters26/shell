function build_export_select
{
	# build export sql so that nz and db2 output same format result
	db2 -x "select 'select count(*) from ' || trim(TABSCHEMA) || '.' || TABNAME ||'/* where EFFECTIVE_END_TIMESTAMP is null*/;'
			from SYSCAT.TABLES
			where TABSCHEMA='$SCHEMA' and TABNAME='$TABLE'" > /tmp/count.$SCHEMA.$TABLE
			
	RC=$?; if [[ $RC -ne 0 ]]; then echo "error generating export select"; cat /tmp/count.$SCHEMA.$TABLE; return 1; fi

	read DB2_EXPORT_SELECT < <(cat /tmp/count.$SCHEMA.$TABLE)

	read NZ_EXPORT_SELECT < <(cat /tmp/count.$SCHEMA.$TABLE)
}

function count_list
{
	db2 connect
	if [[ $? -ne 0 ]]; then echo "Please connect to db2 first. db2 connect to DATABASE user USERID"; return 1; fi
	cleanup_counts
	while read d s t X
	do 
		echo "count $d.$s.$t # $X"
		count $d $s $t
		if [[ $? -ne 0 ]]
		then 
			echo "Error from count function"
		fi 
	
	done  < <(grep -v "#" "$1") 
}

function count
{	
	DATABASE=$1
	SCHEMA=$2
	TABLE=$3
	DB2_STEP=$TABLE.$SCHEMA.$DATABASE
	EXPORT_LOG=tmp/$DB2_STEP.explog
	
	echo $DB2_STEP
		
	build_export_select
	if [[ $? -ne 0 ]]; then echo "Generation of export select failed"; return 1; fi
	
	#super quick so just running in background so db2 can start straight away
	nz_unload -host pbprdapp0405 -db $1 -sql "$NZ_EXPORT_SELECT" -file $DB2_STEP.nzcount
	
	db2 -tvx "export to $DB2_STEP.db2count of del modified by datesiso codepage=920 timestampformat=\"YYYY-MM-DD HH:MM:SS\" coldel| nochardel striplzeros $DB2_EXPORT_SELECT" > $EXPORT_LOG
	EXPORT_RC=$?
	if [[ $EXPORT_RC -ne 0 ]]
	then 
		echo "Error in DB2 export. Exit"
		tail $EXPORT_LOG
		#  echo KILL nzload job 
		#  kill -9 $(jobs -p)
		return 1
	fi
	
	#Read checksum
	read db2count f < <(cat $DB2_STEP.db2count)
	read nzcount f < <(cat $DB2_STEP.nzcount)
	
	echo "$DB2_STEP	DB2: $db2count	NZ: $nzcount" >> counts.tables
	
	if [ $db2count -eq $nzcount ]
	then 
		echo "$DB2_STEP Counts Match" >> counts.log
	else 
		echo "$DB2_STEP Counts Do Not Match" >> counts.log
	fi
	rm $DB2_STEP.db2count
	rm $DB2_STEP.nzcount
}

function cleanup_counts
{
	touch counts.log
	touch counts.tables
	rm counts.log
	rm counts.tables
}

