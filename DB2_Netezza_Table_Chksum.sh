function build_export_select
{
	# build export sql so that nz and db2 output same format result
	db2 -x "with db2_select_cols as (select TABSCHEMA, TABNAME, 
			XMLCAST(XMLGROUP(case when COLNO = 0 then '' else ', ' end || 
      
      CASE WHEN TYPENAME in ('DOUBLE','REAL')
      THEN 'DECIMAL('||COLNAME||',16,8) as ' || COLNAME
      ELSE COLNAME
      END
      
      AS A ORDER BY COLNO) AS VARCHAR(32672)) as SELECT_COLS
			from syscat.columns where tabschema = '$SCHEMA' and TABNAME='$TABLE' and (COLNAME not like '%_ID' and COLNAME not like '%_BK' and COLNAME not in ('EFFECTIVE_TO','EFFECTIVE_FROM','EFFECTIVE_END_TIMESTAMP','EFFECTIVE_START_TIMESTAMP','ETL_UPDATE_TSTMP','CHECKSUM'))
			group by TABSCHEMA, TABNAME),
			cols_present as (select TABSCHEMA, TABNAME, sum(case when COLNAME='BATCHRUNID' then 1 else 0 end) as BATCHRUNID,
								sum(case when COLNAME='EFFECTIVE_END_TIMESTAMP' then 1 else 0 end) as EFFECTIVE_END_TIMESTAMP,
								sum(case when COLNAME='PATIENT_BK' then 1 else 0 end) as PATIENT_BK,
								sum(case when COLNAME='EFFECTIVE_TO' then 1 else 0 end) as EFFECTIVE_TO
								from SYSCAT.COLUMNS
								where TABSCHEMA = '$SCHEMA' and TABNAME='$TABLE'
								group by TABSCHEMA, TABNAME)
		
			select 'select ' || TRIM(L ',' FROM q1.SELECT_COLS) || 
			case when q2.BATCHRUNID>0 THEN ',BATCHRUNID' ELSE '' END
			|| ' from ' || trim(q1.TABSCHEMA)||'.'||q1.TABNAME
			|| case when q2.EFFECTIVE_END_TIMESTAMP>0 THEN ' where EFFECTIVE_END_TIMESTAMP is null ' ELSE '' END
			|| case when q2.EFFECTIVE_TO>0 THEN ' where  EFFECTIVE_TO is null ' ELSE '' END
			|| case when q2.PATIENT_BK>0 THEN ' and not (PATIENT_BK=0 or PATIENT_BK is null) ' ELSE '' END
			|| ';' as DB2_SELECT
			FROM db2_select_cols q1
			inner join cols_present q2
			on q1.TABSCHEMA=q2.TABSCHEMA and q1.TABNAME=q2.TABNAME" > /tmp/select.$SCHEMA.$TABLE
			
	RC=$?; if [[ $RC -ne 0 ]]; then echo "error db2 generating export select"; cat /tmp/select.$SCHEMA.$TABLE; return 1; fi

	read DB2_EXPORT_SELECT < <(cat /tmp/select.$SCHEMA.$TABLE)

	db2 -x "with nz_select_cols as (select TABSCHEMA, TABNAME, XMLCAST(XMLGROUP(
									case when COLNO = 0 then '' else ', ' end
									|| case when TYPENAME = 'TIME' then 'TO_CHAR(current_date + ' || COLNAME || ',''HH24.MI.SS'') as ' || COLNAME
									when TYPENAME = 'TIMESTAMP' then 'TO_CHAR(' || COLNAME || ',''YYYY-MM-DD HH24:MI:SS'') as ' || COLNAME
                  when TYPENAME in ('DOUBLE','REAL') THEN COLNAME || '::numeric(16,8) as ' || COLNAME 
									ELSE COLNAME end
									AS A ORDER BY COLNO) AS VARCHAR(32672)) as SELECT_COLS
									from syscat.columns where tabschema = '$SCHEMA' and TABNAME='$TABLE' and (COLNAME not like '%_ID' and COLNAME not like '%_BK' and COLNAME not in ('EFFECTIVE_TO','EFFECTIVE_FROM','EFFECTIVE_END_TIMESTAMP','EFFECTIVE_START_TIMESTAMP','ETL_UPDATE_TSTMP','CHECKSUM'))
									group by TABSCHEMA, TABNAME),
		 
			cols_present as (select TABSCHEMA, TABNAME, sum(case when COLNAME='BATCHRUNID' then 1 else 0 end) as BATCHRUNID,
								sum(case when COLNAME='EFFECTIVE_END_TIMESTAMP' then 1 else 0 end) as EFFECTIVE_END_TIMESTAMP,
								sum(case when COLNAME='PATIENT_BK' then 1 else 0 end) as PATIENT_BK,
								sum(case when COLNAME='EFFECTIVE_TO' then 1 else 0 end) as EFFECTIVE_TO
								from SYSCAT.COLUMNS
								where TABSCHEMA = '$SCHEMA' and TABNAME='$TABLE'
								group by TABSCHEMA, TABNAME)
			
			select 'select ' || TRIM(L ',' FROM q1.SELECT_COLS) || 
			case when q2.BATCHRUNID>0 THEN ',BATCHRUNID' ELSE '' END
			|| ' from ' || trim(q1.TABSCHEMA)||'.'||q1.TABNAME
			|| case when q2.EFFECTIVE_END_TIMESTAMP>0 THEN ' where EFFECTIVE_END_TIMESTAMP is null ' ELSE '' END
			|| case when q2.EFFECTIVE_TO>0 THEN ' where  EFFECTIVE_TO is null ' ELSE '' END
			|| case when q2.PATIENT_BK>0 THEN ' and not (PATIENT_BK=0 or PATIENT_BK is null) ' ELSE '' END
			|| ';' as DB2_SELECT
			FROM nz_select_cols q1
			inner join cols_present q2
			on q1.TABSCHEMA=q2.TABSCHEMA and q1.TABNAME=q2.TABNAME" > /tmp/select.$SCHEMA.$TABLE

	RC=$?; if [[ $RC -ne 0 ]]; then echo "error nz generating export select"; cat /tmp/select.$SCHEMA.$TABLE; return 1; fi

	read NZ_EXPORT_SELECT < <(cat /tmp/select.$SCHEMA.$TABLE)
}

function checksum_list
{
	db2 connect
	if [[ $? -ne 0 ]]; then echo "Please connect to db2 first. db2 connect to DATABASE user USERID"; return 1; fi
	cleanup_check
	while read d s t X
	do 
		echo "check $d.$s.$t # $X"
		check $d $s $t
		if [[ $? -ne 0 ]]
		then 
			echo "Error from migrate function"
	#       return 1
		fi
	
	done  < <(grep -v "#" "$1") 
}

function check
{	
	DATABASE=$1
	SCHEMA=$2
	TABLE=$3
	DB2_STEP=$TABLE.$SCHEMA.$DATABASE
	EXPORT_LOG=tmp/$DB2_STEP.explog
	
	echo $DB2_STEP
		
	build_export_select
	if [[ $? -ne 0 ]]; then echo "Generation of export select failed"; return 1; fi
	
	#super quick so just running in background so db2 can start straigh away
	nz_unload -host pbprdapp0405 -db $1 -sql "$NZ_EXPORT_SELECT" -file $DB2_STEP.nz
	
	db2 -tvx "export to $DB2_STEP.db2 of del modified by datesiso codepage=920 timestampformat=\"YYYY-MM-DD HH:MM:SS\" coldel| nochardel striplzeros $DB2_EXPORT_SELECT" > $EXPORT_LOG
	EXPORT_RC=$?
	if [[ $EXPORT_RC -ne 0 ]]
	then 
		echo "Error in DB2 export. Exit"
		tail $EXPORT_LOG
		#  echo KILL nzload job 
		#  kill -9 $(jobs -p)
		return 1
	fi
	
	#Fix nulls in nz export
	sed -e 's/NULL//g' -e 's/\\//g' -e 's/|+/|/g' $DB2_STEP.nz > $DB2_STEP.nz.tmp; mv $DB2_STEP.nz.tmp $DB2_STEP.nz
	sed -e 's/NULL//g' -e 's/\\//g' -e 's/|+/|/g' $DB2_STEP.db2 > $DB2_STEP.db2.tmp; mv $DB2_STEP.db2.tmp $DB2_STEP.db2
	
	#Sort for checksum
	sort -o $DB2_STEP.db2 $DB2_STEP.db2
	sort -o $DB2_STEP.nz $DB2_STEP.nz
	
	#Chksum file
	md5sum $DB2_STEP.db2 > $DB2_STEP.db2chk
	md5sum $DB2_STEP.nz > $DB2_STEP.nzchk
	
	#Read checksum
	read db2chk f < <(cat $DB2_STEP.db2chk)
	read nzchk f < <(cat $DB2_STEP.nzchk)
	
	if [ $db2chk != $nzchk ]
	then 
		echo "$DB2_STEP Checksums Do Not Match" >> checksum.log
		mv $DB2_STEP.db2 exceptions/$DB2_STEP.db2
		mv $DB2_STEP.nz exceptions/$DB2_STEP.nz
		diff exceptions/$DB2_STEP.nz exceptions/$DB2_STEP.db2 > exceptions/$DB2_STEP.diff
	else 
		echo "$DB2_STEP Checksums Match" >> checksum.log
		rm $DB2_STEP.db2
		rm $DB2_STEP.nz
	fi
	rm $DB2_STEP.db2chk
	rm $DB2_STEP.nzchk
}

function cleanup_check
{
	touch exceptions/test.txt
	touch tmp/test.txt
	rm exceptions/*
	rm tmp/*
	rm checksum.log
}

