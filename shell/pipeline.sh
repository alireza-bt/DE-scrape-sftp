# !/bin/bash

parent_path=$( cd "$(dirname "${BASH_SOURCE[0]}")" ; pwd -P )
cd "$parent_path"
cd .. # now we are in the main directory of project

#A simple bash script to 1. scrape data 2. move to sftp azure 3. move to database
#set -e

#write one function per step to be able to run the steps separately

#0.1 precheck: if there are any files in data folder -> move to archive
move_data_to_archive () {
	#files_list=`ls ./new_data/*\(.csv\|.html\)` #doesn't work
	files_list=`ls ${new_data_dir}/*.* 2>/dev/null`
	timestamp=$(date "+%Y%m%d_%H%M%S")
	dateVar=$(date "+%Y%m%d")

	for item in ${files_list}
	do
		#get just file name without path
		file_name=$(echo $item | awk -F'/' '{print $NF}')
		suffix=$(echo $file_name | cut -d. -f 2)
		new_name=''
		if [[ $suffix == 'html' ]]
		then
			name=$(echo $file_name | cut -d. -f 1)
			new_name="${name}_$timestamp.$suffix"
		elif [[ scrape_pages_count > 0 ]]
			new_name="${file_name}"
		else
			continue
		fi
		mv $item $archive_data_dir/$new_name
	done
	echo "moved files to archive"
}

#1. activate python venv (This step depends on the venv or environment etc.)
activate_pyenv () {
	source ~/miniforge3/bin/activate
	conda activate testenv
	#check if testenv is activated
	#conda info --envs
	if [[ $CONDA_DEFAULT_ENV != 'testenv' ]]
	then
		echo "testenv is not activate. exiting ..."
		exit 1
	fi
	echo "venv activated: $CONDA_DEFAULT_ENV"
}

# #4. deactivate venv
deactivate_pyenv () {
	conda deactivate
	echo "venv deactivated"
}

#2. Scrape, Parse and save html pages in csv format
scrape_data () {
	move_data_to_archive()
	activate_pyenv()

	for ((i=1;i<=$scrape_pages_count;i+=1))
	do
		#2. scrape data from indeed
		input_file="${new_data_dir}/indeed_$i.html"
		output_file="${new_data_dir}/result_$timestamp.csv"
		py_script="parse.py"
		start_value=$(( i*10 ))

		curl -o $input_file --user-agent 'Chrome/79' "https://de.indeed.com/jobs?q=Data+Engineer&l=hamburg&start=$start_value"
		echo "Scrape page $i done!"

		#3. check if html file exists and then run python script to parse html file
		#python script input parameter: input html file to parse
		if [[ -f "$input_file" ]] 
		then
			echo "$input_file exists."
			#/Users/alirezabitarafan/miniforge3/envs/testenv/bin/python ./$py_script ./$input_file
			python ./$py_script ./$input_file ./$output_file
			echo "parsing $input_file done!"
		else
			echo "$input_file not found!"
		fi

	done

	deactivate_pyenv()
}

#encrypt some fields of csv file
#encrypt_data () {}

#5. send to sftp azure
put_data_to_sftp () {
#connect to sftp using ssh key pair
sftp -i ~/.ssh/id_rsa abstorage1test.storageuser1@abstorage1test.blob.core.windows.net << EOF
put ./${new_data_dir}/*.csv
exit	
EOF
echo "Moved results to sftp"
}

#There are 2 ways here:
# 1-Getting data from SFTP and move to Azure SQL using BCP Commandline
# 2-Moving data directly from SFTP Storage to Azure SQL using DataFactory

#6. Getting data from SFTP
get_from_sftp () {
sftp -i ~/.ssh/id_rsa abstorage1test.storageuser1@abstorage1test.blob.core.windows.net << EOF
	get *_$dateVar_*.csv
	bye
EOF
echo "Received data from sftp"
}

#7. move to DB (Stage) using bcp commandline tool
move_data_to_DB () {
	#import data to azure in 100 rows batch
	#how to change this to create and replace table??
	#TODO:create a loop to move all the data??
	# remove -P to get password prompt
	# bcp data_warehouse.stage.IndeedJobs in "./new_data/result2.csv" -k -c -t ";" -r "\n" -U $username -P $password \
	# -S mydbserver1234.database.windows.net -b 2 -e ./Error_in.log

	# new version (using pythong and sql server driver)
	activate_pyenv
	echo "### start python query ####"
	#TODO: relative path inside bash script
	python python/query_azure.py -h
	echo "### python query finished ###"
	deactivate_pyenv
}


#8. move to (so to say) datalake
#8.0.0 use the following guide to install required things to work with azure database using python:
# https://learn.microsoft.com/en-us/azure/azure-sql/database/connect-query-python?view=azuresql
#8.0.1 einmalig create the dataset (project_datalake)
#8.0.2 einmalig created the table in the datalake


#9. move to target table

# run method (using the input flags)
run_steps() {
	local page_count=${scrape_pages}
	local steps=${steps_to_run}
	
	re='^[0-9]+$'
	#echo "number of input args: $#"
	from_step=$(echo ${steps} | cut -d':' -f 1 )
	to_step=$(echo ${steps} | cut -d':' -f 2 )

	if [ -z ${to_step} ]
	then
		to_step=${end_step}
	fi

	echo "steps to run: ${from_step} to ${to_step}"

	if [ ${from_step} -le 1 ]
	then
		if [[ ${page_count} =~ $re ]]
		then
			scrape_pages_count="${page_count}"
			echo "number of pages to scrape: ${page_count}"
		else
			echo "Input argument is not provided or is not valid! exiting ..."
			exit 1
		fi
	fi

	loop_counter=${from_step}
	while [ ${loop_counter} -le ${to_step} ]
	do
		case "${loop_counter}" in
			1) 
				echo "step 1: scraping"
				#scrape_data()
				;;
			2) 
				echo "step 2: loading to sftp"
				#put_data_to_sftp() 
				;;
			3) 
				echo "step 3: getting from sfpt"
				#get_from_sftp() 
				;;
			4) 
				echo "step 4: loading data to DB"
				move_data_to_DB 
				;;
		esac
		loop_counter=$(( ${loop_counter} + 1 ))
	done
}

help() {
	echo "usage: pipeline.sh [-h | [-s from:to (steps)] [-n scrape_pages_count (if applicable)]]"
	echo "steps: 1)scrape -- 2)load to sfpt -- 3)get from sftp -- 4)load to DB"
	exit 0
}

get_config_values() {
	username=$(awk -F "=" '/USER/ {print $2}' config.ini | tr -d '[:space:]')
	password=$(awk -F "=" '/PASS/ {print $2}' config.ini | tr -d '[:space:]')
	#echo ${username}
	#echo ${password}
}

## Main Part

new_data_dir="new_data"
archive_data_dir="archive_data"
sftp_data_dir="sftp_data"
end_step=4

#0. parse input flags when running the script
steps_to_run="-1"
while getopts n:s:h flag #means -n and -s need argument but -h doesn't need any
do
	case "${flag}" in
		n)	
			if [ ${steps_to_run} = "-1" ]; then
				steps_to_run="0:" #default
			fi
			scrape_pages=${OPTARG}
			;;
		s) 
			steps_to_run=${OPTARG}
			scrape_pages="1" #default
			;;
		h) 
			help 
			;;
	esac
done

get_config_values
run_steps
