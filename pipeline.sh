#!/bin/bash
#A simple bash script to 1. scrape data 2. move to sftp azure 3. move to database
#set -e

#0. get number of pages to parse (default = 5)
scrape_pages_count=5
re='^[0-9]+$'
echo "number of input args: $#"
if [ $# -eq 0 ] #checks if number of arguments are 0
  then
    echo "No arguments supplied! Then Scraping 5 pages"
  elif [[ $1 =~ $re ]] # "=~" means regex matches 
  then
	scrape_pages_count="$1"
	echo "number of pages to scrape: $1"
  else
	echo "Input argument is not a number! exiting ..."
	exit 1
fi

#0.1 precheck: if there are any files in data folder -> move to archive
new_data_dir="new_data"
archive_data_dir="archive_data"

#files_list=`ls ./new_data/*\(.csv\|.html\)` #doesn't work
files_list=`ls ${new_data_dir}/*.* 2>/dev/null`
timestamp=$(date "+%Y%m%d%H%M%S")

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

#1. activate python venv (This step depends on the venv or environment etc.)
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

#4. deactivate venv
conda deactivate
echo "venv deactivated"

#encrypt some fields of csv file


#send to sftp azure
#connect to sftp using ssh key pair
sftp -i ~/.ssh/id_rsa abstorage1test.storageuser1@abstorage1test.blob.core.windows.net << EOF
	put ./${new_data_dir}/*.csv
	exit
EOF
echo "Moved results to sftp"

#move to DB
