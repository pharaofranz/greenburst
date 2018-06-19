#!/bin/sh 

ssd_data=$(df -k | grep nvme)
path=$(echo $ssd_data | awk '{print $1}')
size=$(echo $ssd_data | awk '{print $2}' ) #| rev | cut -c 2- | rev )
used=$(echo $ssd_data | awk '{print $3}' ) #| rev | cut -c 2- | rev )
avail=$(echo $ssd_data | awk '{print $4}') # | rev | cut -c 2- | rev )
used_percent=$(echo $ssd_data | awk '{print $5}'| rev | cut -c 2- | rev )
mounted_on=$(echo $ssd_data | awk '{print $6}')

echo "ssd,tag1=sdata size=$size,used=$used,avail=$avail,used_percent=$used_percent"
