while pidstat -d -p 211204 1 --no-headers
    sudo iotop -p 193852 --no-headers --format "etime pid %cpu %mem rss"; do
       sleep 1
done
