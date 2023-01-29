while ps -p 297472 --no-headers --format "etime pid %cpu %mem rss"; do
       sleep 1
done
