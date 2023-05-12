
today=`date +%Y%m%d`

echo $today

tar --exclude='.gz' --exclude='.yarn' --exclude='node_modules' --exclude='.git' --exclude='.fusebox' -zcvf node-polars.$today.tar.gz .
