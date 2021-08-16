export OZONE_OM_METADATA_LAYOUT=PREFIX
export OZONE_OM_ENABLE_FILESYSTEM_PATHS=true
cd hadoop-ozone/dist/target/ozone-*-SNAPSHOT/compose/ozone
docker-compose up -d --scale datanode=$1
