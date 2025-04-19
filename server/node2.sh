export GO_BUILD_TAGS="oss_cluster"
export MM_FILESETTINGS_DIRECTORY="/tmp/mmdata2"
export MM_SERVICESETTINGS_LISTENADDRESS=":8066"
export MM_SQLSETTINGS_DATASOURCE="postgres://mmuser:mostest@localhost/mattermost_test?sslmode=disable"
export MM_CACHESETTINGS_REDISADDRESS="localhost:6379"
export MM_CLUSTERSETTINGS_ENABLE="true"

make run-server