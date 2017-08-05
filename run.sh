#!/usr/bin/env bash

project=whole-hbase-export
main_class=com.haizhi.HBaseExportServer

start() {
	status
	if [ ! $? -eq 0 ]; then
		echo "${project} is already running.."
		return 1
	fi

    sh mvnw clean
    [ ! $? -eq 0 ] && echo "maven clean fail.." && return -1

    sh mvnw package
    [ ! $? -eq 0 ] && echo "maven package fail.." && return -1

    alias cp=cp
    cp -rf target/${project}/* .
    cd bin
    nohup sh startup.sh > /dev/null 2>&1 &
    echo "${project} start success..."
}

stop() {
	ret=`status`
	if [ -z "${ret}" ]; then
	    echo "${project} not running.."
	    return 1
	fi

	kill -9 ${ret}

	status
	[ $? -eq 0 ] && echo "${project} stop success..." && return 1

	echo "${project} stop fail..."
	return 0
}

package() {

    sh mvnw clean
    [ ! $? -eq 0 ] && echo "maven clean fail.." && return -1

    sh mvnw package
    [ ! $? -eq 0 ] && echo "maven package fail.." && return -1

    alias cp=cp
    cp -rf target/${project}/* .
}

restart() {
    stop
    sleep 2
    start
}

status() {
    pid=`sh check.sh ${project} ${main_class}`
    if [ -z ${pid} ]; then
        return 0
    fi
    echo "${pid}"
	return ${pid}
}

case "$1" in
	start|stop|restart|status|package)
  		$1
		;;
	*)
		echo $"Usage: $0 {start|stop|status|restart|package}"
		exit 1
esac
