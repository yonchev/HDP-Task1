#/bin/bash
hadoop fs -rm /user/hue/task1/*
hadoop fs -rmdir /user/hue/task1
hadoop jar task1.jar /user/hue/flightdelays /user/hue/weather /user/hue/task1
