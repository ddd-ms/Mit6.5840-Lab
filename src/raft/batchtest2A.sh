#!/usr/bin/env fish

# 清除旧日志文件
rm ./lab2A-logs//*.log

set total 100
for i in (seq 1 $total)
    # go test -run 3A -bench . -benchtime 5s -benchmem > ./lab2A-logs/log$i.log
    # go test -run 3A -bench . -benchtime 5s -benchmem -cpu 1,2,4,8,16,32,64,128,256,512,1024 > ./lab2A-logs/log$i.log
    go test -run 3A 2> ./lab2A-logs/stderr-log$i.log
    # if stderr-logi.log is empty, then remove it
    if test -s ./lab2A-logs/stderr-log$i.log
        break
    else
        rm ./lab2A-logs/stderr-log$i.log
    end
end
echo "All Done!"