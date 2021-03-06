# logseeker
快速定位搜索日志辅助工具(针对大日志文件)

功能类似[timecat](https://github.com/fanfank/timecat), golang实现😁

## 支持日志格式

- [x] 按列存储格式 (例如常见: nginx的access log等)
- [x] json格式  

## Options

- s 开始时间(格式与日志格式保持一致就行)
- e 结束时间(格式与日志格式保持一致就行)
- n 进行比较的第n列值，默认第一列(1)
- f 列的分割符号，默认空格(" ")
- j json日志格式的键值(time)

> 设置j则默认为json日志格式

## 用法

```shell
logseeker -s "begin" -e "end" -n 1 -f " " test-access.log

# logseeker -h 查看帮助   -n 默认1 表示第一列  -f 列分割符号默认空格 -j json键值
logseeker -s "begin" -e "end" -n 1 -j "time" test-access.log
```

例如:
```shell
$ ./logseeker -s '2019-04-14T02:11:45.128Z' -e '2019-04-14T02:12:45.128Z' -n 3 test-access.log | wc -l

6337

$ ./logseeker -s '2019-04-14T02:11:45.128Z' -e '2019-04-14T02:12:45.128Z' -j 'time' test-access.log | wc -l

6337

$ ./logseeker -s '2019-04-14T02:11:45.128Z' -e '2019-04-14T02:12:45.128Z' -n 3 test-access.log > /tmp/xx.log

$ ./logseeker -s '2019-04-14T02:11:45.128Z' -e '2019-04-14T02:12:45.128Z' -j 'time' test-access.log > /tmp/xx.log
```


## end

have fun ^_^