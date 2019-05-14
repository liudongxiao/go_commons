package main

import "dmp_web/go/commons/log"

func main() {
	var jsconf = `
    {
        "UseShortFile":true,
        "Appenders": {
            "test_appender": {
                "Type": "file",
                "Target": "/tmp/example.log"
            },
            "a_appender": {
                "Type": "console"
            },
            "level_appender": {
                "@Type":"level表示不同level的日志打印到不同的文件中",
                "Type":"level",
                "Target":"/tmp/level_example.log"
            },
            "daily_appender": {
                "@Type":"dailyfile表示按日分割日志",
                "Type":"dailyfile",
                "Target":"/tmp/daily_example.log",
                "keepday":3
            },
            "track_appender":{
                "Type": "dailyfile",
                "Target": "/tmp/track_appender.log"
            }
        },
        "Loggers": {
            "dmp_web/go/commons/log/a": {
                "@Appenders": "日志输出到test_appender和a_appender和level_appender",
                "Appenders": [
                    "test_appender",
                    "a_appender",
                    "level_appender"
                ],
                "@Level": "记录log和log等级以上的数据",
                "Level": "log"
            },
            "dmp_web/go/commons/log/b": {
                "@Appenders": "日志输出到最近上级的appender,即Root的Appenders",
                "@Level": "只记录debug和error等级的数据",
                "Level": [
                    "debug",
                    "error"
                ]
            }
        },
        "Root": {
            "Level": "log",
            "Appenders": [
                "test_appender"
            ]
        },
        "Roots": {
            "track": {
               "Level": "warn",
               "Appenders": [
                    "track_appender"
                ]
            },
            "transfer": {
               "Level": "log",
               "Appenders": [
                    "test_appender"
                ]
            }
        }
    }
    `
	err := log.Init(jsconf)
	if err != nil {
		panic(err)
	}
	log.SetRootFileAppender("/tmp/chuangjie.log")
	log.SetRootLevel(log.WARN)
	log.Debug("JUST QueryPlanDebug")
	log.Log("JUST Log")
	log.Warn("JUST Warn")
	log.Error("JUST Error")
	log.Errorf("JUST Errorf :%d", 123)

	logger := log.Get("dmp_web/go/commons/log/a")
	logger.Error("hello logger dmp_web/go/commons/log/a")
	logger.Warn("hello logger dmp_web/go/commons/log/a")
	logger.Notice("hello logger dmp_web/go/commons/log/a")
	logger.Log("hello logger dmp_web/go/commons/log/a")
	//看不到debug信息
	logger.Debug("hello logger dmp_web/go/commons/log/a")

	logger = log.Get("dmp_web/go/commons/log/a/b")
	logger.Log("hello logger dmp_web/go/commons/log/a/b")

	logger = log.Get("dmp_web/go/commons/log/b")
	logger.Debug("hello logger dmp_web/go/commons/log/b")
	//看不到log信息
	logger.Log("hello logger dmp_web/go/commons/log/b")
	logger.Error("hello logger dmp_web/go/commons/log/b")

	log.UseRoot("track")
	log.Log("Use ROOT Log")
	log.Warn("Use ROOT Warn")
	log.Error("Use ROOT Error")
}
