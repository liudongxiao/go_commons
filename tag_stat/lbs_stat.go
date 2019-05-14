package tag_stat

import (
	"context"
	"fmt"
	"math"
	"time"

	"dmp_web/go/commons/log"

	"dmp_web/go/commons/Sql"
	"dmp_web/go/commons/db/hive"
	"dmp_web/go/commons/env"
	"dmp_web/go/commons/errors"
	"dmp_web/go/model"
	"dmp_web/go/model/model_rule"
	"dmp_web/go/utils/format"
)

// 设第一点A的经 纬度为(LonA, LatA)，第二点B的经纬度为(LonB, LatB)，按照0度经线的基准，
// 东经取经度的正值(Longitude)，西经取经度负值(-Longitude)，北纬取90-纬度值(90- Latitude)，南纬取90+纬度值(90+Latitude)，
// 则经过上述处理过后的两点被计为(MLonA, MLatA)和(MLonB, MLatB)。那么根据三角推导，可以得到计算两点距离的如下公式：
// C = sin(MLatA)*sin(MLatB)*cos(MLonA-MLonB) + cos(MLatA)*cos(MLatB)
// Distance = R*Arccos(C)*Pi/180

var generateGroupSql = `
CREATE TABLE %<tableName>s AS
SELECT
'did' as typ,
did as visitor_id
FROM dsp.dw_bid_logs where dt >= '%<minDt>s' and dt <= '%<maxDt>s'
AND CAST(CAST(longitude as float) * 1000000 AS bigint) >  %<minLon>d
AND CAST(CAST(longitude as float) * 1000000 AS bigint) <= %<maxLon>d
AND CAST(CAST(latitude  as float) * 1000000 AS bigint) >  %<minLat>d
AND CAST(CAST(latitude  as float) * 1000000 AS bigint) <= %<maxLat>d
group by did
`

var MERIDIAN_LENGTH = 40008000.0 //m 本初子午线周长，被纬度360等分
var EARTH_RADIUS = 6371000.0     //m 地球半径 平均值，米

type LbsStat struct {
	tag  *model.Tag
	hcli hive.Cli
}

//NewLbsStat 构造函数
func NewLBsStat(tag *model.Tag, hcli hive.Cli) *LbsStat {
	return &LbsStat{
		tag,
		hcli,
	}
}

//func (lbsStat *LbsStat) GenerateGroup_old(ctx context.Context) (tableName string, count int64, err error) {
//	//使用mobileTag是因为model.Tag没有把data暴露出来
//	var mobileTag model.MobileTag
//	tag := lbsStat.tag
//
//	if err := model.TagModel.FindId(tag.Id, &mobileTag); err != nil {
//		return "", 0, err
//	}
//	if e := tag.MarkStart(); e != nil {
//		log.Error(e)
//	}
//	rule1 := mobileTag.GetRuleItems()[0]
//	ruleMap := rule1.Value.(bson.M)
//	lat := ruleMap["circle_lat"].(float64)
//	lng := ruleMap["circle_lng"].(float64)
//	radius := ruleMap["radius"].(int)
//
//	sdt := time.Unix(int64(mobileTag.DateBegin), 0).Format("20060102")
//	edt := time.Unix(int64(mobileTag.DateEnd), 0).Format("20060102")
//
//	tableName = fmt.Sprintf("%s.tag_%d_lbs", env.HiveDatabase, mobileTag.Id)
//
//	//判断供应商信息是否为空
//	if len(mobileTag.SupplierIds) <= 0 {
//		return "", 0, errors.New("供应商信息为空!")
//	}
//	LbsSql := getSql(lat, lng, radius, sdt, edt, tableName, mobileTag.GetAdUserIdString())
//	if env.Test {
//		log.QueryPlanDebug(LbsSql)
//		return tableName, 1, nil
//	}
//
//	//删除旧表
//	dropTableSql := fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName)
//	if _, err := lbsStat.HCli.ExecuteSyncCtx(ctx, dropTableSql); err != nil {
//		log.Errorf("删除表失败! err: %+v", err)
//		return "", 0, err
//	}
//
//	//创建新表
//	if _, err := lbsStat.HCli.ExecuteSyncCtx(ctx, LbsSql); err != nil {
//		log.Error("Execute HQL error: ", LbsSql)
//		return "", 0, err
//	} else {
//		if num, err := Sql.CheckTableResultNumber(ctx, lbsStat.HCli, tableName); err != nil {
//			return "", 0, err
//		} else {
//			if num <= 0 {
//				return "", 0, errors.Newf("人群计算结果为0,tabId:%d", mobileTag.Id)
//			} else {
//				return tableName, num, nil
//			}
//		}
//	}
//}

func (lbsStat *LbsStat) GenerateGroup(ctx context.Context) (tableName string, count int64, err error) {
	tag := lbsStat.tag

	if e := tag.MarkStart(); e != nil {
		log.Error(e)
	}
	rule1 := tag.Rule.GetLogicRules()[0]
	ruleMap := rule1.Value.(model_rule.MapValue)
	lat := ruleMap["circle_lat"].(float64)
	lng := ruleMap["circle_lng"].(float64)
	radius := ruleMap["radius"].(int)

	sdt := time.Unix(int64(tag.DateBegin), 0).Format(env.DayFormat)
	edt := time.Unix(int64(tag.DateEnd), 0).Format(env.DayFormat)

	tableName = fmt.Sprintf("%s.tag_%d_lbs", env.HiveDatabase, tag.Id)

	//判断供应商信息是否为空
	if len(tag.SupplierIds) <= 0 {
		return "", 0, errors.Newf("供应商信息为空!")
	}

	suppilers, err := tag.GetAdUserIdString()
	if err != nil {
		return "", 0, err
	}
	LbsSql := getSql(lat, lng, radius, sdt, edt, tableName, suppilers)
	if env.Test {
		log.Debug(LbsSql)
		return tableName, 1, nil
	}

	//删除旧表
	dropTableSql := fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName)
	if _, err := lbsStat.hcli.ExecuteSyncCtx(ctx, dropTableSql); err != nil {
		log.Errorf("删除表失败! err: %+v", err)
		return "", 0, err
	}

	//创建新表
	if _, err := lbsStat.hcli.ExecuteSyncCtx(ctx, LbsSql); err != nil {
		log.Error("Execute HQL error: ", LbsSql)
		return "", 0, err
	} else {
		if num, err := Sql.CheckTableResultNumber(ctx, tableName); err != nil {
			return "", 0, err
		} else {
			if num <= 0 {
				return "", 0, errors.Newf("人群计算结果为0,tabId:%d", tag.Id)
			} else {
				return tableName, num, nil
			}
		}
	}
}

func getSql(lat float64, lon float64, radius int, sdt string, edt string, tableName string, adUserConditionSql string) string {
	minLon := int64(lon*1000000) + int64(distance2Longitude(0.0-radius, lat, lon)*1000000)
	maxLon := int64(lon*1000000) + int64(distance2Longitude(0.0+radius, lat, lon)*1000000)
	minLat := int64(lat*1000000) + int64(distance2Latitude(0.0-radius)*1000000)
	maxLat := int64(lat*1000000) + int64(distance2Latitude(0.0+radius)*1000000)

	sql := generateGroupSql
	params := map[string]interface{}{
		"LatA":            lat,
		"LonA":            lon,
		"minLon":          minLon,
		"maxLon":          maxLon,
		"minLat":          minLat,
		"maxLat":          maxLat,
		"tableName":       tableName,
		"minDt":           sdt,
		"maxDt":           edt,
		"radius":          radius,
		"adUserCondition": adUserConditionSql,
	}

	s := format.Sprintf(sql, params)
	return s
}

//distance 单位m
func distance2Latitude(distance int) float64 {
	return float64(distance) / MERIDIAN_LENGTH * 360.0
}

//distance 单位m
func distance2Longitude(distance int, lat float64, lon float64) float64 {
	return math.Sin(float64(distance)/2/EARTH_RADIUS) * 2.0 * (180.0 / math.Pi)
}
