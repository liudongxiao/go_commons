package tag_stat

import (
	"context"
	"dmp_web/go/model"
	"fmt"

	"dmp_web/go/commons/db/hive"
	"dmp_web/go/commons/log"
)

var pathTmptable = []string{`
--put statistics into a tmp table
CREATE TEMPORARY TABLE %[3]v AS
SELECT bts.paths,count(1) AS cnt
FROM
(
    --filter begintime and merge the same domain_id
    SELECT transform(psts.site_id,psts.b_t_d_ref_d,psts.domain)
    USING 'python splitArrayElement.py' AS (site_id,paths)
    FROM
    (
        --group by visitor_id and site_id,and sort array by begin_time
        SELECT
            ccs.visitor_id,
            ccs.site_id,
            ccs.domain,
            sort_array(collect_list(ccs.b_t_d_ref_d)) AS b_t_d_ref_d
        FROM
        (
            -- concat begin_time,current_domain and referer_domain by '_'
            SELECT
                cs.visitor_id,
                cs.site_id,
                cs.page_domain_id,
                cs.begintime,
                cs.domain,
                concat_ws('_',cast(cs.begintime AS STRING), cs.ref_domain) AS b_t_d_ref_d
            FROM
            (
                --get records including referer domain column, and site_id = {site_id} in {dt} days
                SELECT
                    tf.visitor_id,
                    tf.site_id,
                    tf.page_domain_id,
                    tf.begintime,
                    tf.referer_url_id,
                    mmd.domain,
                    regexp_extract(mr.url, '(\\w+\\.)+\\w+',0) AS ref_domain
                FROM
				(
					select visitor_id,site_id,page_domain_id,begintime,referer_url_id
					from dna.trend_flow
					where
					  	%[1]v and
					  	trend_flow.visitor_id in (select visitor_id from %[2]v)
				) AS tf LEFT JOIN dna.meta_referer AS mr JOIN dna.meta_domain as mmd
                ON tf.referer_url_id = mr.url_id and mmd.domain_id = tf.page_domain_id
            ) AS cs
        ) AS ccs
        GROUP BY ccs.visitor_id,ccs.site_id,ccs.domain
    ) AS psts
) AS bts
GROUP BY
    bts.paths
	`,
	`SELECT svp1.paths,svp1.cnt, cast(svp1.cnt*100.0/svp2.num AS double) AS per
FROM
	%[3]v AS svp1,
	(
		SELECT sum(cnt) AS num
		FROM %[3]v
	) AS svp2`,
}

type TagStatPath struct {
	*Dimension
}

func (t *TagStatPath) GetFunc(metric string) ProcessFunc {
	switch metric {
	case model.MetricPageviews:
		return t.processV
	}
	return nil
}

func (t *TagStatPath) GetModel() model.StatModel {
	return &model.StatPathModel
}

func (t *TagStatPath) processV(ctx context.Context, ret *hive.ExecuteResult, dt string) (int, error) {
	obj := &model.StatPath{
		TagId: t.TagId(),
		Date:  dt,
	}
	var path string
	var cnt int

	return t.baseProcess(ctx, t, ret, func(data *[]interface{}) {
		s := obj.New(data, model.MetricVisitors)
		ret.Scan(&path, &cnt, &s.Visitors)
		s.Path.Domain = path
		s.Path.Count = cnt
	})
}

func (t *TagStatPath) Process(ctx context.Context) {
	// 创建临时表
	tmpTableName := fmt.Sprintf("dmpstage.stat_path_tmp_tag_%v_%v",
		t.TagId(), t.date.TagNow())
	start, end := t.date.RangeString()
	statement1 := fmt.Sprintf(pathTmptable[0],
		t.date.ToHQL("dt"), t.stage.Tbl[0], tmpTableName)
	// 做计算
	statement2 := fmt.Sprintf(pathTmptable[1], nil, nil, tmpTableName)
	tag := fmt.Sprintf("tag: %v, dt: (%v/%v)", t.TagId(), start, end)
	log.Debugf("%v start", tag)

	err := t.HiveCtx(ctx, statement1, func(ret *hive.ExecuteResult) {
		if !ret.Wait() {
			t.CancelAll(ret.Err())
			return
		}
		log.Debugf("%v statement1 done, start statement2", tag)
		err := t.HiveCtx(ctx, statement2, func(ret *hive.ExecuteResult) {
			if !ret.Wait() {
				t.CancelAll(ret.Err())
				return
			}
			log.Debugf("%v statement2 done, start statement2", tag)
			t.processV(ctx, ret, start)
		})
		if err != nil {
			t.CancelAll(ret.Err())
			return
		}
	})
	if err != nil {
		t.CancelAll(err)
		return
	}
}

func (t *TagStatPath) ProcessSql() ([]string, error) {
	return nil, nil
}
