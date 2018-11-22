package com.xiaoxiang.flink;

import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

import java.util.Random;

/**
 * UD Source
 */
public class ConnectSource implements ParallelSourceFunction<Record> {

    /**
     *  产生数据
     */
    @Override
    public void run(SourceContext<Record> sourceContext) throws Exception {
        while (true) {

            /*
             *    v1 业务线
             *    v2 业务Id
             *    v3 业务属性值
             *    v4 时间戳
             *    ....
             */

            Random random = new Random(100);

            for (int i = 0 ; i < 4 ; i++) {
                Record record = new Record();

                record.setBizName("" + i);
                record.setBizId(i);
                record.setAttr(random.nextInt () / 10);
                record.setData("json string or other");
                record.setTimestamp(System.currentTimeMillis () / 1000);

                sourceContext.collect(record);
            }

            Thread.sleep(200);
        }
    }

    /**
     * 关闭资源
     */
    @Override
    public void cancel() {
    }

}