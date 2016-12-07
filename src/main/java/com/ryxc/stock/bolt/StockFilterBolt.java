package com.ryxc.stock.bolt;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.ryxc.stock.dto.StockRealTimeEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by tonye0115 on 2016/12/7.
 */
public class StockFilterBolt extends BaseBasicBolt {

    private final Logger log = LoggerFactory.getLogger(this.getClass());

    /**
     * 过滤逻辑
     */
    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        StockRealTimeEvent event = (StockRealTimeEvent) tuple.getValue(0);
        log.info("---StockRealTimeEvent："+event);
        if (event.getNewPrice() == 0) {
            log.info("过滤掉无效行情："+event);
            return;
        }
        if (event.getBuyPrice1() > 0 && event.getSellPrice1() > 0
                && (event.getBuyPrice2() + event.getSellPrice2()) == 0
                && (event.getBuyPrice5() + event.getSellPrice5()) == 0) {
            log.info("过滤掉无效行情："+event);
            return;
        }
        basicOutputCollector.emit(new Values(event));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("filter-stock"));

    }
}
