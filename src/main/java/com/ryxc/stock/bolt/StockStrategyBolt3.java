package com.ryxc.stock.bolt;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.espertech.esper.client.*;
import com.ryxc.stock.dto.ResultStock;
import com.ryxc.stock.dto.StockRealTimeEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Created by tonye0115 on 2016/12/9.
 */
public class StockStrategyBolt3 extends BaseBasicBolt {
    private final Logger log = LoggerFactory.getLogger(this.getClass());

    private EPServiceProvider epService;
    private BasicOutputCollector basicOutputCollector;

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        log.info("----------股票策略3（放巨量）初始化...");
        Configuration configuration = new Configuration();
        configuration.addEventType("StockRealTimeEvent",StockRealTimeEvent.class.getName());
        epService = EPServiceProviderManager.getProvider("strategy3", configuration);
        EPStatement stmt = epService.getEPAdministrator().createEPL("select stockCode,newPrice,sum(newPrice*current*100) as totalValue " +
                "from StockRealTimeEvent.win:time(10 sec) group by stockCode " +
                "having sum(newPrice*current*100)>10000000");
        stmt.addListener(new UpdateListener() {
            @Override
            public void update(EventBean[] newEvents, EventBean[] oldEvents) {
                if(newEvents!=null){
                    try {
                        EventBean theEvent = newEvents[0];
                        String stockCode = (String)theEvent.get("stockCode");
                        Double newPrice = (Double) theEvent.get("newPrice");
                        Double totalValue = (Double) theEvent.get("totalValue");
                        log.info("--------股票策略3（放巨量）选出股票：" + stockCode + " 最新价：" + newPrice + " 成交额:" + totalValue);
                        basicOutputCollector.emit(new Values(new ResultStock(stockCode,newPrice,3,totalValue)));
                    } catch (Exception e){
                        e.printStackTrace();
                        log.error("epservice处理错误："+e.getStackTrace());
                    }
                }

            }
        });
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        this.basicOutputCollector = basicOutputCollector;
        StockRealTimeEvent stockRealTimeEvent = (StockRealTimeEvent) tuple.getValue(0);
        log.info("策略3（放巨量）===> Esper：" + stockRealTimeEvent);
        epService.getEPRuntime().sendEvent(stockRealTimeEvent);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("StockStrageBolt3"));
    }


    @Override
    public void cleanup() {
        if(epService.isDestroyed()){
            epService.destroy();
        }
    }

}
