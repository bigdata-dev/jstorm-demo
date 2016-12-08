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
 * Created by tonye0115 on 2016/12/8.
 */
public class StockStrategyBolt1 extends BaseBasicBolt{
    private final Logger log = LoggerFactory.getLogger(this.getClass());
    private EPServiceProvider epService;
    private BasicOutputCollector basicOutputCollector;
    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        log.info("--------------------股票策略1(大卖盘)初始化....");
        Configuration configuration = new Configuration();
        configuration.addEventType("StockRealTimeEvent",StockRealTimeEvent.class.getName());
        epService = EPServiceProviderManager.getProvider("strategy1", configuration);
        //选出股票的卖5档总手数大于买5档口总手数100倍时的股票
        EPStatement stmt = epService.getEPAdministrator().createEPL("select * from StockRealTimeEvent where " +
                "(buyCount5+buyCount4+buyCount3+buyCount2+buyCount1)*100" +
                "<=(sellCount5+sellCount4+sellCount3+sellCount2+sellCount1)");

        stmt.addListener(new UpdateListener() {
            @Override
            public void update(EventBean[] newEvents, EventBean[] oldEvents) {
                if(newEvents!=null){
                    EventBean theEvent = newEvents[0];
                    StockRealTimeEvent stockRTEvent = (StockRealTimeEvent)theEvent.getUnderlying();
                    log.info("--------股票策略1(大买盘)选出股票："+stockRTEvent.getStockCode()+" 最新价:"+stockRTEvent.getNewPrice());
                    basicOutputCollector.emit(new Values(new ResultStock(stockRTEvent.getStockCode(),stockRTEvent.getNewPrice(),1)));
                }
            }
        });
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        this.basicOutputCollector = basicOutputCollector;
        StockRealTimeEvent stockRealTimeEvent = (StockRealTimeEvent)tuple.getValue(0);
        log.info("策略1(大卖盘) ===> Esper:"+stockRealTimeEvent);
        epService.getEPRuntime().sendEvent(stockRealTimeEvent);

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("StockStrategy1"));
    }

    @Override
    public void cleanup(){
        if(!epService.isDestroyed()){
            epService.destroy();
        }
    }

}
