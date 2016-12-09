package com.ryxc.stock.bolt;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import com.ryxc.stock.dbstore.ConnectionPool;
import com.ryxc.stock.dto.ResultStock;
import org.apache.commons.dbutils.QueryRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.Map;

/**
 * Created by tonye0115 on 2016/12/9.
 */
public class ReportBolt extends BaseBasicBolt {

    private final Logger log = LoggerFactory.getLogger(this.getClass());
    private QueryRunner query;

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        log.info("----------ReportBolt 初始化...");
        query = new QueryRunner(ConnectionPool.getInstance().getDataSource());
    }


    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        ResultStock resultStock = (ResultStock) tuple.getValue(0);
        String sql = "insert into result(stock_code,stock_price,total_value,stategy_id) values(?,?,?,?)";
        Object params[] = {resultStock.getStockCode(), resultStock.getNewPrice(),
                resultStock.getTotalValue(), resultStock.getStrategyId()};
        try {
            query.update(sql,params);
        } catch (SQLException e) {
            log.error("SQLException:", e);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    @Override
    public void cleanup() {
    }

}
