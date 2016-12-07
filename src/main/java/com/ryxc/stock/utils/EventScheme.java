package com.ryxc.stock.utils;

import backtype.storm.spout.Scheme;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import com.alibaba.fastjson.JSONObject;
import com.ryxc.stock.dto.StockRealTimeEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.util.List;

/**
 * Created by tonye0115 on 2016/12/7.
 */
public class EventScheme implements Scheme {

    private static final Logger log = LoggerFactory.getLogger(EventScheme.class);

    @Override
    public List<Object> deserialize(byte[] bytes) {
        try {
            String msg = new String(bytes, "UTF-8");
            StockRealTimeEvent stockRealTimeEvent = JSONObject.parseObject(msg, StockRealTimeEvent.class);
            return new Values(stockRealTimeEvent);
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
            log.error("Exception:", e);
        }
        return null;
    }

    @Override
    public Fields getOutputFields() {
        return new Fields("msg");
    }
}
