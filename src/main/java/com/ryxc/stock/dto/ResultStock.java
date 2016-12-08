package com.ryxc.stock.dto;

import java.io.Serializable;

/**
 * Created by tonye0115 on 2016/12/8.
 */
public class ResultStock implements Serializable {

    private String stockCode;
    private double newPrice;
    private int strategyId;

    public ResultStock(String stockCode, double newPrice, int strategyId) {
        this.stockCode = stockCode;
        this.newPrice = newPrice;
        this.strategyId = strategyId;
    }

    public String getStockCode() {
        return stockCode;
    }

    public void setStockCode(String stockCode) {
        this.stockCode = stockCode;
    }

    public double getNewPrice() {
        return newPrice;
    }

    public void setNewPrice(double newPrice) {
        this.newPrice = newPrice;
    }

    public int getStrategyId() {
        return strategyId;
    }

    public void setStrategyId(int strategyId) {
        this.strategyId = strategyId;
    }
}