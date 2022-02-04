pct4_chg = """SELECT TOP 10 SYMBOL, DATE, CLOSE_PRICE, VOLUME, CHANGE_PCT FROM india_stocks_daily WHERE DATE = CAST(GETDATE() as DATE) AND CHANGE_PCT > ? AND VOLUME > ? AND CLOSE_PRICE > ? ORDER BY CHANGE_PCT DESC"""

#------Query only when it is not run on the same day with date under consideration------------
# pct4_chg = """SELECT TOP 10 SYMBOL, CLOSE_PRICE, VOLUME, CHANGE_PCT FROM india_stocks_daily 
# WHERE DATE = CAST(DATEADD(DAY, -2, GETDATE()) as DATE) AND CHANGE_PCT > ? AND VOLUME > ? AND CLOSE_PRICE > ? ORDER BY CHANGE_PCT DESC"""

screener = """SELECT * FROM master_india_screener WHERE SCREENER = ? AND CLOSE_PRICE BETWEEN ? AND ? AND VOLUME BETWEEN ? AND ? AND CREATED_DT IN (SELECT MAX(CREATED_DT) FROM master_india_screener WHERE SCREENER = ? ) AND SYMBOL NOT IN (SELECT DISTINCT SYMBOL FROM exclusion_inclusion_list WHERE LIST_TYPE = 'EXCLUSION LIST');
"""

stock_symbol_reconcilation = """SELECT * FROM 
(SELECT msr.SYMBOL, msr.ISIN, isd.SYMBOL as SYM_D, w.SYMBOL as SYM_W, m.SYMBOL as SYM_M FROM 
master_stock_reference msr 
LEFT JOIN 
(SELECT DISTINCT SYMBOL FROM india_stocks_daily) isd 
ON 
msr.SYMBOL = isd.SYMBOL
LEFT JOIN 
(SELECT DISTINCT SYMBOL FROM india_stocks_weekly) w 
ON 
msr.SYMBOL = w.SYMBOL
LEFT JOIN 
(SELECT DISTINCT SYMBOL FROM india_stocks_weekly) m
ON 
msr.SYMBOL = m.SYMBOL
) t 
WHERE SYM_D IS NULL 
OR SYM_W IS NULL 
OR SYM_M IS NULL"""