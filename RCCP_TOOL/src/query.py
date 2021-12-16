def demand_temp1():

    return (r"""
        SELECT * INTO demand_temp1 FROM 
        (
        SELECT 
        d1.*, CONCAT(DOMESTIC_INTERNATIONAL ,  ' - ' , LANDED_NONLANDED) AS DOMESTIC_LANDED_VS_DOMESTIC_NONLANDED ,
        CASE WHEN pm.PRODUCT_ID IS NULL THEN 'NO' ELSE 'YES' END AS PRODUCT_AVAILABLE
        FROM (
        SELECT 
        *, format(DATES, 'yyyy-MM') AS 'YR_MONTH',
        CASE WHEN WAREHOUSE_FLAG = 'Direct Ship' OR WAREHOUSE_FLAG = 'AZ' OR WAREHOUSE_FLAG = 'DS' OR WAREHOUSE_FLAG = 'DU' THEN 'NON-LANDED' ELSE 'LANDED' END AS LANDED_NONLANDED,
        CASE WHEN COMPANY_ID = 11 OR COMPANY_ID=14 OR COMPANY_ID = 74 THEN 'DOMESTIC' ELSE 'INTERNATIONAL' END AS DOMESTIC_INTERNATIONAL
        FROM 
        demand_plan) d1
        LEFT JOIN 
        product_master pm
        ON 
        d1.PRODUCT_ID_CLEANSED = pm.PRODUCT_ID
        ) tbl """)


def inv_temp1():
    return ("""
    SELECT * INTO inv_temp1 FROM 
    (
    SELECT 
    inv.*, CASE WHEN pm.PRODUCT_ID IS NULL THEN 'NO' ELSE 'YES' END AS PRODUCT_AVAILABLE
    FROM (
    SELECT 
    *
    FROM 
    inventory) inv
    LEFT JOIN 
    product_master pm
    ON 
    inv.PRODUCT_ID_CLEANSED = pm.PRODUCT_ID
    ) tbl
    """   )
    
    
def sku_final_list():
    return (
        """
        SELECT * INTO sku_final_list FROM 
        (
        SELECT *, COALESCE(PM_PRODUCT_ID, DMD_PRODUCT_ID, PO_PRODUCT_ID, INV_PRODUCT_ID ) AS FINAL_SKU_LIST 
        FROM 
        (SELECT 
        distinct pm.PRODUCT_ID as PM_PRODUCT_ID, dt.PRODUCT_ID_CLEANSED as DMD_PRODUCT_ID , pt.PRODUCT_ID_CLEANSED as PO_PRODUCT_ID , inv.PRODUCT_ID_CLEANSED as INV_PRODUCT_ID 
        FROM
        product_master pm
        FULL JOIN
        demand_temp1 dt 
        ON
        pm.PRODUCT_ID = dt.PRODUCT_ID_CLEANSED
        FULL JOIN
        po_temp1 pt  
        ON
        pm.PRODUCT_ID = pt.PRODUCT_ID_CLEANSED
        FULL JOIN
        inv_temp1 inv  
        ON
        pm.PRODUCT_ID = inv.PRODUCT_ID_CLEANSED
        ) t1 ) tbl
        """    )