
select  *
from (select avg(ss_list_price) B1_LP
            ,count(ss_list_price) B1_CNT
            ,count(distinct ss_list_price) B1_CNTD
      from store_sales
      where ss_quantity between 0 and 5
        and (ss_list_price between 135 and 135+10 
             or ss_coupon_amt between 675 and 675+1000
             or ss_wholesale_cost between 30 and 30+20)) B1,
     (select avg(ss_list_price) B2_LP
            ,count(ss_list_price) B2_CNT
            ,count(distinct ss_list_price) B2_CNTD
      from store_sales
      where ss_quantity between 6 and 10
        and (ss_list_price between 141 and 141+10
          or ss_coupon_amt between 13532 and 13532+1000
          or ss_wholesale_cost between 45 and 45+20)) B2,
     (select avg(ss_list_price) B3_LP
            ,count(ss_list_price) B3_CNT
            ,count(distinct ss_list_price) B3_CNTD
      from store_sales
      where ss_quantity between 11 and 15
        and (ss_list_price between 108 and 108+10
          or ss_coupon_amt between 6586 and 6586+1000
          or ss_wholesale_cost between 3 and 3+20)) B3,
     (select avg(ss_list_price) B4_LP
            ,count(ss_list_price) B4_CNT
            ,count(distinct ss_list_price) B4_CNTD
      from store_sales
      where ss_quantity between 16 and 20
        and (ss_list_price between 92 and 92+10
          or ss_coupon_amt between 15029 and 15029+1000
          or ss_wholesale_cost between 38 and 38+20)) B4,
     (select avg(ss_list_price) B5_LP
            ,count(ss_list_price) B5_CNT
            ,count(distinct ss_list_price) B5_CNTD
      from store_sales
      where ss_quantity between 21 and 25
        and (ss_list_price between 118 and 118+10
          or ss_coupon_amt between 14130 and 14130+1000
          or ss_wholesale_cost between 9 and 9+20)) B5,
     (select avg(ss_list_price) B6_LP
            ,count(ss_list_price) B6_CNT
            ,count(distinct ss_list_price) B6_CNTD
      from store_sales
      where ss_quantity between 26 and 30
        and (ss_list_price between 80 and 80+10
          or ss_coupon_amt between 11160 and 11160+1000
          or ss_wholesale_cost between 26 and 26+20)) B6
limit 100;

-- end query 28 in stream 0 using template query28