
select  *
from (select avg(ss_list_price) B1_LP
            ,count(ss_list_price) B1_CNT
            ,count(distinct ss_list_price) B1_CNTD
      from store_sales
      where ss_quantity between 0 and 5
        and (ss_list_price between 98 and 98+10 
             or ss_coupon_amt between 1444 and 1444+1000
             or ss_wholesale_cost between 53 and 53+20)) B1,
     (select avg(ss_list_price) B2_LP
            ,count(ss_list_price) B2_CNT
            ,count(distinct ss_list_price) B2_CNTD
      from store_sales
      where ss_quantity between 6 and 10
        and (ss_list_price between 51 and 51+10
          or ss_coupon_amt between 12499 and 12499+1000
          or ss_wholesale_cost between 37 and 37+20)) B2,
     (select avg(ss_list_price) B3_LP
            ,count(ss_list_price) B3_CNT
            ,count(distinct ss_list_price) B3_CNTD
      from store_sales
      where ss_quantity between 11 and 15
        and (ss_list_price between 126 and 126+10
          or ss_coupon_amt between 15515 and 15515+1000
          or ss_wholesale_cost between 66 and 66+20)) B3,
     (select avg(ss_list_price) B4_LP
            ,count(ss_list_price) B4_CNT
            ,count(distinct ss_list_price) B4_CNTD
      from store_sales
      where ss_quantity between 16 and 20
        and (ss_list_price between 155 and 155+10
          or ss_coupon_amt between 15619 and 15619+1000
          or ss_wholesale_cost between 11 and 11+20)) B4,
     (select avg(ss_list_price) B5_LP
            ,count(ss_list_price) B5_CNT
            ,count(distinct ss_list_price) B5_CNTD
      from store_sales
      where ss_quantity between 21 and 25
        and (ss_list_price between 101 and 101+10
          or ss_coupon_amt between 4636 and 4636+1000
          or ss_wholesale_cost between 32 and 32+20)) B5,
     (select avg(ss_list_price) B6_LP
            ,count(ss_list_price) B6_CNT
            ,count(distinct ss_list_price) B6_CNTD
      from store_sales
      where ss_quantity between 26 and 30
        and (ss_list_price between 48 and 48+10
          or ss_coupon_amt between 8652 and 8652+1000
          or ss_wholesale_cost between 52 and 52+20)) B6
limit 100;

-- end query 28 in stream 0 using template query28