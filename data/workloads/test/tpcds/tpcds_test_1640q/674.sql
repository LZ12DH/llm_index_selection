
select  *
from (select avg(ss_list_price) B1_LP
            ,count(ss_list_price) B1_CNT
            ,count(distinct ss_list_price) B1_CNTD
      from store_sales
      where ss_quantity between 0 and 5
        and (ss_list_price between 187 and 187+10 
             or ss_coupon_amt between 11701 and 11701+1000
             or ss_wholesale_cost between 32 and 32+20)) B1,
     (select avg(ss_list_price) B2_LP
            ,count(ss_list_price) B2_CNT
            ,count(distinct ss_list_price) B2_CNTD
      from store_sales
      where ss_quantity between 6 and 10
        and (ss_list_price between 89 and 89+10
          or ss_coupon_amt between 8533 and 8533+1000
          or ss_wholesale_cost between 77 and 77+20)) B2,
     (select avg(ss_list_price) B3_LP
            ,count(ss_list_price) B3_CNT
            ,count(distinct ss_list_price) B3_CNTD
      from store_sales
      where ss_quantity between 11 and 15
        and (ss_list_price between 173 and 173+10
          or ss_coupon_amt between 11277 and 11277+1000
          or ss_wholesale_cost between 40 and 40+20)) B3,
     (select avg(ss_list_price) B4_LP
            ,count(ss_list_price) B4_CNT
            ,count(distinct ss_list_price) B4_CNTD
      from store_sales
      where ss_quantity between 16 and 20
        and (ss_list_price between 58 and 58+10
          or ss_coupon_amt between 2163 and 2163+1000
          or ss_wholesale_cost between 35 and 35+20)) B4,
     (select avg(ss_list_price) B5_LP
            ,count(ss_list_price) B5_CNT
            ,count(distinct ss_list_price) B5_CNTD
      from store_sales
      where ss_quantity between 21 and 25
        and (ss_list_price between 119 and 119+10
          or ss_coupon_amt between 5411 and 5411+1000
          or ss_wholesale_cost between 21 and 21+20)) B5,
     (select avg(ss_list_price) B6_LP
            ,count(ss_list_price) B6_CNT
            ,count(distinct ss_list_price) B6_CNTD
      from store_sales
      where ss_quantity between 26 and 30
        and (ss_list_price between 55 and 55+10
          or ss_coupon_amt between 938 and 938+1000
          or ss_wholesale_cost between 20 and 20+20)) B6
limit 100;

-- end query 28 in stream 0 using template query28