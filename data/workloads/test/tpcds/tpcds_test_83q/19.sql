
select  *
from (select avg(ss_list_price) B1_LP
            ,count(ss_list_price) B1_CNT
            ,count(distinct ss_list_price) B1_CNTD
      from store_sales
      where ss_quantity between 0 and 5
        and (ss_list_price between 27 and 27+10 
             or ss_coupon_amt between 6887 and 6887+1000
             or ss_wholesale_cost between 72 and 72+20)) B1,
     (select avg(ss_list_price) B2_LP
            ,count(ss_list_price) B2_CNT
            ,count(distinct ss_list_price) B2_CNTD
      from store_sales
      where ss_quantity between 6 and 10
        and (ss_list_price between 67 and 67+10
          or ss_coupon_amt between 15581 and 15581+1000
          or ss_wholesale_cost between 45 and 45+20)) B2,
     (select avg(ss_list_price) B3_LP
            ,count(ss_list_price) B3_CNT
            ,count(distinct ss_list_price) B3_CNTD
      from store_sales
      where ss_quantity between 11 and 15
        and (ss_list_price between 5 and 5+10
          or ss_coupon_amt between 3073 and 3073+1000
          or ss_wholesale_cost between 76 and 76+20)) B3,
     (select avg(ss_list_price) B4_LP
            ,count(ss_list_price) B4_CNT
            ,count(distinct ss_list_price) B4_CNTD
      from store_sales
      where ss_quantity between 16 and 20
        and (ss_list_price between 74 and 74+10
          or ss_coupon_amt between 3558 and 3558+1000
          or ss_wholesale_cost between 6 and 6+20)) B4,
     (select avg(ss_list_price) B5_LP
            ,count(ss_list_price) B5_CNT
            ,count(distinct ss_list_price) B5_CNTD
      from store_sales
      where ss_quantity between 21 and 25
        and (ss_list_price between 26 and 26+10
          or ss_coupon_amt between 7281 and 7281+1000
          or ss_wholesale_cost between 48 and 48+20)) B5,
     (select avg(ss_list_price) B6_LP
            ,count(ss_list_price) B6_CNT
            ,count(distinct ss_list_price) B6_CNTD
      from store_sales
      where ss_quantity between 26 and 30
        and (ss_list_price between 190 and 190+10
          or ss_coupon_amt between 11539 and 11539+1000
          or ss_wholesale_cost between 9 and 9+20)) B6
limit 100;

-- end query 28 in stream 0 using template query28