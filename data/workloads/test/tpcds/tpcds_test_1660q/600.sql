
select  *
from (select avg(ss_list_price) B1_LP
            ,count(ss_list_price) B1_CNT
            ,count(distinct ss_list_price) B1_CNTD
      from store_sales
      where ss_quantity between 0 and 5
        and (ss_list_price between 104 and 104+10 
             or ss_coupon_amt between 5624 and 5624+1000
             or ss_wholesale_cost between 65 and 65+20)) B1,
     (select avg(ss_list_price) B2_LP
            ,count(ss_list_price) B2_CNT
            ,count(distinct ss_list_price) B2_CNTD
      from store_sales
      where ss_quantity between 6 and 10
        and (ss_list_price between 5 and 5+10
          or ss_coupon_amt between 15617 and 15617+1000
          or ss_wholesale_cost between 76 and 76+20)) B2,
     (select avg(ss_list_price) B3_LP
            ,count(ss_list_price) B3_CNT
            ,count(distinct ss_list_price) B3_CNTD
      from store_sales
      where ss_quantity between 11 and 15
        and (ss_list_price between 47 and 47+10
          or ss_coupon_amt between 2376 and 2376+1000
          or ss_wholesale_cost between 46 and 46+20)) B3,
     (select avg(ss_list_price) B4_LP
            ,count(ss_list_price) B4_CNT
            ,count(distinct ss_list_price) B4_CNTD
      from store_sales
      where ss_quantity between 16 and 20
        and (ss_list_price between 97 and 97+10
          or ss_coupon_amt between 3575 and 3575+1000
          or ss_wholesale_cost between 18 and 18+20)) B4,
     (select avg(ss_list_price) B5_LP
            ,count(ss_list_price) B5_CNT
            ,count(distinct ss_list_price) B5_CNTD
      from store_sales
      where ss_quantity between 21 and 25
        and (ss_list_price between 102 and 102+10
          or ss_coupon_amt between 2220 and 2220+1000
          or ss_wholesale_cost between 31 and 31+20)) B5,
     (select avg(ss_list_price) B6_LP
            ,count(ss_list_price) B6_CNT
            ,count(distinct ss_list_price) B6_CNTD
      from store_sales
      where ss_quantity between 26 and 30
        and (ss_list_price between 143 and 143+10
          or ss_coupon_amt between 1710 and 1710+1000
          or ss_wholesale_cost between 28 and 28+20)) B6
limit 100;

-- end query 28 in stream 0 using template query28