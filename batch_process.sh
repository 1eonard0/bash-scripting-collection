#!/bin/bash

echo "============================================================================================================="
echo "=================> START OF BATCH PROCESS <================================="

#During this scenario I had the objetive of implement a batch process that should sum totals depend on the 
#Type of product and the term of product.


# Database connection details
PGHOST="postgres.host.db"
PGPORT="5432"
PGDATABASE="dbname"
PGUSER="username"
PGPASSWORD="password"

QUERY_DATE="SELECT cast(NOW() AT TIME ZONE 'America/Mexico_City' as date);"

DATE_FORMAT=$(psql -t -A -h "$PGHOST" -p "$PGPORT" -d "$PGDATABASE" -U "$PGUSER" -c "$QUERY_DATE")

echo "Date used to collect data: $DATE_FORMAT"

QUERY="select dsrh.id from public.diary_serie_report_history dsrh where dsrh.created_date = '$DATE_FORMAT';"

#Get the last report id
REPORT_ID=$(psql -t -A -h "$PGHOST" -p "$PGPORT" -d "$PGDATABASE" -U "$PGUSER" -c "$QUERY")
echo "REPORT_ID: '$REPORT_ID'"

if [ -z "$REPORT_ID" ]; then
    NEW_REPORT="INSERT INTO public.diary_serie_report_history (id, created_date) VALUES(gen_random_uuid(), '$DATE_FORMAT');"
    psql -h "$PGHOST" -p "$PGPORT" -d "$PGDATABASE" -U "$PGUSER" -c "$NEW_REPORT"

    echo "New report created."

    #Get the last report id
    REPORT_ID=$(psql -t -A -h "$PGHOST" -p "$PGPORT" -d "$PGDATABASE" -U "$PGUSER" -c "$QUERY")

    echo "Report Id: $REPORT_ID"
    echo "Storing previous balances (Saldo anteriores)"

    QUERY="select coalesce(sum(a.balance_amount), 0) from public.account a join public.account_scheme as2 on a.account_scheme_id = as2.account_scheme_id where as2.product_number in ('2004', '2006', '2013', '2015') and cast(a.last_updated as date) = '$DATE_FORMAT';"

    # total for view products 
    results=$(psql -t -A -h "$PGHOST" -p "$PGPORT" -d "$PGDATABASE" -U "$PGUSER" -c "$QUERY")

    echo "Total results for view products: $results"

    QUERY="INSERT INTO public.view_investment_balance_history (diary_serie_report_id, balance_amount, row_name) VALUES('$REPORT_ID', $results, 'VIEW');"
    psql -h "$PGHOST" -p "$PGPORT" -d "$PGDATABASE" -U "$PGUSER" -c "$QUERY"

    echo "Total for view products stored"
    echo "Looking for 'Product to fixed term' with the term of 7, 30, 60, 90, 180, 270, 360, 540, 720, 1080, 1440, 1800, ..."

    for i in 7 30 60 90 180 270 360 540 720 1080 1440 1800; do
        QUERY="SELECT COALESCE(sum(td.credit_amount), 0) as s_anterior from public.account a inner join public.transaction_detail td on td.target_address =  a.account_id where a.object_type = 'InvestmentAccount' and a.accounting_id = '2102020111101' and a.term = $i and cast(td.created as date) = '$DATE_FORMAT';"   
        
        TOTAL_AMOUNT=$(psql -t -A -h "$PGHOST" -p "$PGPORT" -d "$PGDATABASE" -U "$PGUSER" -c "$QUERY")

        echo "Total amount for term $i : $TOTAL_AMOUNT"

        QUERY="INSERT INTO public.view_investment_balance_history (diary_serie_report_id, balance_amount, row_name) VALUES('$REPORT_ID', $TOTAL_AMOUNT, '$i');"
        psql -h "$PGHOST" -p "$PGPORT" -d "$PGDATABASE" -U "$PGUSER" -c "$QUERY"

        echo "value stored: [ report id: '$REPORT_ID', total amount: '$TOTAL_AMOUNT', term: '$i' ]"
    done
else
    echo "Report Id already exist."
fi

echo "=================> END OF BATCH PROCESS  <================================="
echo "============================================================================================================="
