#!/bin/bash

echo "============================================================================================================="
echo "=================> START RESPAWING ACCOUNT COMMISSIONS <================================="

#During this scenario, I was working on a set of data that were miss. Inicially we were synchronizing our DB collecting the data
#From our origin (in this case other_dbname) and then inserting it in our target DB (in this case dbname).
#To solve this problem I decided to change the approach and start quering first the target DB, then with the target account number
#I can query the origin just to collect the lack data to restore my registers in the target DB.

# Database connection details
PGHOST="postgresql.host.db"
PGPORT="5432"
PGDATABASE="dbname"
PGUSER="username"
PGPASSWORD="password"

OP_PGDATABASE="other_dbname"

TARGET_QUERY="select a.account_number, a.account_id, a.created, a.last_updated, coalesce(a.last_yield_payment, null) from dbname.target.account a left join dbname.target.iv_account_commission iac on a.account_id = iac.account_id  where a.account_number like '%-2301-%' and iac.account_id is null limit 100000;"

AU_DATA_RETURNED=$(psql -t -A -h "$PGHOST" -p "$PGPORT" -d "$PGDATABASE" -U "$PGUSER" -c "$TARGET_QUERY")

#echo "Data: $AU_DATA_RETURNED"


echo "$AU_DATA_RETURNED" | while IFS= read -r line; do

    IFS='|' read -r account_number account_id created last_updated last_yield_payment <<< "$line"

    AU_ACCOUNT_COMMISSION="select * from dbname.target.iv_account_commission iac where iac.account_id = '$account_id';"

    AU_DATA_RETURNED=$(psql -t -A -h "$PGHOST" -p "$PGPORT" -d "$PGDATABASE" -U "$PGUSER" -c "$AU_ACCOUNT_COMMISSION")


    if [ -z "$AU_DATA_RETURNED" ]; then 
      echo "No results found. '$account_id' " 
      ORIGIN_QUERY="select tasa, plazo, diasxplazo from other_dbname.origin.acreedores a  where (a.idsucursal || '-' || a.idproducto  || '-' || idauxiliar) = '$account_number';"

      OP_DATA_RETURNED=$(psql -t -A -h "$PGHOST" -p "$PGPORT" -d "$OP_PGDATABASE" -U "$PGUSER" -c "$ORIGIN_QUERY")

      RATE=$(echo "$OP_DATA_RETURNED" | cut -d '|' -f1)
      TERM=$(echo "$OP_DATA_RETURNED" | cut -d '|' -f2)
      DAYS_PER_TERM=$(echo "$OP_DATA_RETURNED" | cut -d '|' -f3)

      uuid=$(uuidgen)

      INSERT_QRY="insert into dbname.target.iv_account_commission(account_id, concept, created_date, financed, fixed_amount, id, last_updated, name, percentage_amount, periodicity, status, taxed, use_case, type, accounting_account, iv_last_payment_date, processing_priority, movement_type, days_in_year, account_balance_type, alfanumeric_reference, term, days_per_term) values ('$account_id', 2, '$created', 0, 0, '$uuid' , '$last_updated', 'Tasa producto generico', $RATE, $TERM, 1, 1, 2, 5, '', '$last_yield_payment', 3, 2, 2, 2, 'Pago de rendimientos', $TERM, $DAYS_PER_TERM);"


      INSERTED_ROW=$(psql -t -A -h "$PGHOST" -p "$PGPORT" -d "$PGDATABASE" -U "$PGUSER" -c "$INSERT_QRY")
      echo "Row inserted: $INSERTED_ROW"
    else 
      echo "Results found: $account_id" 
    fi

done



echo "=================> END RESPAWING ACCOUNT COMMISSIONS <================================="
echo "============================================================================================================="
