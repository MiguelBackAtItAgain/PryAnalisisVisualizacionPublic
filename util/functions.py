from filecmp import dircmp

import pandas as pd

def get_table_regs(table_name, cols, iteration, db_context):
    columns_str = ','.join(cols)
    return pd.read_sql_query(f'SELECT {columns_str} FROM {table_name} WHERE ETL_ID = {iteration}', db_context)


def get_keys(table_name, cols, db_context):
    natural_keys_str = ','.join(cols)
    return pd.read_sql_query(f'SELECT ID, {natural_keys_str} FROM {table_name}', db_context).set_index(cols).to_dict()['ID']


def upsert(table_name, cols, df, db_context):
    existing_table_key_pairs = get_keys(table_name=table_name, cols=cols, db_context=db_context)
    columns = df.columns.tolist()
    if len(columns) > 0:
        for natural_key in cols:
            columns.remove(natural_key)

    if len(cols) == 1:
        df['ID'] = df.apply(lambda row: existing_table_key_pairs.get(*tuple(row[cols].values), None), axis=1)
    else:
        df['ID'] = df.apply(lambda row: existing_table_key_pairs.get(tuple(row[cols].values), None), axis=1)

    update_regs = df[df['ID'].notnull()]
    if not update_regs.empty:
        existing_elements = pd.read_sql_query('SELECT * FROM {table_name} WHERE ID IN ({ids})'.format(table_name=table_name, ids=','.join(update_regs['ID'].astype(str))), db_context)
        update_regs = update_regs.merge(existing_elements, how='outer', indicator=True).query('_merge == "left_only"').drop('_merge', axis=1)
        print(f'Updating {len(update_regs)} rows in {table_name}')
        update_query = f'UPDATE {table_name} SET {",".join([f"{col} = %s" for col in columns])} WHERE ID = %s'
        for index, row in update_regs.iterrows():
            db_context.execute(update_query, tuple(row[columns].values) + (row['ID'],))

    new_elements = df[df['ID'].isnull()]
    print(f'Inserting {len(new_elements)} rows in {table_name}')
    if not new_elements.empty:
        new_elements.to_sql(table_name, db_context, if_exists='append', index=False)
