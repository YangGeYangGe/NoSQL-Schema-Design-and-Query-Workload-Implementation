import pandas as pd

def change_to_csv():
    tsv_file='Posts.tsv'
    csv_table=pd.read_table(tsv_file,sep='\t')
    csv_table.to_csv('Posts.csv',index=False)

    tsv_file='Tags.tsv'
    csv_table=pd.read_table(tsv_file,sep='\t')
    csv_table.to_csv('Tags.csv',index=False)

    tsv_file='Users.tsv'
    csv_table=pd.read_table(tsv_file,sep='\t')
    csv_table.to_csv('Users.csv',index=False)

    tsv_file='Votes.tsv'
    csv_table=pd.read_table(tsv_file,sep='\t')
    csv_table.to_csv('Votes.csv',index=False)

change_to_csv()