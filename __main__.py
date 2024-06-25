import duckdb
import pandas as pd
import pandasql as psql
import gc

RAW_PATH = 'raw/'

SILVER_PATH = 'silver/'

def ler_acidentes_IPEA(conn):
    """
    [Description]

        Reads traffic accident files by year and municipality, made available by IPEA.
        Numbers and rates of deaths from traffic accidents in Brazil, by year, municipality, sex. 
    
    [Source]

        Link: https://www.ipea.gov.br/atlasviolencia/filtros-series/12/violencia-no-transito

    [Goal]

        Reading of all files aggregated by year and municipality, making them available in a dataframe,
        seeking to obtain better performance in data analysis.
    
    """
    
    # Reading downloaded files using duckdb
    df_transportes = conn.execute(f"""SELECT * FROM '{RAW_PATH}IPEA/bitos-em-acidentes-de-transporte.csv' WHERE cod in ('3304557','5300108','3550308')""").df()
    df_mulheres = conn.execute(f"""SELECT * FROM '{RAW_PATH}IPEA/bitos-em-acidentes-de-transporte-mulheres.csv' WHERE cod in ('3304557','5300108','3550308')""").df()
    df_homens = conn.execute(f"""SELECT * FROM '{RAW_PATH}IPEA/bitos-em-acidentes-de-transporte-homens.csv' WHERE cod in ('3304557','5300108','3550308')""").df()
    df_jovens = conn.execute(f"""SELECT * FROM '{RAW_PATH}IPEA/bitos-em-acidentes-de-transporte-de-jovens.csv' WHERE cod in ('3304557','5300108','3550308')""").df()
    df_jovens_homens = conn.execute(f"""SELECT * FROM '{RAW_PATH}IPEA/bitos-em-acidentes-de-transporte-de-jovens-homens.csv' WHERE cod in ('3304557','5300108','3550308')""").df()
    df_jovens_mulheres = conn.execute(f"SELECT * FROM '{RAW_PATH}IPEA/bitos-em-acidentes-de-transporte-de-jovens-mulheres.csv' WHERE cod in ('3304557','5300108','3550308')""").df()
    df_tx_transportes = conn.execute(f"""SELECT * FROM '{RAW_PATH}IPEA/taxa-de-obitos-em-acidentes-de-transporte.csv' WHERE cod in ('3304557','5300108','3550308')""").df()
    df_tx_mulheres = conn.execute(f"""SELECT * FROM '{RAW_PATH}IPEA/taxa-de-obitos-em-acidentes-de-transporte-mulheres.csv' WHERE cod in ('3304557','5300108','3550308')""").df()
    df_tx_homens = conn.execute(f"""SELECT * FROM '{RAW_PATH}IPEA/taxa-de-obitos-em-acidentes-de-transporte-homens.csv' WHERE cod in ('3304557','5300108','3550308')""").df()
    df_tx_jovens = conn.execute(f"""SELECT * FROM '{RAW_PATH}IPEA/taxa-de-obitos-em-acidentes-de-transporte-de-jovens.csv' WHERE cod in ('3304557','5300108','3550308')""").df()
    df_tx_jovens_homens = conn.execute(f"""SELECT * FROM '{RAW_PATH}IPEA/taxa-de-obitos-em-acidentes-de-transporte-de-jovens-homens.csv' WHERE cod in ('3304557','5300108','3550308')""").df()
    df_tx_jovens_mulheres = conn.execute(f"""SELECT * FROM '{RAW_PATH}IPEA/taxa-de-obitos-em-acidentes-de-transporte-de-jovens-mulheres.csv' WHERE cod in ('3304557','5300108','3550308')""").df()

    # Query dataframes using to aggregate values ​​by month and year.
    query = """
        SELECT df_transportes.*, 
        df_mulheres.valor as total_mulheres, 
        df_homens.valor as total_homens, 
        df_jovens.valor as total_jovens, 
        df_jovens_homens.valor as total_jovens_homens, 
        df_jovens_mulheres.valor as total_jovens_mulheres,
        df_tx_transportes.valor as taxa_transporte,
        df_tx_mulheres.valor as taxa_mulheres,
        df_tx_homens.valor as taxa_homens,
        df_tx_jovens.valor as taxa_jovens,
        df_tx_jovens_homens.valor as taxa_jovens_homens,
        df_tx_jovens_mulheres.valor as taxa_jovens_mulheres


        FROM df_transportes
        LEFT JOIN df_mulheres
        ON df_transportes.cod = df_mulheres.cod AND df_transportes.período = df_mulheres.período
        LEFT JOIN df_homens
        ON df_transportes.cod = df_homens.cod AND df_transportes.período = df_homens.período
        LEFT JOIN df_jovens
        ON df_transportes.cod = df_jovens.cod AND df_transportes.período = df_jovens.período
        LEFT JOIN df_jovens_homens
        ON df_transportes.cod = df_jovens_homens.cod AND df_transportes.período = df_jovens_homens.período
        LEFT JOIN df_jovens_mulheres
        ON df_transportes.cod = df_jovens_mulheres.cod AND df_transportes.período = df_jovens_mulheres.período
        LEFT JOIN df_tx_transportes
        ON df_transportes.cod = df_tx_transportes.cod AND df_transportes.período = df_tx_transportes.período
        LEFT JOIN df_tx_mulheres
        ON df_transportes.cod = df_tx_mulheres.cod AND df_transportes.período = df_tx_mulheres.período
        LEFT JOIN df_tx_homens
        ON df_transportes.cod = df_tx_homens.cod AND df_transportes.período = df_tx_homens.período
        LEFT JOIN df_tx_jovens
        ON df_transportes.cod = df_tx_jovens.cod AND df_transportes.período = df_tx_jovens.período
        LEFT JOIN df_tx_jovens_homens
        ON df_transportes.cod = df_tx_jovens_homens.cod AND df_transportes.período = df_tx_jovens_homens.período
        LEFT JOIN df_tx_jovens_mulheres
        ON df_transportes.cod = df_tx_jovens_mulheres.cod AND df_transportes.período = df_tx_jovens_mulheres.período

        where df_transportes.período in ('2010','2011','2012','2013','2014','2015','2016','2017','2018','2019','2020')
    """

    # Using pandasql to execute query
    df_total = psql.sqldf(query, locals())

    # Cleaning unused dataframe
    del df_transportes
    del df_mulheres
    del df_homens
    del df_jovens
    del df_jovens_homens
    del df_jovens_mulheres
    del df_tx_transportes
    del df_tx_mulheres
    del df_tx_homens
    del df_tx_jovens
    del df_tx_jovens_homens
    del df_tx_jovens_mulheres
    
    # Garbage collect
    gc.collect()
    
    return df_total

if __name__ == '__main__':

    conn = duckdb.connect(':memory:')

    print(ler_acidentes_IPEA(conn))

    