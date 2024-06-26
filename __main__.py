import duckdb
import pandas as pd
import pandasql as psql
import gc

# Files paths.
RAW_PATH = 'raw/'
SILVER_PATH = 'silver/'

# Definition of states for analysis according to the IBGE code
CODIGO_IBGE_SIH = "330455,530010,355030"

CODIGO_IBGE = "3304557,5300108,3550308"

def ler_acidentes_IPEA(conn) -> pd.DataFrame:
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

def ler_acidentes(conn) -> None:
    """
    [Description]

        Reads SIMU files - Transport Accidents.
        Records with the fields necessary for the analysis.
    
    [Source]

        Link: https://bigdata-arquivos.icict.fiocruz.br/PUBLICO/SIMU/temas/simu-acidentes-transportes-mun-T.zip

    [Goal]

        Reading the .csv file through duckdb returning a dataframe.
    
    """
    x = conn.read_csv(f"""{RAW_PATH}SIMU - Acidentes de Transportes/Acidentes de Transportes.csv""")

    df_simu_acidentes = conn.execute("""SELECT * FROM x WHERE "Código IBGE" in ('3304557','5300108','3550308') """).df()

    # Rename column 'Código IBGE'
    df_simu_acidentes.rename(columns={'Código IBGE': 'cod'}, inplace=True)
    
    # Reading IPEA accidents
    df_ipea_acientes = ler_acidentes_IPEA(conn)

    # Rename column 'período'
    df_ipea_acientes.rename(columns={'período': 'ano'}, inplace=True)

    # Creating a new dataframe with merge between IPEA and SIMU data.
    df_result = df_simu_acidentes.merge(df_ipea_acientes, on=['cod','ano'], how='inner')

    # Creating .parquet file.
    df_result.to_parquet(f"""{SILVER_PATH}acidentes-geral.parquet""", engine='pyarrow', compression='snappy')
 
def ler_carteira(conn) -> None:
    """
    [Description]

        Reads the SIMU files - Enterprise Portfolio.
        Records with the fields necessary for the analysis.

    [Source]

        Link: https://bigdata-arquivos.icict.fiocruz.br/PUBLICO/SIMU/bases_dados/CARTEIRA/simu-carteira-mun-T.zip

    [Goal]

        Reading the .csv file through duckdb returning a dataframe.


    """
    x = conn.read_csv(f"""{RAW_PATH}simu-carteira-mun-T.csv""")

    result = conn.execute(
        f"""SELECT * FROM x WHERE "Código IBGE" in ({CODIGO_IBGE}) """
    ).df()

    # Create .parquet file.
    result.to_parquet(
        f"""{SILVER_PATH}simu-carteira-mun-T.parquet""", engine="pyarrow", compression="snappy"
    )

    # Cleaning dataframe.
    del result

    # Garbage collect.
    gc.collect()

def ler_frotas(conn) -> None:
    """
    [Description]

        Reads SIMU files - Motorization - Evolutionary Fleet.
        Records with the fields necessary for the analysis.

    [Source]

        Link: https://bigdata-arquivos.icict.fiocruz.br/PUBLICO/SIMU/bases_dados/FROTA/simu-frota-mun_T.zip

    [Goal]

        Reading the .csv file through duckdb returning a dataframe.

    """
    x = conn.read_csv(f"""{RAW_PATH}simu-frota-mun_T.csv""")

    result = conn.execute(
        f"""SELECT * FROM x WHERE "Código IBGE" in ({CODIGO_IBGE}) """
    ).df()

    # Creating .parquet file.
    result.to_parquet(
        f"""{SILVER_PATH}simu-frota-mun_T.parquet""", engine="pyarrow", compression="snappy"
    )
    # Cleaning dataframe.
    del result

    # Garbage collect.
    gc.collect()

if __name__ == '__main__':

    conn = duckdb.connect(':memory:')

    print(ler_acidentes_IPEA(conn))

    ler_acidentes(conn)

    ler_carteira(conn)

    ler_frotas(conn)

    